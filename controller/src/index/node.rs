//! Node->Kubelet IP

use super::Index;
use crate::{
    k8s::{self, ResourceExt},
    KubeletIps,
};
use anyhow::{anyhow, Context, Result};
use ipnet::IpNet;
use std::{
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
    net::IpAddr,
};
use tracing::{debug, instrument, trace, warn};

#[derive(Debug, Default)]
pub(super) struct NodeIndex {
    index: HashMap<String, KubeletIps>,
}

// === impl NodeIndex ===

impl NodeIndex {
    pub fn get(&self, name: &str) -> Option<KubeletIps> {
        self.index.get(name).cloned()
    }
}

// === impl Index ===

impl Index {
    /// Tracks the kubelet IP for each node.
    ///
    /// As pods are we created, we refer to the node->kubelet index to automatically allow traffic
    /// from the kubelet.
    #[instrument(
        skip(self, node),
        fields(name = ?node.metadata.name)
    )]
    pub fn apply_node(&mut self, node: k8s::Node) -> Result<()> {
        match self.nodes.index.entry(node.name()) {
            HashEntry::Vacant(entry) => {
                let ips = kubelet_ips(node)
                    .with_context(|| format!("failed to load kubelet IPs for {}", entry.key()))?;
                debug!(?ips, "Adding");
                entry.insert(ips);
                // TODO check pods waiting on this node's registry.
            }

            HashEntry::Occupied(_) => trace!("Already existed"),
        }

        Ok(())
    }

    #[instrument(skip(self))]
    pub fn delete_node(&mut self, name: &str) -> Result<()> {
        self.nodes
            .index
            .remove(name)
            .ok_or_else(|| anyhow!("node {} already deleted", name))?;
        debug!("Deleted");
        Ok(())
    }

    #[instrument(skip(self, nodes))]
    pub fn reset_nodes(&mut self, nodes: Vec<k8s::Node>) -> Result<()> {
        // Avoid rebuilding data for nodes that have not changed.
        let mut prior = self.nodes.index.keys().cloned().collect::<HashSet<_>>();

        let mut result = Ok(());
        for node in nodes.into_iter() {
            let name = node.name();
            if !prior.remove(&name) {
                trace!(%name, "Already existed");
            } else if let Err(error) = self.apply_node(node) {
                warn!(%name, %error, "Failed to apply node");
                result = Err(error);
            }
        }

        for name in prior.into_iter() {
            debug!(?name, "Removing defunct node");
            let removed = self.nodes.index.remove(&name).is_some();
            debug_assert!(removed, "node must be removable");
            if !removed {
                result = Err(anyhow!("node {} already removed", name));
            }
        }

        result
    }
}

fn cidr_to_kubelet_ip(cidr: String) -> Result<IpAddr> {
    cidr.parse::<IpNet>()
        .with_context(|| format!("invalid CIDR {}", cidr))?
        .hosts()
        .next()
        .ok_or_else(|| anyhow!("pod CIDR network is empty"))
}

fn kubelet_ips(node: k8s::Node) -> Result<KubeletIps> {
    let spec = node.spec.ok_or_else(|| anyhow!("node missing spec"))?;

    let addrs = if spec.pod_cidrs.is_empty() {
        let cidr = spec
            .pod_cidr
            .ok_or_else(|| anyhow!("node missing pod_cidr"))?;
        let ip = cidr_to_kubelet_ip(cidr)?;
        vec![ip]
    } else {
        spec.pod_cidrs
            .into_iter()
            .map(cidr_to_kubelet_ip)
            .collect::<Result<Vec<_>>>()?
    };

    Ok(KubeletIps(addrs.into()))
}
