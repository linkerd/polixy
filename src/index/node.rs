//! Node->Kubelet IP

use super::Index;
use crate::{k8s, FromResource, KubeletIps};
use anyhow::{anyhow, Context, Result};
use ipnet::IpNet;
use std::{
    collections::{hash_map::Entry as HashEntry, HashSet},
    net::IpAddr,
};
use tracing::{debug, instrument, trace, warn};

impl Index {
    /// Tracks the kubelet IP for each node.
    ///
    /// As pods are we created, we refer to the node->kubelet index to automatically allow traffic
    /// from the kubelet.
    #[instrument(
        skip(self, node),
        fields(name = ?node.metadata.name)
    )]
    pub(super) fn apply_node(&mut self, node: k8s::Node) -> Result<()> {
        let name = k8s::NodeName::from_resource(&node);

        match self.node_ips.entry(name) {
            HashEntry::Vacant(entry) => {
                let ips = Self::kubelet_ips(node)
                    .with_context(|| format!("failed to load kubelet IPs for {}", entry.key()))?;
                debug!(?ips, "Adding");
                entry.insert(ips);
            }
            HashEntry::Occupied(_) => trace!("Already existed"),
        }

        Ok(())
    }

    #[instrument(
        skip(self, node),
        fields(name = ?node.metadata.name)
    )]
    pub(super) fn delete_node(&mut self, node: k8s::Node) -> Result<()> {
        let name = k8s::NodeName::from_resource(&node);
        if self.node_ips.remove(&name).is_some() {
            debug!("Deleted");
            Ok(())
        } else {
            Err(anyhow!("node {} already deleted", name))
        }
    }

    #[instrument(skip(self, nodes))]
    pub(super) fn reset_nodes(&mut self, nodes: Vec<k8s::Node>) -> Result<()> {
        // Avoid rebuilding data for nodes that have not changed.
        let mut prior_names = self.node_ips.keys().cloned().collect::<HashSet<_>>();

        let mut result = Ok(());
        for node in nodes.into_iter() {
            let name = k8s::NodeName::from_resource(&node);
            if !prior_names.remove(&name) {
                if let Err(error) = self.apply_node(node) {
                    warn!(%name, %error, "Failed to apply node");
                    result = Err(error);
                }
            } else {
                trace!(%name, "Already existed");
            }
        }

        for name in prior_names.into_iter() {
            debug!(?name, "Removing defunct node");
            let removed = self.node_ips.remove(&name).is_some();
            debug_assert!(removed, "node must be removable");
            if !removed {
                result = Err(anyhow!("node {} already removed", name));
            }
        }

        result
    }

    fn kubelet_ips(node: k8s::Node) -> Result<KubeletIps> {
        let spec = node.spec.ok_or_else(|| anyhow!("node missing spec"))?;

        let addrs = if let Some(nets) = spec.pod_cidrs {
            nets.into_iter()
                .map(Self::cidr_to_kubelet_ip)
                .collect::<Result<Vec<_>>>()?
        } else {
            let cidr = spec
                .pod_cidr
                .ok_or_else(|| anyhow!("node missing pod_cidr"))?;
            let ip = Self::cidr_to_kubelet_ip(cidr)?;
            vec![ip]
        };

        Ok(KubeletIps(addrs.into()))
    }

    fn cidr_to_kubelet_ip(cidr: String) -> Result<IpAddr> {
        cidr.parse::<IpNet>()
            .with_context(|| format!("invalid CIDR {}", cidr))?
            .hosts()
            .next()
            .ok_or_else(|| anyhow!("pod CIDR network is empty"))
    }
}
