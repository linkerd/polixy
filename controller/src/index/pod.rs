use super::{DefaultAllow, Index, Namespace, SrvIndex};
use crate::{
    k8s::{self, polixy},
    KubeletIps, Lookup, PodIps, ServerRx, ServerRxTx,
};
use anyhow::{anyhow, bail, Result};
use parking_lot::Mutex;
use std::{
    borrow::Borrow,
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
    hash::Hash,
    net::IpAddr,
    sync::Arc,
};
use tokio::sync::watch;
use tracing::{debug, instrument, trace, warn};

#[derive(Debug, Default)]
pub(super) struct PodIndex {
    index: HashMap<k8s::PodName, Pod>,
}

#[derive(Debug)]
struct Pod {
    servers: Arc<PodServers>,
    labels: k8s::Labels,
    default_rx: ServerRx,
}

#[derive(Debug, Default)]
struct PodServers {
    by_port: HashMap<u16, Arc<PodServer>>,
    by_name: HashMap<polixy::server::PortName, Vec<Arc<PodServer>>>,
}

#[derive(Debug)]
struct PodServer {
    server_name: Mutex<Option<polixy::server::Name>>,
    tx: ServerRxTx,
}

// === impl PodIndex ===

impl PodIndex {
    pub(super) fn link_servers(&self, servers: &SrvIndex) {
        for pod in self.index.values() {
            pod.link_servers(&servers)
        }
    }

    pub fn reset_server<N>(&self, name: &N)
    where
        k8s::polixy::server::Name: Borrow<N>,
        N: Hash + Eq + ?Sized,
    {
        for (pod_name, pod) in self.index.iter() {
            let rx = pod.default_rx.clone();
            for (port_num, port) in pod.servers.by_port.iter() {
                let mut sn = port.server_name.lock();
                if sn.as_ref().map(|n| n.borrow()) == Some(name) {
                    debug!(pod = %pod_name, port = %port_num, "Removing server from pod");
                    *sn = None;
                    port.tx
                        .send(rx.clone())
                        .expect("pod config receiver must still be held");
                } else {
                    trace!(pod = %pod_name, port = %port_num, server = ?sn, "Server does not match");
                }
            }
        }
    }
}

// === impl Pod ===

impl Pod {
    pub fn link_servers(&self, servers: &SrvIndex) {
        for (name, port, rx) in servers.iter_matching(self.labels.clone()) {
            for port in self.servers.collect_port(&port).into_iter() {
                let mut sn = port.server_name.lock();
                if let Some(sn) = sn.as_ref() {
                    if sn != name {
                        // TODO handle conflicts differently?
                        tracing::warn!("Pod port matches multiple servers: {} and {}", sn, name);
                        debug_assert!(false);
                        continue;
                    }
                }
                *sn = Some(name.clone());

                port.tx
                    .send(rx.clone())
                    .expect("pod config receiver must be set");
                debug!(server = %name, "Pod server updated");
            }
        }
    }
}

// === impl PodServers ===

impl PodServers {
    fn collect_port(&self, port_match: &polixy::server::Port) -> Vec<Arc<PodServer>> {
        match port_match {
            polixy::server::Port::Number(ref port) => self
                .by_port
                .get(port)
                .into_iter()
                .cloned()
                .collect::<Vec<_>>(),
            polixy::server::Port::Name(ref name) => self
                .by_name
                .get(name)
                .into_iter()
                .flat_map(|p| p.iter())
                .cloned()
                .collect::<Vec<_>>(),
        }
    }
}

// === impl Index ===

impl Index {
    /// Builds a `Pod`, linking it with servers and nodes.
    #[instrument(
        skip(self, pod),
        fields(
            ns = ?pod.metadata.namespace,
            name = ?pod.metadata.name,
        )
    )]
    pub(super) fn apply_pod(&mut self, pod: k8s::Pod) -> Result<()> {
        let ns_name = k8s::NsName::from_pod(&pod);
        let Namespace {
            default_allow,
            ref mut pods,
            ref mut servers,
            ..
        } = self.namespaces.get_or_default(ns_name.clone());

        let pod_name = k8s::PodName::from_pod(&pod);
        match pods.index.entry(pod_name.clone()) {
            HashEntry::Vacant(pod_entry) => {
                let default_allow = match DefaultAllow::from_annotation(&pod.metadata) {
                    Ok(Some(allow)) => allow,
                    Ok(None) => *default_allow,
                    Err(error) => {
                        warn!(%error, "Ignoring invalid default-allow annotation");
                        *default_allow
                    }
                };

                let spec = pod.spec.ok_or_else(|| anyhow!("pod missing spec"))?;
                let kubelet_ips = {
                    let name = spec
                        .node_name
                        .clone()
                        .map(k8s::NodeName::from)
                        .ok_or_else(|| anyhow!("pod missing node name"))?;
                    self.nodes.get(&name)?
                };

                let status = pod.status.ok_or_else(|| anyhow!("pod missing status"))?;
                let pod_ips = mk_pod_ips(status)?;

                let default_rx = self.default_allows.get(default_allow);
                let (pod_servers, lookups) =
                    collect_pod_servers(spec, default_rx.clone(), pod_ips, kubelet_ips);

                let labels = k8s::Labels::from(pod.metadata.labels);
                let pod = Pod {
                    servers: pod_servers,
                    labels,
                    default_rx,
                };
                pod.link_servers(&servers);

                if self.lookups.insert((ns_name, pod_name), lookups).is_some() {
                    unreachable!("pod must not exist in lookups");
                }

                pod_entry.insert(pod);
            }

            HashEntry::Occupied(mut pod_entry) => {
                // Note that the default-allow annotation may not be changed at runtime.
                debug_assert!(
                    self.lookups.contains_key(&(ns_name, pod_name)),
                    "pod must exist in lookups"
                );

                let p = pod_entry.get_mut();
                if p.labels.as_ref() != &pod.metadata.labels {
                    let labels = k8s::Labels::from(pod.metadata.labels);
                    p.labels = labels;
                    p.link_servers(&servers);
                }
            }
        }

        Ok(())
    }

    #[instrument(
        skip(self, pod),
        fields(
            ns = ?pod.metadata.namespace,
            name = ?pod.metadata.name,
        )
    )]
    pub(super) fn delete_pod(&mut self, pod: k8s::Pod) -> Result<()> {
        let ns_name = k8s::NsName::from_pod(&pod);
        let pod_name = k8s::PodName::from_pod(&pod);
        self.rm_pod(ns_name, pod_name)
    }

    fn rm_pod(&mut self, ns: k8s::NsName, pod: k8s::PodName) -> Result<()> {
        self.namespaces
            .index
            .get_mut(&ns)
            .ok_or_else(|| anyhow!("namespace {} doesn't exist", ns))?
            .pods
            .index
            .remove(&pod)
            .ok_or_else(|| anyhow!("pod {} doesn't exist", pod))?;

        self.lookups
            .remove(&(ns, pod))
            .ok_or_else(|| anyhow!("pod doesn't exist in namespace"))?;

        debug!("Removed pod");

        Ok(())
    }

    #[instrument(skip(self, pods))]
    pub(super) fn reset_pods(&mut self, pods: Vec<k8s::Pod>) -> Result<()> {
        let mut prior_pods = self
            .namespaces
            .iter()
            .map(|(name, ns)| {
                let pods = ns.pods.index.keys().cloned().collect::<HashSet<_>>();
                (name.clone(), pods)
            })
            .collect::<HashMap<_, _>>();

        let mut result = Ok(());
        for pod in pods.into_iter() {
            let ns_name = k8s::NsName::from_pod(&pod);
            if let Some(ns) = prior_pods.get_mut(&ns_name) {
                let pod_name = k8s::PodName::from_pod(&pod);
                ns.remove(&pod_name);
            }

            if let Err(error) = self.apply_pod(pod) {
                result = Err(error);
            }
        }

        for (ns, pods) in prior_pods.into_iter() {
            for pod in pods.into_iter() {
                if let Err(error) = self.rm_pod(ns.clone(), pod) {
                    result = Err(error);
                }
            }
        }

        result
    }
}

fn collect_pod_servers(
    spec: k8s::PodSpec,
    server_rx: ServerRx,
    pod_ips: PodIps,
    kubelet_ips: KubeletIps,
) -> (Arc<PodServers>, Arc<HashMap<u16, Lookup>>) {
    let mut pod_servers = PodServers::default();
    let mut lookups = HashMap::new();

    for container in spec.containers.into_iter() {
        for p in container.ports.into_iter() {
            if p.protocol.map(|p| p == "TCP").unwrap_or(true) {
                let port = p.container_port as u16;
                if pod_servers.by_port.contains_key(&port) {
                    debug!(port, "Port duplicated");
                    continue;
                }

                let (tx, rx) = watch::channel(server_rx.clone());
                let pod_port = Arc::new(PodServer {
                    tx,
                    server_name: Mutex::new(None),
                });

                let name = p.name.map(k8s::polixy::server::PortName::from);
                if let Some(name) = name.clone() {
                    pod_servers
                        .by_name
                        .entry(name)
                        .or_default()
                        .push(pod_port.clone());
                }

                trace!(%port, ?name, "Adding port");
                pod_servers.by_port.insert(port, pod_port);
                lookups.insert(
                    port,
                    Lookup {
                        rx,
                        name,
                        pod_ips: pod_ips.clone(),
                        kubelet_ips: kubelet_ips.clone(),
                    },
                );
            }
        }
    }

    (pod_servers.into(), lookups.into())
}

fn mk_pod_ips(status: k8s::PodStatus) -> Result<PodIps> {
    let ips = if status.pod_ips.is_empty() {
        status
            .pod_ip
            .iter()
            .map(|ip| ip.parse::<IpAddr>().map_err(Into::into))
            .collect::<Result<Vec<IpAddr>>>()?
    } else {
        status
            .pod_ips
            .iter()
            .flat_map(|ip| ip.ip.as_ref())
            .map(|ip| ip.parse().map_err(Into::into))
            .collect::<Result<Vec<IpAddr>>>()?
    };
    if ips.is_empty() {
        bail!("pod missing IP addresses");
    };
    Ok(PodIps(ips.into()))
}
