use super::{DefaultAllow, Index, Namespace, NodeIndex, SrvIndex};
use crate::{
    k8s::{self, polixy, ResourceExt},
    lookup, KubeletIps, PodIps, ServerRx, ServerRxTx,
};
use anyhow::{anyhow, bail, Result};
use parking_lot::Mutex;
use std::{
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
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
    servers: Box<PortServers>,
    labels: k8s::Labels,
    default_allow_rx: ServerRx,
}

#[derive(Debug, Default)]
struct PortServers {
    by_port: HashMap<u16, Arc<Server>>,
    by_name: HashMap<String, Vec<Arc<Server>>>,
}

#[derive(Debug)]
struct Server {
    name: Mutex<Option<String>>,
    tx: ServerRxTx,
}

// === impl Index ===

impl Index {
    /// Builds a `Pod`, linking it with servers and nodes.
    #[instrument(
        skip(self, pod, lookups),
        fields(
            ns = ?pod.metadata.namespace,
            name = ?pod.metadata.name,
        )
    )]
    pub(super) fn apply_pod(&mut self, pod: k8s::Pod, lookups: &mut lookup::Writer) -> Result<()> {
        let Namespace {
            default_allow,
            ref mut pods,
            ref mut servers,
            ..
        } = self.namespaces.get_or_default(k8s::NsName::from_pod(&pod));

        let default_allow = *default_allow;
        let allows = self.default_allows.clone();
        let mk_default_allow =
            move |da: Option<DefaultAllow>| allows.get(da.unwrap_or(default_allow));

        pods.apply(pod, &self.nodes, servers, lookups, mk_default_allow)
    }

    #[instrument(
        skip(self, pod),
        fields(
            ns = ?pod.metadata.namespace,
            name = ?pod.metadata.name,
        )
    )]
    pub(super) fn delete_pod(&mut self, pod: k8s::Pod, lookups: &mut lookup::Writer) -> Result<()> {
        let ns_name = k8s::NsName::from_pod(&pod);
        let pod_name = k8s::PodName::from_pod(&pod);
        self.rm_pod(ns_name, pod_name, lookups)
    }

    fn rm_pod(
        &mut self,
        ns: k8s::NsName,
        pod: k8s::PodName,
        lookups: &mut lookup::Writer,
    ) -> Result<()> {
        self.namespaces
            .index
            .get_mut(&ns)
            .ok_or_else(|| anyhow!("namespace {} doesn't exist", ns))?
            .pods
            .index
            .remove(&pod)
            .ok_or_else(|| anyhow!("pod {} doesn't exist", pod))?;

        lookups.unset(&ns, &pod)?;

        debug!("Removed pod");

        Ok(())
    }

    #[instrument(skip(self, pods))]
    pub(super) fn reset_pods(
        &mut self,
        pods: Vec<k8s::Pod>,
        lookups: &mut lookup::Writer,
    ) -> Result<()> {
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
            let ns_name = pod.namespace().unwrap();
            if let Some(ns) = prior_pods.get_mut(ns_name.as_str()) {
                ns.remove(pod.name().as_str());
            }

            if let Err(error) = self.apply_pod(pod, lookups) {
                result = Err(error);
            }
        }

        for (ns, pods) in prior_pods.into_iter() {
            for pod in pods.into_iter() {
                if let Err(error) = self.rm_pod(ns.clone(), pod, lookups) {
                    result = Err(error);
                }
            }
        }

        result
    }
}

// === impl PodIndex ===

impl PodIndex {
    fn apply(
        &mut self,
        pod: k8s::Pod,
        nodes: &NodeIndex,
        servers: &SrvIndex,
        lookups: &mut lookup::Writer,
        get_default_allow_rx: impl Fn(Option<DefaultAllow>) -> ServerRx,
    ) -> Result<()> {
        let ns_name = pod.namespace().expect("pod must have a namespace");
        let pod_name = pod.name();
        match self.index.entry(pod_name.clone().into()) {
            HashEntry::Vacant(pod_entry) => {
                let spec = pod.spec.ok_or_else(|| anyhow!("pod missing spec"))?;
                let status = pod.status.ok_or_else(|| anyhow!("pod missing status"))?;

                // Lookup the pod's node's kubelet IP or stop processing the update. When the pod
                // gets assigned to a node, we'll get a new update and proceed. This is needed
                // because we always permit kubelet access.
                let kubelet = match spec.node_name.as_ref() {
                    Some(node) => {
                        // We've received a pod update before the node is known. This should be
                        // effectively impossible. We could technically handle this (unlikely)
                        // situation gracefully, but it would require updating pods on node
                        // creation, which seems unnecessarily complex.
                        nodes.get(node.as_str()).expect("node not yet indexed")
                    }
                    None => {
                        debug!("Pod is not yet assigned to a Node");
                        return Ok(());
                    }
                };

                // Similarly, lookup the pod's IPs so we can return it with port lookups so servers
                // can ignore connections targeting other endpoints.
                let pod_ips = Self::mk_pod_ips(status)?;

                // Check the pod for a default-allow annotation. If it's set, use it; otherwise use
                // the default policy from the namespace or cluster. We retain this value (and not
                // only the policy) so that we can more conveniently de-duplicate changes
                let default_allow_rx = match DefaultAllow::from_annotation(&pod.metadata) {
                    Ok(allow) => get_default_allow_rx(allow),
                    Err(error) => {
                        warn!(%error, "Ignoring invalid default-allow annotation");
                        get_default_allow_rx(None)
                    }
                };

                // Read the pod's ports and extract:
                // - `ServerTx`s to be linkerd against the server index; and
                // - lookup receivers to be returned to API clients.
                let (ports, pod_lookups) =
                    Self::extract_ports(spec, default_allow_rx.clone(), pod_ips, kubelet);

                // Start tracking the pod's metadata so it can be linked against servers as they are
                // created. Immediately link the pod against the server index.
                let pod = Pod {
                    default_allow_rx,
                    labels: pod.metadata.labels.into(),
                    servers: ports.into(),
                };
                pod.link_servers(&servers);
                pod_entry.insert(pod);

                // The pod has been linked against servers and is registered for subsequent updates,
                // so make it discoverable to API clients.
                lookups
                    .set(ns_name, pod_name, pod_lookups)
                    .expect("pod must not already exist");

                Ok(())
            }

            HashEntry::Occupied(mut entry) => {
                debug_assert!(
                    lookups.contains(&ns_name, &pod_name),
                    "pod must exist in lookups"
                );

                // Labels can be updated at runtime (even though that's kind of weird). If the
                // labels have changed, then we relink servers to pods in case label selections have
                // changed.
                let p = entry.get_mut();
                if p.labels.as_ref() != &pod.metadata.labels {
                    p.labels = pod.metadata.labels.into();
                    p.link_servers(&servers);
                }

                // Note that the default-allow annotation may not be changed at runtime.
                Ok(())
            }
        }
    }

    /// Extracts port information from a pod spec.
    fn extract_ports(
        spec: k8s::PodSpec,
        server_rx: ServerRx,
        pod_ips: PodIps,
        kubelet: KubeletIps,
    ) -> (PortServers, HashMap<u16, lookup::PodPort>) {
        let mut servers = PortServers::default();
        let mut lookups = HashMap::new();

        for container in spec.containers.into_iter() {
            for p in container.ports.into_iter() {
                if p.protocol.map(|p| p == "TCP").unwrap_or(true) {
                    let port = p.container_port as u16;
                    if servers.by_port.contains_key(&port) {
                        debug!(port, "Port duplicated");
                        continue;
                    }

                    let (tx, rx) = watch::channel(server_rx.clone());
                    let pod_port = Arc::new(Server {
                        tx,
                        name: Mutex::new(None),
                    });

                    trace!(%port, name = ?p.name, "Adding port");
                    if let Some(name) = p.name {
                        servers
                            .by_name
                            .entry(name)
                            .or_default()
                            .push(pod_port.clone());
                    }

                    servers.by_port.insert(port, pod_port);
                    lookups.insert(
                        port,
                        lookup::PodPort {
                            rx,
                            pod_ips: pod_ips.clone(),
                            kubelet_ips: kubelet.clone(),
                        },
                    );
                }
            }
        }

        (servers, lookups)
    }

    /// Extract the pod's IPs or throws an error.
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

    pub(super) fn link_servers(&self, servers: &SrvIndex) {
        for pod in self.index.values() {
            pod.link_servers(&servers)
        }
    }

    pub(super) fn reset_server(&self, name: &str) {
        for (pod_name, pod) in self.index.iter() {
            let rx = pod.default_allow_rx.clone();
            for (port, server) in pod.servers.by_port.iter() {
                let mut sn = server.name.lock();
                if sn.as_ref().map(|n| n.as_str()) == Some(name) {
                    debug!(pod = %pod_name, %port, "Removing server from pod");
                    *sn = None;
                    server
                        .tx
                        .send(rx.clone())
                        .expect("pod config receiver must still be held");
                } else {
                    trace!(pod = %pod_name, %port, server = ?sn, "Server does not match");
                }
            }
        }
    }
}

// === impl Pod ===

impl Pod {
    /// Links this pods to server (by label selector).
    //
    // XXX This doesn't properly reset a policy when a server is removed or de-selects a pod.
    fn link_servers(&self, servers: &SrvIndex) {
        for (name, port, rx) in servers.iter_matching(self.labels.clone()) {
            for p in self.servers.collect_port(&port).into_iter() {
                self.link_server_port(name, rx, p);
            }
        }
    }

    fn link_server_port(&self, name: &str, rx: &ServerRx, port: Arc<Server>) {
        // Either this port is using a default allow policy, and the server name is unset,
        // or multiple servers select this pod. If there's a conflict, we panic if the proxy
        // is running in debug mode. In release mode, we log a warning and ignore the
        // conflicting server.
        let mut sn = port.name.lock();
        if let Some(sn) = sn.as_ref() {
            if sn != name {
                debug_assert!(false, "Pod port must not match multiple servers");
                tracing::warn!("Pod port matches multiple servers: {} and {}", sn, name);
                return;
            }
        }
        *sn = Some(name.to_string());

        port.tx
            .send(rx.clone())
            .expect("pod config receiver must be set");
        debug!(server = %name, "Pod server updated");
    }
}

// === impl PortServers ===

impl PortServers {
    /// Finds all ports on this pod that match a server's port reference.
    ///
    /// Numeric port matches will only return a single server, generally, while named port
    /// references may select an arbitrary number of server ports.
    fn collect_port(&self, port_match: &polixy::server::Port) -> Vec<Arc<Server>> {
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
