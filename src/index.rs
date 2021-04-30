use crate::{k8s, v1, watch::Watch, FromResource};
use anyhow::{anyhow, bail, Context, Error, Result};
use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use futures::prelude::*;
use ipnet::IpNet;
use kube::{api::ListParams, Api};
use kube_runtime::watcher;
use parking_lot::Mutex;
use std::{
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
    hash::Hash,
    net::IpAddr,
    sync::Arc,
};
use tokio::{sync::watch, time};
use tracing::{debug, instrument, warn};

type SharedLookupMap = Arc<DashMap<(k8s::NsName, k8s::PodName), Arc<HashMap<u16, Lookup>>>>;

#[derive(Clone, Debug)]
pub struct Handle(SharedLookupMap);

#[derive(Clone, Debug)]
pub struct Lookup {
    pub name: Option<v1::server::PortName>,
    pub kubelet_ips: KubeletIps,

    /// Each pod-port has a watch for servers; and then the server config can be
    /// updated as its authorizations change.
    pub server: watch::Receiver<watch::Receiver<ServerConfig>>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct KubeletIps(Arc<Vec<IpAddr>>);

struct Index {
    /// Cached Node IPs.
    node_ips: HashMap<k8s::NodeName, KubeletIps>,

    /// Holds per-namespace pod/server/authorization indexes.
    namespaces: HashMap<k8s::NsName, NsIndex>,

    /// A shared map containing watches for all pods.  API clients simply
    /// retrieve watches from this pre-populated map.
    lookups: SharedLookupMap,

    /// A default server config to use when no server matches.
    default_config_rx: watch::Receiver<ServerConfig>,
    /// Keeps the above server config receiver alive. Never updated.
    _default_config_tx: watch::Sender<ServerConfig>,

    // Resource watches.
    nodes: Watch<k8s::Node>,
    pods: Watch<k8s::Pod>,
    servers: Watch<v1::Server>,
    authorizations: Watch<v1::Authorization>,
}

#[derive(Debug, Default)]
struct NsIndex {
    /// Caches pod labels so we can differentiate innocuous updates (like status
    /// changes) from label changes that could impact server indexing.
    pods: HashMap<k8s::PodName, Pod>,

    /// Caches a watch for each server.
    servers: HashMap<v1::server::Name, Server>,

    authzs: HashMap<v1::authz::Name, AuthzMeta>,
}

#[derive(Debug)]
struct Pod {
    port_names: Arc<HashMap<v1::server::PortName, u16>>,
    port_lookups: Arc<HashMap<u16, PodPort>>,
    labels: v1::Labels,
}

#[derive(Debug)]
struct PodPort {
    server_name: Mutex<Option<v1::server::Name>>,
    tx: watch::Sender<watch::Receiver<ServerConfig>>,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub protocol: ProxyProtocol,
    pub authorizations: Vec<Arc<Authz>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyProtocol {
    Detect { timeout: time::Duration },
    Opaque,
    Http,
    Grpc,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct AuthzMeta {
    pub servers: ServerSelector,
    pub authz: Arc<Authz>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Authz {
    Unauthenticated(Vec<IpNet>),
    Authenticated {
        identities: Vec<String>,
        suffixes: Vec<Vec<String>>,
    },
}

/// Selects servers for an authorization.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ServerSelector {
    Name(v1::server::Name),
    Selector(Arc<v1::labels::Selector>),
}

#[derive(Debug)]
struct Server {
    meta: ServerMeta,
    authorizations: HashMap<v1::authz::Name, Arc<Authz>>,
    rx: watch::Receiver<ServerConfig>,
    tx: watch::Sender<ServerConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerMeta {
    labels: v1::Labels,
    port: v1::server::Port,
    pod_selector: Arc<v1::labels::Selector>,
    protocol: ProxyProtocol,
}

pub fn run(client: kube::Client) -> (Handle, impl Future<Output = Error>) {
    let lookups = SharedLookupMap::default();

    // Watches Nodes, Pods, Servers, and Authorizations to update the lookup map
    // with an entry for each linkerd-injected pod.
    let idx = Index::new(
        watcher(Api::all(client.clone()), ListParams::default()).into(),
        watcher(
            Api::all(client.clone()),
            ListParams::default().labels("linkerd.io/control-plane-ns"),
        )
        .into(),
        watcher(Api::all(client.clone()), ListParams::default()).into(),
        watcher(Api::all(client), ListParams::default()).into(),
        lookups.clone(),
    );

    (Handle(lookups), idx.index())
}

// === impl Handle ===

impl Handle {
    pub fn lookup(&self, ns: k8s::NsName, name: k8s::PodName, port: u16) -> Option<Lookup> {
        self.0.get(&(ns, name))?.get(&port).cloned()
    }
}

// === impl Index ===

impl Index {
    fn new(
        nodes: Watch<k8s::Node>,
        pods: Watch<k8s::Pod>,
        servers: Watch<v1::Server>,
        authorizations: Watch<v1::Authorization>,
        lookups: SharedLookupMap,
    ) -> Self {
        // A default config to be provided to pods when no matching server
        // exists.
        let (_default_config_tx, default_config_rx) = watch::channel(ServerConfig {
            protocol: ProxyProtocol::Detect {
                timeout: time::Duration::from_secs(5),
            },
            authorizations: vec![
                // Permit all traffic when a `Server` instance is not present.
                Arc::new(Authz::Unauthenticated(vec!["0.0.0.0/0".parse().unwrap()])),
            ],
        });

        Self {
            nodes,
            pods,
            authorizations,
            servers,
            lookups,
            node_ips: HashMap::default(),
            namespaces: HashMap::default(),

            default_config_rx,
            _default_config_tx,
        }
    }

    /// Drives indexing for all resource types.
    ///
    /// This is all driven on a single task, so it's not necessary for any of the
    /// indexing logic to worry about concurrent access for the internal indexing
    /// structures.  All updates are published to the shared `lookups` map after
    /// indexing ocrurs; but the indexing task is soley responsible for mutating
    /// it. The associated `Handle` is used for reads against this.
    #[instrument(skip(self), fields(result))]
    async fn index(mut self) -> Error {
        loop {
            let res = tokio::select! {
                // Track the kubelet IPs for all nodes.
                up = self.nodes.recv() => match up {
                        watcher::Event::Applied(node) => self.apply_node(node),
                        watcher::Event::Deleted(node) => self.delete_node(node),
                        watcher::Event::Restarted(nodes) => self.reset_nodes(nodes),
                },

                up = self.pods.recv() => match up {
                        watcher::Event::Applied(pod) => self.apply_pod(pod),
                        watcher::Event::Deleted(pod) => self.delete_pod(pod),
                        watcher::Event::Restarted(pods) => self.reset_pods(pods),
                },

                up = self.servers.recv() => match up {
                    watcher::Event::Applied(srv) => {
                        self.apply_server(srv);
                        Ok(())
                    }
                    watcher::Event::Deleted(srv) => self.delete_server(srv),
                    watcher::Event::Restarted(srvs) => self.reset_servers(srvs),
                },

                up = self.authorizations.recv() => match up {
                    watcher::Event::Applied(authz) => self.apply_authz(authz),
                    watcher::Event::Deleted(authz) => self.delete_authz(authz),
                    watcher::Event::Restarted(authzs) => self.reset_authzs(authzs),
                },
            };
            if let Err(error) = res {
                debug!(%error);
            }
        }
    }

    // === Node->IP indexing ===

    fn apply_node(&mut self, node: k8s::Node) -> Result<()> {
        let name = k8s::NodeName::from_resource(&node);
        if let HashEntry::Vacant(entry) = self.node_ips.entry(name) {
            let ips = Self::kubelet_ips(node)
                .with_context(|| format!("failed to load kubelet IPs for {}", entry.key()))?;
            debug!(name = %entry.key(), ?ips, "Adding node");
            entry.insert(ips);
        } else {
            debug!(?node.metadata, "Node already existed");
        }
        Ok(())
    }

    fn delete_node(&mut self, node: k8s::Node) -> Result<()> {
        let name = k8s::NodeName::from_resource(&node);
        if self.node_ips.remove(&name).is_some() {
            debug!(%name, "Node deleted");
            Ok(())
        } else {
            Err(anyhow!("node {} already deleted", name))
        }
    }

    fn reset_nodes(&mut self, nodes: Vec<k8s::Node>) -> Result<()> {
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
                debug!(%name, "Node already existed");
            }
        }

        debug!(?prior_names, "Removing defunct nodes");
        for name in prior_names.into_iter() {
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
        let cidr = spec
            .pod_cidr
            .ok_or_else(|| anyhow!("node missing pod_cidr"))?;
        let mut addrs = Vec::new();

        let ip = Self::cidr_to_kubelet_ip(cidr)?;
        addrs.push(ip);
        if let Some(cidrs) = spec.pod_cidrs {
            for cidr in cidrs.into_iter().skip(1) {
                let ip = Self::cidr_to_kubelet_ip(cidr)?;
                addrs.push(ip);
            }
        }

        Ok(KubeletIps(addrs.into()))
    }

    fn cidr_to_kubelet_ip(cidr: String) -> Result<IpAddr> {
        cidr.parse::<IpNet>()
            .with_context(|| format!("invalid CIDR {}", cidr))?
            .hosts()
            .next()
            .ok_or_else(|| anyhow!("pod CIDR network is empty"))
    }

    // === Pod->Port->Server indexing ===

    fn apply_pod(&mut self, pod: k8s::Pod) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&pod);
        let pod_name = k8s::PodName::from_resource(&pod);
        let spec = pod.spec.ok_or_else(|| anyhow!("pod missing spec"))?;

        let NsIndex {
            ref mut pods,
            ref mut servers,
            ..
        } = self.namespaces.entry(ns_name.clone()).or_default();

        let lookups_entry = self.lookups.entry((ns_name, pod_name.clone()));

        match (pods.entry(pod_name), lookups_entry) {
            (HashEntry::Vacant(pod_entry), DashEntry::Vacant(lookups_entry)) => {
                let labels = v1::Labels::from(pod.metadata.labels);

                let kubelet = {
                    let name = spec
                        .node_name
                        .map(k8s::NodeName::from)
                        .ok_or_else(|| anyhow!("pod missing node name"))?;
                    self.node_ips
                        .get(&name)
                        .ok_or_else(|| anyhow!("node IP does not exist for node {}", name))?
                        .clone()
                };

                let mut port_names = HashMap::new();
                let mut pod_ports = HashMap::new();
                let mut lookups = HashMap::new();
                for container in spec.containers.into_iter() {
                    if let Some(ps) = container.ports {
                        for p in ps.into_iter() {
                            if p.protocol.map(|p| p == "TCP").unwrap_or(true) {
                                let port = p.container_port as u16;
                                if pod_ports.contains_key(&port) {
                                    debug!(port, "Port duplicated");
                                    continue;
                                }

                                let name = p.name.map(Into::into);
                                if let Some(name) = name.clone() {
                                    match port_names.entry(name) {
                                        HashEntry::Occupied(entry) => {
                                            debug!(name = %entry.key(), "Port name duplicated");
                                            continue;
                                        }
                                        HashEntry::Vacant(entry) => {
                                            entry.insert(port);
                                        }
                                    }
                                }

                                let (tx, server) = watch::channel(self.default_config_rx.clone());

                                let pp = PodPort {
                                    server_name: Mutex::new(None),
                                    tx,
                                };
                                pod_ports.insert(port, pp);

                                let lookup = Lookup {
                                    name,
                                    server,
                                    kubelet_ips: kubelet.clone(),
                                };
                                lookups.insert(port, lookup);
                            }
                        }
                    }
                }

                for (srv_name, server) in servers.iter() {
                    let pod_port = match server.meta.port {
                        v1::server::Port::Number(ref p) => pod_ports.get(p),
                        v1::server::Port::Name(ref n) => {
                            port_names.get(n).and_then(|p| pod_ports.get(p))
                        }
                    };
                    if let Some(pod_port) = pod_port {
                        if server.meta.pod_selector.matches(&labels) {
                            // TODO handle conflicts
                            *pod_port.server_name.lock() = Some(srv_name.clone());
                            pod_port
                                .tx
                                .send(server.rx.clone())
                                .expect("pod config receiver must be set");
                        }
                    }
                }

                lookups_entry.insert(Arc::new(lookups));
                pod_entry.insert(Pod {
                    port_names: port_names.into(),
                    port_lookups: pod_ports.into(),
                    labels,
                });
            }

            (HashEntry::Occupied(mut pod_entry), DashEntry::Occupied(_)) => {
                let labels = v1::Labels::from(pod.metadata.labels);

                if pod_entry.get().labels != labels {
                    for (srv_name, server) in servers.iter() {
                        let pod_port = match server.meta.port {
                            v1::server::Port::Number(ref p) => pod_entry.get().port_lookups.get(p),
                            v1::server::Port::Name(ref n) => pod_entry
                                .get()
                                .port_names
                                .get(n)
                                .and_then(|p| pod_entry.get().port_lookups.get(p)),
                        };

                        if let Some(pod_port) = pod_port {
                            if server.meta.pod_selector.matches(&labels) {
                                // TODO handle conflicts
                                *pod_port.server_name.lock() = Some(srv_name.clone());
                                pod_port
                                    .tx
                                    .send(server.rx.clone())
                                    .expect("pod config receiver must be set");
                            }
                        }
                    }

                    pod_entry.get_mut().labels = labels;
                }
            }

            _ => unreachable!("pod label and server indexes must be consistent"),
        }

        Ok(())
    }

    fn delete_pod(&mut self, pod: k8s::Pod) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&pod);
        let pod_name = k8s::PodName::from_resource(&pod);
        self.rm_pod(&ns_name, &pod_name)
    }

    fn rm_pod(&mut self, ns: &k8s::NsName, pod: &k8s::PodName) -> Result<()> {
        self.namespaces
            .get_mut(ns)
            .ok_or_else(|| anyhow!("namespace {} doesn't exist", ns))?
            .pods
            .remove(pod)
            .ok_or_else(|| anyhow!("pod {} doesn't exist", pod))?;

        self.lookups
            .remove(&(ns.clone(), pod.clone()))
            .ok_or_else(|| anyhow!("pod {} doesn't exist in namespace {}", pod, ns))?;

        Ok(())
    }

    fn reset_pods(&mut self, pods: Vec<k8s::Pod>) -> Result<()> {
        let mut prior_pod_labels = self
            .namespaces
            .iter()
            .map(|(n, ns)| {
                let pods = ns
                    .pods
                    .iter()
                    .map(|(n, p)| (n.clone(), p.labels.clone()))
                    .collect::<HashMap<_, _>>();
                (n.clone(), pods)
            })
            .collect::<HashMap<_, _>>();

        let mut result = Ok(());
        for pod in pods.into_iter() {
            let ns_name = k8s::NsName::from_resource(&pod);
            let pod_name = k8s::PodName::from_resource(&pod);

            if let Some(prior) = prior_pod_labels.get_mut(&ns_name) {
                if let Some(prior_labels) = prior.remove(&pod_name) {
                    let labels = v1::Labels::from(pod.metadata.labels.clone());
                    if prior_labels == labels {
                        continue;
                    }
                }
            }

            if let Err(error) = self.apply_pod(pod) {
                result = Err(error);
            }
        }

        for (ns, pods) in prior_pod_labels.into_iter() {
            for (pod, _) in pods.into_iter() {
                if let Err(error) = self.rm_pod(&ns, &pod) {
                    result = Err(error);
                }
            }
        }

        result
    }

    // === Server indexing ===

    fn apply_server(&mut self, srv: v1::Server) {
        let ns_name = k8s::NsName::from_resource(&srv);
        let srv_name = v1::server::Name::from_resource(&srv);
        let labels = v1::Labels::from(srv.metadata.labels);
        let port = srv.spec.port;

        let NsIndex {
            ref pods,
            authzs: ref ns_authzs,
            ref mut servers,
        } = self.namespaces.entry(ns_name).or_default();

        match servers.entry(srv_name) {
            HashEntry::Occupied(mut entry) => {
                let protocol = Self::mk_protocol(srv.spec.proxy_protocol.as_ref());

                // If something about the server changed, we need to update the
                // config to reflect the change.
                if entry.get().meta.labels != labels || entry.get().meta.protocol == protocol {
                    // NB: Only a single task applies server updates, so it's
                    // okay to borrow a version, modify, and send it.  We don't
                    // need a lock because serialization is guaranteed.
                    let mut config = entry.get().rx.borrow().clone();

                    if entry.get().meta.labels != labels {
                        let mut authorizations = HashMap::with_capacity(ns_authzs.len());
                        for (authz_name, a) in ns_authzs.iter() {
                            let matches = match a.servers {
                                ServerSelector::Name(ref n) => n == entry.key(),
                                ServerSelector::Selector(ref s) => s.matches(&labels),
                            };
                            if matches {
                                authorizations.insert(authz_name.clone(), a.authz.clone());
                            }
                        }

                        config.authorizations = authorizations.values().cloned().collect();
                        entry.get_mut().meta.labels = labels;
                        entry.get_mut().authorizations = authorizations;
                    }

                    config.protocol = protocol.clone();
                    entry.get_mut().meta.protocol = protocol;

                    entry
                        .get()
                        .tx
                        .send(config)
                        .expect("server update must succeed");
                }

                // If the pod/port selector didn't change, we don't need to
                // refresh the index.
                if *entry.get().meta.pod_selector == srv.spec.pod_selector
                    && entry.get().meta.port == port
                {
                    return;
                }

                entry.get_mut().meta.pod_selector = srv.spec.pod_selector.into();
                entry.get_mut().meta.port = port;
            }

            HashEntry::Vacant(entry) => {
                let protocol = Self::mk_protocol(srv.spec.proxy_protocol.as_ref());

                let mut authorizations = HashMap::with_capacity(ns_authzs.len());
                for (authz_name, a) in ns_authzs.iter() {
                    let matches = match a.servers {
                        ServerSelector::Name(ref n) => n == entry.key(),
                        ServerSelector::Selector(ref s) => s.matches(&labels),
                    };
                    if matches {
                        authorizations.insert(authz_name.clone(), a.authz.clone());
                    }
                }

                let (tx, rx) = watch::channel(ServerConfig {
                    protocol: protocol.clone(),
                    authorizations: authorizations.values().cloned().collect(),
                });
                entry.insert(Server {
                    tx,
                    rx,
                    authorizations,
                    meta: ServerMeta {
                        labels,
                        port,
                        pod_selector: srv.spec.pod_selector.into(),
                        protocol,
                    },
                });
            }
        }

        // If we've updated the server->pod selection, then we need to reindex
        // all pods and servers.
        for pod in pods.values() {
            for (srv_name, srv) in servers.iter() {
                let pod_port = match srv.meta.port {
                    v1::server::Port::Number(ref p) => pod.port_lookups.get(p),
                    v1::server::Port::Name(ref n) => {
                        pod.port_names.get(&n).and_then(|p| pod.port_lookups.get(p))
                    }
                };

                if let Some(pod_port) = pod_port {
                    if srv.meta.pod_selector.matches(&pod.labels) {
                        // TODO handle conflicts
                        let mut sn = pod_port.server_name.lock();
                        debug_assert!(sn.is_none(), "pod port matches multiple servers");
                        *sn = Some(srv_name.clone());

                        // It's up to the lookup stream to de-duplicate updates.
                        pod_port
                            .tx
                            .send(srv.rx.clone())
                            .expect("pod config receiver is held");
                    }
                }
            }
        }
    }

    fn mk_protocol(p: Option<&v1::server::ProxyProtocol>) -> ProxyProtocol {
        match p {
            Some(v1::server::ProxyProtocol::Detect) | None => ProxyProtocol::Detect {
                timeout: time::Duration::from_secs(5),
            },
            Some(v1::server::ProxyProtocol::Opaque) => ProxyProtocol::Opaque,
            Some(v1::server::ProxyProtocol::Http) => ProxyProtocol::Http,
            Some(v1::server::ProxyProtocol::Grpc) => ProxyProtocol::Grpc,
        }
    }

    fn delete_server(&mut self, srv: v1::Server) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&srv);
        let srv_name = v1::server::Name::from_resource(&srv);
        self.rm_server(ns_name, srv_name)
    }

    fn rm_server(&mut self, ns_name: k8s::NsName, srv_name: v1::server::Name) -> Result<()> {
        let ns = self
            .namespaces
            .get_mut(&ns_name)
            .ok_or_else(|| anyhow!("removing server from non-existent namespace {}", ns_name))?;

        if ns.servers.remove(&srv_name).is_none() {
            bail!("removing non-existent server {}", srv_name);
        }

        // Reset the server config for all pods that were using this server.
        for pod in ns.pods.values_mut() {
            for port in pod.port_lookups.values() {
                let mut sn = port.server_name.lock();
                if sn.as_ref() == Some(&srv_name) {
                    *sn = None;
                    port.tx
                        .send(self.default_config_rx.clone())
                        .expect("pod config receiver must still be held");
                }
            }
        }

        Ok(())
    }

    fn reset_servers(&mut self, srvs: Vec<v1::Server>) -> Result<()> {
        let mut prior_servers = self
            .namespaces
            .iter()
            .map(|(n, ns)| {
                let servers = ns
                    .servers
                    .iter()
                    .map(|(n, s)| (n.clone(), s.meta.clone()))
                    .collect::<HashMap<_, _>>();
                (n.clone(), servers)
            })
            .collect::<HashMap<_, _>>();

        let mut result = Ok(());
        for srv in srvs.into_iter() {
            let ns_name = k8s::NsName::from_resource(&srv);
            let srv_name = v1::server::Name::from_resource(&srv);

            if let Some(prior_servers) = prior_servers.get_mut(&ns_name) {
                if let Some(prior_meta) = prior_servers.remove(&srv_name) {
                    let labels = v1::Labels::from(srv.metadata.labels.clone());
                    let port = srv.spec.port.clone();
                    let protocol = Self::mk_protocol(srv.spec.proxy_protocol.as_ref());
                    let meta = ServerMeta {
                        labels,
                        port,
                        pod_selector: Arc::new(srv.spec.pod_selector.clone()),
                        protocol,
                    };
                    if prior_meta == meta {
                        continue;
                    }
                }
            }

            self.apply_server(srv);
        }

        for (ns_name, ns_servers) in prior_servers.into_iter() {
            for (srv_name, _) in ns_servers.into_iter() {
                if let Err(e) = self.rm_server(ns_name.clone(), srv_name) {
                    result = Err(e);
                }
            }
        }

        result
    }

    // === Authorization indexing ===

    fn apply_authz(&mut self, authorization: v1::Authorization) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&authorization);
        let authz_name = v1::authz::Name::from_resource(&authorization);
        let meta = Self::mk_authz(&ns_name, authorization.spec)?;

        let NsIndex {
            ref mut authzs,
            servers: ref mut ns_servers,
            ..
        } = self.namespaces.entry(ns_name).or_default();

        match authzs.entry(authz_name) {
            HashEntry::Vacant(entry) => {
                for (srv_name, srv) in ns_servers.iter_mut() {
                    let matches = match meta.servers {
                        ServerSelector::Name(ref n) => n == srv_name,
                        ServerSelector::Selector(ref s) => s.matches(&srv.meta.labels),
                    };
                    if matches {
                        srv.add_authz(entry.key(), meta.authz.clone());
                    }
                }
                entry.insert(meta);
            }

            HashEntry::Occupied(mut entry) => {
                // If the authorization changed materially, then update it in all servers.
                if entry.get() != &meta {
                    for (srv_name, srv) in ns_servers.iter_mut() {
                        let matches = match meta.servers {
                            ServerSelector::Name(ref n) => n == srv_name,
                            ServerSelector::Selector(ref s) => s.matches(&srv.meta.labels),
                        };
                        if matches {
                            srv.add_authz(entry.key(), meta.authz.clone());
                        } else {
                            srv.remove_authz(entry.key());
                        }
                    }
                    entry.insert(meta);
                }
            }
        };

        Ok(())
    }

    fn mk_authz(ns_name: &k8s::NsName, spec: v1::authz::AuthorizationSpec) -> Result<AuthzMeta> {
        let servers = {
            let v1::authz::Server { name, selector } = spec.server;
            match (name, selector) {
                (Some(n), None) => ServerSelector::Name(n),
                (None, Some(sel)) => ServerSelector::Selector(sel.into()),
                (Some(_), Some(_)) => bail!("authorization selection is ambiguous"),
                (None, None) => bail!("authorization selects no servers"),
            }
        };

        let authz = match (spec.authenticated, spec.unauthenticated) {
            (Some(auth), None) => {
                let mut identities = Vec::new();
                let mut suffixes = Vec::new();

                if let Some(ids) = auth.identities {
                    for id in ids.into_iter() {
                        if id == "*" {
                            suffixes.push(Vec::new());
                        } else if id.starts_with("*.") {
                            let mut parts = id.split('.');
                            let star = parts.next();
                            debug_assert_eq!(star, Some("*"));
                            suffixes.push(parts.map(|p| p.to_string()).collect());
                        } else {
                            identities.push(id);
                        }
                    }
                }

                if let Some(sas) = auth.service_account_refs {
                    for sa in sas.into_iter() {
                        let ns = sa.namespace.unwrap_or_else(|| ns_name.to_string());
                        let name = sa.name;
                        // FIXME configurable cluster domain
                        identities.push(format!(
                            "{}.{}.serviceaccount.linkerd.cluster.local",
                            ns, name
                        ));
                    }
                }

                if identities.is_empty() && suffixes.is_empty() {
                    bail!("authorization authorizes no clients");
                }

                Authz::Authenticated {
                    identities,
                    suffixes,
                }
            }

            (None, Some(unauth)) if !unauth.networks.is_empty() => {
                let mut nets = Vec::with_capacity(unauth.networks.len());
                for s in unauth.networks.into_iter() {
                    let net = s.parse::<IpNet>()?;
                    nets.push(net);
                }
                Authz::Unauthenticated(nets)
            }

            (Some(_), Some(_)) => {
                bail!("authorization allows both authenticated and unauthenticated clients");
            }
            _ => bail!("authorization authorizes no clients"),
        };

        let authz = Arc::new(authz);
        Ok(AuthzMeta { servers, authz })
    }

    fn delete_authz(&mut self, authz: v1::Authorization) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&authz);
        let authz_name = v1::authz::Name::from_resource(&authz);
        self.rm_authz(ns_name, authz_name)
    }

    fn rm_authz(&mut self, ns_name: k8s::NsName, authz_name: v1::authz::Name) -> Result<()> {
        let ns = self
            .namespaces
            .get_mut(&ns_name)
            .ok_or_else(|| anyhow!("removing authz from non-existent namespace"))?;
        for srv in ns.servers.values_mut() {
            srv.remove_authz(&authz_name);
        }
        Ok(())
    }

    fn reset_authzs(&mut self, authzs: Vec<v1::Authorization>) -> Result<()> {
        let mut prior_authzs = self
            .namespaces
            .iter()
            .map(|(n, ns)| (n.clone(), ns.authzs.clone()))
            .collect::<HashMap<_, _>>();

        let mut result = Ok(());
        for authz in authzs.into_iter() {
            let ns_name = k8s::NsName::from_resource(&authz);
            let authz_name = v1::authz::Name::from_resource(&authz);

            if let Some(prior_ns) = prior_authzs.get_mut(&ns_name) {
                if let Some(prior_authz) = prior_ns.remove(&authz_name) {
                    match Self::mk_authz(&ns_name, authz.spec.clone()) {
                        Ok(meta) => {
                            if prior_authz == meta {
                                continue;
                            }
                        }
                        Err(e) => {
                            result = Err(e);
                            continue;
                        }
                    }
                }
            }

            if let Err(e) = self.apply_authz(authz) {
                result = Err(e);
            }
        }

        for (ns_name, ns_authzs) in prior_authzs.into_iter() {
            for (authz_name, _) in ns_authzs.into_iter() {
                if let Err(e) = self.rm_authz(ns_name.clone(), authz_name) {
                    result = Err(e);
                }
            }
        }

        result
    }
}

// === impl Server ===

impl Server {
    fn add_authz(&mut self, name: &v1::authz::Name, authz: Arc<Authz>) {
        self.authorizations.insert(name.clone(), authz);
        let mut config = self.rx.borrow().clone();
        config.authorizations = self.authorizations.values().cloned().collect();
        self.tx.send(config).expect("config must send")
    }

    fn remove_authz(&mut self, name: &v1::authz::Name) {
        if self.authorizations.remove(name).is_some() {
            let mut config = self.rx.borrow().clone();
            config.authorizations = self.authorizations.values().cloned().collect();
            self.tx.send(config).expect("config must send")
        }
    }
}

// === impl KubeletIps ===

impl KubeletIps {
    pub fn as_nets(&self) -> Vec<IpNet> {
        self.0.iter().copied().map(IpNet::from).collect()
    }
}
