use crate::v1;
use anyhow::{anyhow, Context, Error, Result};
use dashmap::{mapref::entry::Entry as DashEntry, DashMap};
use futures::prelude::*;
use ipnet::IpNet;
use k8s_openapi::api as k8s;
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::watcher;
use serde::de::DeserializeOwned;
use std::{
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
    fmt,
    hash::Hash,
    net::IpAddr,
    pin::Pin,
    sync::Arc,
};
use tokio::{sync::watch, time};
use tracing::{debug, info, instrument, warn};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct NodeName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NsName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PodName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PortName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum ServerPort {
    Number(u16),
    Name(PortName),
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SrvName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct AuthzName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct KubeletIps(Arc<Vec<IpAddr>>);

type SharedLookupMap = Arc<DashMap<NsName, DashMap<PodName, Arc<HashMap<u16, Lookup>>>>>;

#[derive(Clone, Debug)]
pub struct Handle(SharedLookupMap);

struct Index {
    nodes: Watch<k8s::core::v1::Node>,
    pods: Watch<k8s::core::v1::Pod>,
    servers: Watch<v1::Server>,
    authorizations: Watch<v1::Authorization>,

    node_ips: HashMap<NodeName, KubeletIps>,
    namespaces: HashMap<NsName, NsIndex>,
    lookups: SharedLookupMap,

    default_config_rx: watch::Receiver<ServerConfig>,
    _default_config_tx: watch::Sender<ServerConfig>,
}

#[derive(Debug, Default)]
struct NsIndex {
    /// Caches pod labels so we can differentiate innocuous updates (like status
    /// changes) from label changes that could impact server indexing.
    pods: HashMap<PodName, Pod>,

    /// Caches a watch for each server.
    servers: HashMap<SrvName, Server>,

    authzs: HashMap<AuthzName, AuthzMeta>,
}

#[derive(Debug)]
struct Pod {
    port_names: Arc<HashMap<PortName, u16>>,
    port_lookups: Arc<HashMap<u16, watch::Sender<watch::Receiver<ServerConfig>>>>,
    labels: v1::Labels,
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

#[derive(Debug, PartialEq, Eq)]
pub struct AuthzMeta {
    pub server_selector: v1::labels::Selector,
    pub authz: Arc<Authz>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Authz {
    pub networks: Vec<IpNet>,
    pub tls: Option<Tls>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Tls {
    pub identities: Vec<Vec<String>>,
    pub suffixes: Vec<Vec<String>>,
}

#[derive(Debug)]
struct Server {
    port: ServerPort,
    pod_selector: v1::labels::Selector,
    protocol: ProxyProtocol,
    labels: v1::Labels,
    //authorizations: HashMap<AuthzName, Arc<Authz>>,
    rx: watch::Receiver<ServerConfig>,
    tx: watch::Sender<ServerConfig>,
}

#[derive(Debug)]
pub struct PodLookups {
    servers: Arc<HashMap<u16, Lookup>>,
}

#[derive(Debug)]
pub struct PodPort {
    lookup: Lookup,
}

#[derive(Clone, Debug)]
pub struct Lookup {
    pub name: Option<PortName>,
    pub kubelet_ips: KubeletIps,
    pub server: watch::Receiver<watch::Receiver<ServerConfig>>,
}

struct Watch<T>(Pin<Box<dyn Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static>>);

pub fn run(client: kube::Client) -> (Handle, impl Future<Output = Error>) {
    let idx = Index::new(client);
    let h = Handle(idx.lookups.clone());
    (h, idx.index())
}

// === impl Index ===

impl Index {
    fn new(client: kube::Client) -> Self {
        // Needed to know the CIDR for each node (so that connections from kubelet
        // may be authorized).
        let nodes: Watch<k8s::core::v1::Node> =
            watcher(Api::all(client.clone()), ListParams::default()).into();

        let pods: Watch<k8s::core::v1::Pod> =
            watcher(Api::all(client.clone()), ListParams::default()).into();

        let authorizations: Watch<v1::Authorization> =
            watcher(Api::all(client.clone()), ListParams::default()).into();

        let servers: Watch<v1::Server> = watcher(Api::all(client), ListParams::default()).into();

        let (_default_config_tx, default_config_rx) = watch::channel(ServerConfig {
            protocol: ProxyProtocol::Detect {
                timeout: time::Duration::from_secs(5),
            },
            authorizations: Vec::default(),
        });

        Self {
            nodes,
            pods,
            authorizations,
            servers,
            node_ips: HashMap::default(),
            namespaces: HashMap::default(),
            lookups: Default::default(),
            default_config_rx,
            _default_config_tx,
        }
    }

    #[instrument(skip(self), fields(result))]
    async fn index(mut self) -> Error {
        loop {
            tokio::select! {
                // Track the kubelet IPs for all nodes.
                up = self.nodes.recv() => {
                    let res = match up {
                        watcher::Event::Applied(node) => self.apply_node(node),
                        watcher::Event::Deleted(node) => self.delete_node(node),
                        watcher::Event::Restarted(nodes) => self.reset_nodes(nodes),
                    };
                    if let Err(error) = res {
                        debug!(%error);
                    }
                }

                up = self.pods.recv() => {
                    let res = match up {
                        watcher::Event::Applied(pod) => self.apply_pod(pod),
                        watcher::Event::Deleted(pod) => self.delete_pod(pod),
                        watcher::Event::Restarted(pods) => self.reset_pods(pods),
                    };
                    if let Err(error) = res {
                        debug!(%error);
                    }
                }

                up = self.servers.recv() => match up {
                    watcher::Event::Applied(srv) => self.apply_server(srv),

                    watcher::Event::Deleted(srv) => {
                        let _n = NsName::from_resource(&srv);
                        todo!("Handle srv delete")
                    }

                    watcher::Event::Restarted(srvs) => {
                        for srv in srvs.into_iter() {
                            let _n = NsName::from_resource(&srv);
                            todo!("Handle srv delete")
                        }
                    }
                },

                up = self.authorizations.recv() => match up {
                    watcher::Event::Applied(authz) => {
                        let _n = NsName::from_resource(&authz);
                        todo!("Handle authz apply")
                    }

                    watcher::Event::Deleted(authz) => {
                        let _n = NsName::from_resource(&authz);
                        todo!("Handle authz delete")
                    }

                    watcher::Event::Restarted(authzs) => {
                        for authz in authzs.into_iter() {
                            let _n = NsName::from_resource(&authz);
                            todo!("Handle authz delete")
                        }
                    }
                },
            }
        }
    }

    // === Node->IP indexing ===

    fn apply_node(&mut self, node: k8s::core::v1::Node) -> Result<()> {
        let name = NodeName::from_resource(&node);
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

    fn delete_node(&mut self, node: k8s::core::v1::Node) -> Result<()> {
        let name = NodeName::from_resource(&node);
        if self.node_ips.remove(&name).is_some() {
            debug!(%name, "Node deleted");
            Ok(())
        } else {
            Err(anyhow!("node {} already deleted", name))
        }
    }

    fn reset_nodes(&mut self, nodes: Vec<k8s::core::v1::Node>) -> Result<()> {
        // Avoid rebuilding data for nodes that have not changed.
        let mut prior_names = self.node_ips.keys().cloned().collect::<HashSet<_>>();

        let mut result = Ok(());
        for node in nodes.into_iter() {
            let name = NodeName::from_resource(&node);
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

    fn kubelet_ips(node: k8s::core::v1::Node) -> Result<KubeletIps> {
        let spec = node.spec.ok_or_else(|| anyhow!("node missing spec"))?;
        let cidr = spec
            .pod_cidr
            .ok_or_else(|| anyhow!("node missing pod_cidr"))?;
        let mut addrs = Vec::new();

        let ip = Self::cidr_to_kubelet_ip(cidr)?;
        addrs.push(ip);
        if let Some(cidrs) = spec.pod_cidrs {
            for cidr in cidrs.into_iter() {
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

    fn apply_pod(&mut self, pod: k8s::core::v1::Pod) -> Result<()> {
        let ns_name = NsName::from_resource(&pod);
        let pod_name = PodName::from_resource(&pod);
        let spec = pod.spec.ok_or_else(|| anyhow!("pod missing spec"))?;

        let NsIndex {
            ref mut pods,
            ref mut servers,
            ..
        } = self.namespaces.entry(ns_name.clone()).or_default();
        let lookups = self.lookups.entry(ns_name).or_default();

        match (pods.entry(pod_name.clone()), lookups.entry(pod_name)) {
            (HashEntry::Vacant(pod_entry), DashEntry::Vacant(lookups_entry)) => {
                let labels = v1::Labels::from(pod.metadata.labels);

                let kubelet = {
                    let name = spec
                        .node_name
                        .map(|n| NodeName(n.into()))
                        .ok_or_else(|| anyhow!("pod missing node name"))?;
                    self.node_ips
                        .get(&name)
                        .ok_or_else(|| anyhow!("node IP does not exist for node {}", name))?
                        .clone()
                };

                let mut port_names = HashMap::new();
                let mut port_txs = HashMap::new();
                let mut lookups = HashMap::new();
                for container in spec.containers.into_iter() {
                    if let Some(ps) = container.ports {
                        for p in ps.into_iter() {
                            if p.protocol.map(|p| p == "TCP").unwrap_or(true) {
                                let port = p.container_port as u16;
                                if port_txs.contains_key(&port) {
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
                                let lookup = Lookup {
                                    name,
                                    server,
                                    kubelet_ips: kubelet.clone(),
                                };
                                port_txs.insert(port, tx);
                                lookups.insert(port, lookup);
                            }
                        }
                    }
                }

                for server in servers.values() {
                    let tx = match server.port {
                        ServerPort::Number(ref p) => port_txs.get(p),
                        ServerPort::Name(ref n) => port_names.get(n).and_then(|p| port_txs.get(p)),
                    };
                    if let Some(tx) = tx {
                        if server.pod_selector.matches(&labels) {
                            tx.send(server.rx.clone())
                                .expect("pod config receiver must be set");
                        }
                    }
                }

                lookups_entry.insert(Arc::new(lookups));
                pod_entry.insert(Pod {
                    port_names: port_names.into(),
                    port_lookups: port_txs.into(),
                    labels,
                });
            }

            (HashEntry::Occupied(mut pod_entry), DashEntry::Occupied(_)) => {
                let labels = v1::Labels::from(pod.metadata.labels);

                if pod_entry.get().labels != labels {
                    for server in servers.values() {
                        let tx = match server.port {
                            ServerPort::Number(ref p) => pod_entry.get().port_lookups.get(p),
                            ServerPort::Name(ref n) => pod_entry
                                .get()
                                .port_names
                                .get(n)
                                .and_then(|p| pod_entry.get().port_lookups.get(p)),
                        };

                        if let Some(tx) = tx {
                            if server.pod_selector.matches(&labels) {
                                tx.send(server.rx.clone())
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

    fn delete_pod(&mut self, pod: k8s::core::v1::Pod) -> Result<()> {
        let ns_name = NsName::from_resource(&pod);
        let pod_name = PodName::from_resource(&pod);
        self.rm_pod(&ns_name, &pod_name)
    }

    fn rm_pod(&mut self, ns: &NsName, pod: &PodName) -> Result<()> {
        self.namespaces
            .get_mut(&ns)
            .ok_or_else(|| anyhow!("namespace {} doesn't exist", ns))?
            .pods
            .remove(&pod)
            .ok_or_else(|| anyhow!("pod {} doesn't exist", pod))?;

        self.lookups
            .get_mut(&ns)
            .ok_or_else(|| anyhow!("namespace {} doesn't exist", ns))?
            .remove(&pod)
            .ok_or_else(|| anyhow!("pod {} doesn't exist", pod))?;

        Ok(())
    }

    fn reset_pods(&mut self, pods: Vec<k8s::core::v1::Pod>) -> Result<()> {
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
            let ns_name = NsName::from_resource(&pod);
            let pod_name = PodName::from_resource(&pod);

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
        let ns_name = NsName::from_resource(&srv);
        let srv_name = SrvName::from_resource(&srv);
        let labels = v1::Labels::from(srv.metadata.labels);

        let NsIndex {
            ref pods,
            ref authzs,
            ref mut servers,
        } = self.namespaces.entry(ns_name).or_default();

        let port = match srv.spec.port {
            v1::server::Port::Number(n) => ServerPort::Number(n),
            v1::server::Port::Name(n) => ServerPort::Name(n.into()),
        };

        match servers.entry(srv_name) {
            HashEntry::Occupied(mut entry) => {
                let s = entry.get_mut();
                let protocol = Self::mk_protocol(srv.spec.proxy_protocol.as_ref());

                // If something about the server changed, we need to update the
                // config to reflect the change.
                if s.labels != labels || s.protocol != protocol {
                    // NB: Only a single task applies server updates, so it's
                    // okay to borrow a version, modify, and send it.  We don't
                    // need a lock because serialization is guaranteed.
                    let mut config = s.rx.borrow().clone();

                    if s.labels != labels {
                        config.authorizations = authzs
                            .values()
                            .filter_map(|a| {
                                if a.server_selector.matches(&labels) {
                                    Some(a.authz.clone())
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>();
                        s.labels = labels;
                    }

                    config.protocol = protocol.clone();
                    s.protocol = protocol;

                    s.tx.send(config).expect("server update must succeed");
                }

                // If the pod/port selector didn't change, we don't need to
                // refresh the index.
                if s.pod_selector == srv.spec.pod_selector && s.port == port {
                    return;
                }

                s.pod_selector = srv.spec.pod_selector;
                s.port = port;
            }

            HashEntry::Vacant(entry) => {
                let protocol = Self::mk_protocol(srv.spec.proxy_protocol.as_ref());
                fn authzs() -> Vec<Arc<Authz>> {
                    todo!()
                }
                let (tx, rx) = watch::channel(ServerConfig {
                    protocol: protocol.clone(),
                    authorizations: authzs(),
                });
                entry.insert(Server {
                    port,
                    pod_selector: srv.spec.pod_selector,
                    protocol,
                    labels,
                    tx,
                    rx,
                });
            }
        }

        // If we've updated the server->pod selection, then we need to reindex
        // all pods and servers.
        for pod in pods.values() {
            for srv in servers.values() {
                let tx = match srv.port {
                    ServerPort::Number(ref p) => pod.port_lookups.get(p),
                    ServerPort::Name(ref n) => {
                        pod.port_names.get(&n).and_then(|p| pod.port_lookups.get(p))
                    }
                };

                if let Some(tx) = tx {
                    if srv.pod_selector.matches(&pod.labels) {
                        // It's up to the lookup stream to de-duplicate updates.
                        tx.send(srv.rx.clone())
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

    // === Authorization indexing ===

    fn apply_authz(&mut self, authz: v1::Authorization) {
        let ns_name = NsName::from_resource(&authz);
        let authz_name = AuthzName::from_resource(&authz);

        let NsIndex { ref mut authzs, .. } = self.namespaces.entry(ns_name).or_default();

        match authzs.entry(authz_name) {
            HashEntry::Vacant(entry) => {
                entry.insert(AuthzMeta {
                    server_selector: authz.spec.server_selector,
                    authz: Arc::new(Authz {
                        networks: authz.spec.
                    })
                });
            }
            HashEntry::Occupied(_) => todo!("update authz"),
        }
    }
}

// === impl Watch ===

impl<T, W> From<W> for Watch<T>
where
    W: Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static,
{
    fn from(watch: W) -> Self {
        Watch(watch.boxed())
    }
}

impl<T> Watch<T>
where
    T: Resource + Clone + DeserializeOwned + fmt::Debug + Send + Sync + 'static,
    T::DynamicType: Clone + Eq + Hash + Default,
{
    async fn recv(&mut self) -> watcher::Event<T> {
        loop {
            match self
                .0
                .next()
                .await
                .expect("watch stream must not terminate")
            {
                Ok(ev) => return ev,
                Err(error) => info!(%error, "Disconnected"),
            }
        }
    }
}

// === impl Handle ===

impl Handle {
    pub fn lookup(&self, ns: NsName, name: PodName, port: u16) -> Option<Lookup> {
        let ns = self.0.get(&ns)?;
        let pod = ns.get(&name)?;
        let lookup = pod.get(&port)?;
        Some(lookup.clone())
    }
}

trait FromResource<T> {
    fn from_resource(resource: &T) -> Self;
}

// === NodeName ===

impl FromResource<k8s::core::v1::Node> for NodeName {
    fn from_resource(n: &k8s::core::v1::Node) -> Self {
        Self(n.name().into())
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === NsName ===

impl<T: Resource> FromResource<T> for NsName {
    fn from_resource(t: &T) -> Self {
        t.namespace().unwrap_or_else(|| "default".into()).into()
    }
}

impl<T: Into<Arc<str>>> From<T> for NsName {
    fn from(ns: T) -> Self {
        Self(ns.into())
    }
}

impl fmt::Display for NsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === PodName ===

impl FromResource<k8s::core::v1::Pod> for PodName {
    fn from_resource(p: &k8s::core::v1::Pod) -> Self {
        Self(p.name().into())
    }
}

impl<T: Into<Arc<str>>> From<T> for PodName {
    fn from(pod: T) -> Self {
        Self(pod.into())
    }
}

impl fmt::Display for PodName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === PortName ===

impl<T: Into<Arc<str>>> From<T> for PortName {
    fn from(p: T) -> Self {
        Self(p.into())
    }
}

impl fmt::Display for PortName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === SrvName ===

impl FromResource<v1::Server> for SrvName {
    fn from_resource(s: &v1::Server) -> Self {
        Self(s.name().into())
    }
}

impl fmt::Display for SrvName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === AuthzName ===

impl FromResource<v1::Authorization> for AuthzName {
    fn from_resource(s: &v1::Authorization) -> Self {
        Self(s.name().into())
    }
}

impl fmt::Display for AuthzName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
