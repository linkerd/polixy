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
use kube_runtime::{reflector, watcher};
use reflector::ObjectRef;
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
struct SrvName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct KubeletIps(Arc<Vec<IpAddr>>);

type SharedPodMap = Arc<DashMap<NsName, DashMap<PodName, Pod>>>;

#[derive(Clone, Debug)]
pub struct Handle(SharedPodMap);

struct Index {
    nodes: Watch<k8s::core::v1::Node>,
    pods: Watch<k8s::core::v1::Pod>,
    servers: Reflect<v1::Server>,
    authorizations: Reflect<v1::Authorization>,
    node_ips: HashMap<NodeName, KubeletIps>,
    namespaces: HashMap<NsName, NsIndex>,
    pod_states: SharedPodMap,

    default_config_rx: watch::Receiver<ServerConfig>,
    _default_config_tx: watch::Sender<ServerConfig>,
}

#[derive(Debug, Default)]
struct NsIndex {
    /// Caches pod labels so we can differentiate innocuous updates (like status
    /// changes) from label changes that could impact server indexing.
    pod_info: HashMap<PodName, PodInfo>,

    /// Caches a watch for each server.
    servers: HashMap<SrvName, Server>,
}

#[derive(Debug)]
struct PodInfo {
    port_names: Arc<HashMap<PortName, u16>>,
    labels: v1::Labels,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub protocol: ProxyProtocol,
    pub authorizations: Vec<Arc<Authz>>,
}

#[derive(Clone, Debug)]
pub enum ProxyProtocol {
    Detect { timeout: time::Duration },
    Opaque,
    Http,
    Grpc,
}

#[derive(Debug)]
pub struct Authz {
    pub networks: Vec<IpNet>,
    pub tls: Option<Tls>,
}

#[derive(Debug)]
pub struct Tls {
    pub identities: Vec<Vec<String>>,
    pub suffixes: Vec<Vec<String>>,
}

#[derive(Debug)]
struct Server {
    port: v1::server::Port,
    pod_selector: v1::labels::Selector,
    labels: v1::Labels,
    rx: watch::Receiver<ServerConfig>,
    tx: watch::Sender<ServerConfig>,
}

#[derive(Debug)]
pub struct Pod {
    servers: Arc<HashMap<u16, PodPort>>,
}

#[derive(Debug)]
pub struct PodPort {
    lookup: Lookup,
    tx: watch::Sender<watch::Receiver<ServerConfig>>,
}

#[derive(Clone, Debug)]
pub struct Lookup {
    pub name: Option<PortName>,
    pub kubelet_ips: KubeletIps,
    pub server: watch::Receiver<watch::Receiver<ServerConfig>>,
}

struct Reflect<T>
where
    T: Resource + 'static,
    T::DynamicType: Eq + Hash,
{
    cache: reflector::Store<T>,
    rx: Watch<T>,
}

struct Watch<T>(Pin<Box<dyn Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static>>);

pub fn run(client: kube::Client) -> (Handle, impl Future<Output = Error>) {
    let idx = Index::new(client);
    let h = Handle(idx.pod_states.clone());
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

        let authorizations: Reflect<v1::Authorization> =
            watcher(Api::all(client.clone()), ListParams::default()).into();

        let servers: Reflect<v1::Server> = watcher(Api::all(client), ListParams::default()).into();

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
            pod_states: Default::default(),
            default_config_rx,
            _default_config_tx,
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
            ref mut pod_info,
            ref mut servers,
        } = self.namespaces.entry(ns_name.clone()).or_default();
        let pod_states = self.pod_states.entry(ns_name).or_default();

        let info_entry = pod_info.entry(pod_name.clone());
        let state_entry = pod_states.entry(pod_name);
        match (info_entry, state_entry) {
            (HashEntry::Vacant(info_entry), DashEntry::Vacant(pod_entry)) => {
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

                let mut port_names = HashMap::<PortName, u16>::new();
                let mut pod_ports = HashMap::new();
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
                                let lookup = Lookup {
                                    name,
                                    server,
                                    kubelet_ips: kubelet.clone(),
                                };
                                pod_ports.insert(port, PodPort { tx, lookup });
                            }
                        }
                    }
                }

                for server in servers.values() {
                    let pod_port = match server.port.clone() {
                        v1::server::Port::Number(p) => pod_ports.get(&p),
                        v1::server::Port::Name(n) => {
                            let name: PortName = n.into();
                            port_names.get(&name).and_then(|p| pod_ports.get(p))
                        }
                    };
                    if let Some(pod_port) = pod_port {
                        if server.pod_selector.matches(&labels) {
                            pod_port
                                .tx
                                .send(server.rx.clone())
                                .expect("pod config receiver must be set");
                        }
                    }
                }

                pod_entry.insert(Pod {
                    servers: pod_ports.into(),
                });
                info_entry.insert(PodInfo {
                    labels,
                    port_names: port_names.into(),
                });
            }

            (HashEntry::Occupied(mut info_entry), DashEntry::Occupied(pod_entry)) => {
                let labels = v1::Labels::from(pod.metadata.labels);

                if info_entry.get().labels != labels {
                    for server in servers.values() {
                        let pod_port = match server.port.clone() {
                            v1::server::Port::Number(p) => pod_entry.get().servers.get(&p),
                            v1::server::Port::Name(n) => {
                                let name: PortName = n.into();
                                info_entry
                                    .get()
                                    .port_names
                                    .get(&name)
                                    .and_then(|p| pod_entry.get().servers.get(p))
                            }
                        };
                        if let Some(pod_port) = pod_port {
                            if server.pod_selector.matches(&labels) {
                                pod_port
                                    .tx
                                    .send(server.rx.clone())
                                    .expect("pod config receiver must be set");
                            }
                        }
                    }

                    info_entry.get_mut().labels = labels;
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
        self.pod_states
            .get_mut(&ns)
            .ok_or_else(|| anyhow!("namespace {} doesn't exist", ns))?
            .remove(&pod)
            .ok_or_else(|| anyhow!("pod {} doesn't exist", pod))?;

        self.namespaces
            .get_mut(&ns)
            .ok_or_else(|| anyhow!("namespace {} doesn't exist", ns))?
            .pod_info
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
                    .pod_info
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

                up = self.servers.rx.recv() => match up {
                    watcher::Event::Applied(srv) => {
                        let ns_name = NsName::from_resource(&srv);
                        let srv_name = SrvName::from_resource(&srv);
                        let labels = v1::Labels::from(srv.metadata.labels);
                        let ns = self.namespaces.entry(ns_name).or_default();
                        match ns.servers.entry(srv_name) {
                            HashEntry::Occupied(mut entry) => {
                                entry.get_mut().labels = labels;
                                todo!("Update pods");
                            }
                            HashEntry::Vacant(entry) => {
                                fn new_server_config() -> ServerConfig {
                                    todo!("build a server config");
                                }
                                let (tx, rx) = watch::channel(new_server_config());
                                entry.insert(Server {
                                    port: srv.spec.port,
                                    pod_selector: srv.spec.pod_selector,
                                    labels,
                                    tx,
                                    rx,
                                });
                                todo!("Update pods");
                            }
                        }

                    }

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

                up = self.authorizations.rx.recv() => match up {
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
}

// === impl Reflect ===

impl<T, W> From<W> for Reflect<T>
where
    T: Resource + Clone + DeserializeOwned + fmt::Debug + Send + Sync + 'static,
    T::DynamicType: Clone + Eq + Hash + Default,
    W: Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static,
{
    fn from(watch: W) -> Self {
        let store = reflector::store::Writer::<T>::default();
        let cache = store.as_reader();
        let rx = Watch(reflector(store, watch).boxed());
        Self { cache, rx }
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
        let server = pod.servers.get(&port)?;
        Some(server.lookup.clone())
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
