use crate::v1;
use anyhow::{anyhow, Error, Result};
use dashmap::DashMap;
use futures::prelude::*;
use k8s_openapi::api as k8s;
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::{reflector, watcher};
use serde::de::DeserializeOwned;
use std::{
    collections::{hash_map, HashMap, HashSet},
    fmt,
    hash::Hash,
    net::IpAddr,
    pin::Pin,
    sync::Arc,
};
use tokio::sync::watch;
use tracing::{info, instrument, warn};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NodeName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NsName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PodName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SrvName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct KubeletIps(Arc<Vec<IpAddr>>);

type PodPortMap = Arc<DashMap<(NsName, PodName), Pod>>;

#[derive(Clone, Debug, Default)]
pub struct Handle {
    pod_ports: PodPortMap,
}

struct Index {
    nodes: Watch<k8s::core::v1::Node>,
    node_ips: HashMap<NodeName, KubeletIps>,
    pods: Watch<k8s::core::v1::Pod>,
    pod_labels: HashMap<(NsName, PodName), v1::Labels>,
    servers: Reflect<v1::Server>,
    authorizations: Reflect<v1::Authorization>,
}

#[derive(Debug)]
struct NsPods {
    pods: DashMap<PodName, Pod>,
}

#[derive(Clone, Debug)]
pub struct Server {}

#[derive(Debug)]
struct ServerChannel {
    rx: watch::Receiver<Server>,
    tx: watch::Sender<Server>,
}

#[derive(Debug)]
pub struct Pod {
    kubelet: KubeletIps,
    servers: Arc<HashMap<u16, PodPort>>,
}

#[derive(Debug)]
pub struct PodPort {
    rx: watch::Receiver<watch::Receiver<Server>>,
    tx: watch::Sender<watch::Receiver<Server>>,
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
    let h = Handle::default();
    (h.clone(), Index::new(client).index(h))
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

        Self {
            nodes,
            node_ips: HashMap::default(),
            pods,
            pod_labels: HashMap::default(),
            authorizations,
            servers,
        }
    }

    fn cidr_to_kubelet_ip(cidr: String) -> Result<IpAddr> {
        cidr.parse::<ipnet::IpNet>()?
            .hosts()
            .next()
            .ok_or_else(|| anyhow!("pod CIDR network is empty"))
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

    fn update_pod(&mut self, pod: k8s::core::v1::Pod) -> Result<Option<((NsName, PodName), Pod)>> {
        let ns = NsName::from_resource(&pod);
        let pn = PodName::new(&pod);
        let key = (ns, pn);
        let spec = pod.spec.ok_or_else(|| anyhow!("pod missing spec"))?;

        // Pod label changes may alter a pod's policy at
        // runtime. Determine whether there are new labels for
        // this pod before doing any linking.
        let labels = v1::Labels::from(pod.metadata.labels);
        let _labels = match self.pod_labels.entry(key.clone()) {
            hash_map::Entry::Vacant(e) => {
                e.insert(labels.clone());
                labels
            }
            hash_map::Entry::Occupied(mut e) => {
                if e.get() == &labels {
                    return Ok(None);
                } else {
                    e.insert(labels.clone());
                    labels
                }
            }
        };

        let kubelet = {
            let n = spec
                .node_name
                .map(|n| NodeName(n.into()))
                .ok_or_else(|| anyhow!("pod missing node name"))?;
            self.node_ips
                .get(&n)
                .ok_or_else(|| anyhow!("node IP does not exist for node {}", n))?
                .clone()
        };

        let servers = Arc::new(HashMap::default());

        // TODO discover servers for port.

        let pod = Pod { kubelet, servers };

        Ok(Some((key, pod)))
    }

    #[instrument(skip(self, pod_ports), fields(result))]
    async fn index(mut self, Handle { pod_ports }: Handle) -> Error {
        loop {
            tokio::select! {
                // Track the kubelet IPs for all nodes.
                up = self.nodes.recv() => match up {
                    watcher::Event::Applied(node) => {
                        let n = NodeName::new(&node);
                        if let hash_map::Entry::Vacant(entry) = self.node_ips.entry(n)  {
                            match Self::kubelet_ips(node) {
                                Ok(ips) => {
                                    entry.insert(ips);
                                }
                                Err(error) => warn!(%error, "Failed to load kubelet IPs"),
                            }
                        }
                    }
                    watcher::Event::Deleted(node) => {
                        let n = NodeName::new(&node);
                        self.node_ips.remove(&n);
                    }
                    watcher::Event::Restarted(nodes) => {
                        let mut old_names = self.node_ips.keys().cloned().collect::<HashSet<_>>();
                        for node in nodes.into_iter() {
                            let n = NodeName::new(&node);
                            if !old_names.remove(&n) {
                                match Self::kubelet_ips(node) {
                                    Ok(ips) => {
                                        self.node_ips.insert(n, ips);
                                    }
                                    Err(error) => warn!(node = %n, %error, "Failed to load kubelet IPs"),
                                }
                            }
                        }
                        for n in old_names.into_iter() {
                            self.node_ips.remove(&n);
                        }
                    }
                },

                up = self.pods.recv() => match up {
                    watcher::Event::Applied(pod) => {
                        match self.update_pod(pod) {
                            Ok(None) => {}
                            Ok(Some((key, pod))) => {
                                pod_ports.insert(key, pod);
                            }
                            Err(error) => warn!(%error, "Could not update pod"),
                        }
                    }

                    watcher::Event::Deleted(pod) => {
                        let ns = NsName::from_resource(&pod);
                        let pn = PodName::new(&pod);
                        let key = (ns, pn);
                        self.pod_labels.remove(&key);
                        pod_ports.remove(&key);
                        // Dropping the pod port should indicate to all watchers
                        // that no further updates are possible.
                    }

                    watcher::Event::Restarted(pods) => {
                        for pod in pods.into_iter() {
                            let _n = NsName::from_resource(&pod);
                            todo!("Handle ns delete")
                        }
                    }
                },

                up = self.servers.rx.recv() => match up {
                    watcher::Event::Applied(srv) => {
                        let _n = NsName::from_resource(&srv);
                        todo!("Handle srv apply")
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
    pub async fn lookup(
        &self,
        _ns: &str,
        _name: &str,
        _port: u16,
    ) -> Option<watch::Receiver<watch::Receiver<Server>>> {
        todo!("Support lookup")
    }
}

// === NodeName ===

impl NodeName {
    fn new(n: &k8s::core::v1::Node) -> Self {
        Self(n.name().into())
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === NsName ===

impl NsName {
    fn from_resource<T: Resource>(t: &T) -> Self {
        let ns = t.namespace().unwrap_or_else(|| "default".into());
        Self(ns.into())
    }
}

impl fmt::Display for NsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === PodName ===

impl PodName {
    fn new(p: &k8s::core::v1::Pod) -> Self {
        Self(p.name().into())
    }
}

impl fmt::Display for PodName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === SrvName ===

impl fmt::Display for SrvName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
