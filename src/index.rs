use crate::v1;
use anyhow::Error;
use dashmap::DashMap;
use futures::prelude::*;
use k8s_openapi::api as k8s;
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::{reflector, watcher};
use serde::de::DeserializeOwned;
use std::{collections::HashMap, fmt, hash::Hash, net::IpAddr, pin::Pin, sync::Arc};
use tokio::sync::watch;
use tracing::{info, instrument};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NodeName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NsName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PodName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SrvName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PodPortKey(NsName, PodName, u16);

type PodPortMap = Arc<DashMap<PodPortKey, PodPort>>;

#[derive(Clone, Debug)]
pub struct Handle {
    pod_ports: PodPortMap,
}

struct Index {
    nodes: Reflect<k8s::core::v1::Node>,
    pods: Reflect<k8s::core::v1::Pod>,
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
pub struct Node {
    kubelet_ips: Vec<IpAddr>,
}

#[derive(Debug)]
pub struct Pod {
    node: Arc<Node>,
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
    rx: Pin<Box<dyn Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static>>,
}

pub fn run(client: kube::Client) -> (Handle, impl Future<Output = Error>) {
    let pod_ports = Arc::new(DashMap::new());
    let idx = Index::new(client).index(pod_ports.clone());

    (Handle { pod_ports }, idx)
}

// === impl Index ===

impl Index {
    fn new(client: kube::Client) -> Self {
        // Needed to know the CIDR for each node (so that connections from kubelet
        // may be authorized).
        let nodes: Reflect<k8s::core::v1::Node> =
            watcher(Api::all(client.clone()), ListParams::default()).into();

        let pods: Reflect<k8s::core::v1::Pod> =
            watcher(Api::all(client.clone()), ListParams::default()).into();

        let authorizations: Reflect<v1::Authorization> =
            watcher(Api::all(client.clone()), ListParams::default()).into();

        let servers: Reflect<v1::Server> = watcher(Api::all(client), ListParams::default()).into();

        Self {
            nodes,
            pods,
            authorizations,
            servers,
        }
    }

    #[instrument(skip(self, _pod_ports), fields(result))]
    async fn index(mut self, _pod_ports: PodPortMap) -> Error {
        // let Self {
        //     mut nodes,
        //     mut pods,
        //     mut servers,
        //     mut authorizations,
        // } = self;

        loop {
            tokio::select! {
                up = self.nodes.recv() => match up {
                    watcher::Event::Applied(_node) => {
                        todo!("Determine kubelet IP for the node")
                    }

                    watcher::Event::Deleted(_node) => {
                        todo!("Handle node delete")
                    }

                    watcher::Event::Restarted(nodes) => {
                        for _node in nodes.into_iter() {
                            todo!("Determine kubelet IPs for all nodes")
                        }
                    }
                },

                up = self.pods.recv() => match up {
                    watcher::Event::Applied(pod) => {
                        let _n = NsName::from_resource(&pod);
                        todo!("Handle pod apply")
                    }

                    watcher::Event::Deleted(pod) => {
                        let _n = NsName::from_resource(&pod);
                        todo!("Handle pod delete")
                    }

                    watcher::Event::Restarted(pods) => {
                        for pod in pods.into_iter() {
                            let _n = NsName::from_resource(&pod);
                            todo!("Handle ns delete")
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

                up = self.servers.recv() => match up {
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
        let rx = reflector(store, watch).boxed();
        Self { cache, rx }
    }
}

impl<T> Reflect<T>
where
    T: Resource + Clone + DeserializeOwned + fmt::Debug + Send + Sync + 'static,
    T::DynamicType: Clone + Eq + Hash + Default,
{
    async fn recv(&mut self) -> watcher::Event<T> {
        loop {
            match self
                .rx
                .next()
                .await
                .expect("Reflect stream must not terminate")
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
