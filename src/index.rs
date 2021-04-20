use crate::{authz::Authorization, grpc::proto, server::Server};
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
use std::{fmt, hash::Hash, pin::Pin, sync::Arc};
use tokio::sync::watch;
use tracing::{info, instrument};

pub type Lookup = watch::Receiver<proto::InboundProxyConfig>;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NsName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SrvName(Arc<str>);

#[derive(Clone, Debug)]
pub struct Handle {
    servers: reflector::Store<Server>,
    authorizations: reflector::Store<Authorization>,
    configs: Arc<DashMap<NsName, Arc<NsConfigs>>>,
}

#[derive(Debug)]
pub struct NsConfigs {
    configs: DashMap<SrvName, Config>,
}

#[derive(Debug)]
struct Config {
    rx: watch::Receiver<proto::InboundProxyConfig>,
    tx: watch::Sender<proto::InboundProxyConfig>,
}

struct Watch<T>
where
    T: Resource + 'static,
    T::DynamicType: Eq + Hash,
{
    cache: reflector::Store<T>,
    rx: Pin<Box<dyn Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static>>,
}

pub fn run(client: kube::Client) -> (Handle, impl Future<Output = Error>) {
    // Needed to know the CIDR for each node (so that connections from kubelet
    // may be authorized).
    let nodes: Watch<k8s::core::v1::Node> =
        watcher(Api::all(client.clone()), ListParams::default()).into();

    // Tracks all namespaces so their labels are known for account matching.
    let namespaces: Watch<k8s::core::v1::Namespace> =
        watcher(Api::all(client.clone()), ListParams::default()).into();

    // Tracks all service accounts so their labels are known for authorization.
    let accounts: Watch<k8s::core::v1::ServiceAccount> =
        watcher(Api::all(client.clone()), ListParams::default()).into();

    let authorizations: Watch<Authorization> =
        watcher(Api::all(client.clone()), ListParams::default()).into();

    let servers: Watch<Server> = watcher(Api::all(client), ListParams::default()).into();

    let configs = Arc::new(DashMap::new());

    let handle = Handle {
        configs: configs.clone(),
        servers: servers.cache.clone(),
        authorizations: authorizations.cache.clone(),
    };

    let idx = index(
        nodes,
        namespaces,
        accounts,
        servers,
        authorizations,
        configs,
    );

    (handle, idx)
}

#[instrument(
    skip(nodes, namespaces, accounts, servers, authorizations, _configs),
    fields(result)
)]
async fn index(
    mut nodes: Watch<k8s::core::v1::Node>,
    mut namespaces: Watch<k8s::core::v1::Namespace>,
    mut accounts: Watch<k8s::core::v1::ServiceAccount>,
    mut servers: Watch<Server>,
    mut authorizations: Watch<Authorization>,
    _configs: Arc<DashMap<NsName, Arc<NsConfigs>>>,
) -> Error {
    loop {
        tokio::select! {
            up = nodes.recv() => match up {
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

            up = namespaces.recv() => match up {
                watcher::Event::Applied(ns) => {
                    let _n = NsName::from_resource(&ns);
                    todo!("Handle ns apply")
                }

                watcher::Event::Deleted(ns) => {
                    let _n = NsName::from_resource(&ns);
                    todo!("Handle ns delete")
                }

                watcher::Event::Restarted(nss) => {
                    for ns in nss.into_iter() {
                        let _n = NsName::from_resource(&ns);
                        todo!("Handle ns delete")
                    }
                }
            },

            up = accounts.recv() => match up {
                watcher::Event::Applied(sa) => {
                    let _n = NsName::from_resource(&sa);
                    todo!("Handle sa apply")
                }

                watcher::Event::Deleted(sa) => {
                    let _n = NsName::from_resource(&sa);
                    todo!("Handle sa delete")
                }

                watcher::Event::Restarted(sas) => {
                    for sa in sas.into_iter() {
                        let _n = NsName::from_resource(&sa);
                        todo!("Handle sa restart")
                    }
                }
            },

            up = authorizations.recv() => match up {
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

            up = servers.recv() => match up {
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

// === impl Watch ===

impl<T, W> From<W> for Watch<T>
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

impl<T> Watch<T>
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
                .expect("Watch stream must not terminate")
            {
                Ok(ev) => return ev,
                Err(error) => info!(%error, "Disconnected"),
            }
        }
    }
}

// === impl Handle ===

impl Handle {
    pub async fn lookup(&self, _ns: &str, _name: &str, _port: u16) -> Option<Lookup> {
        todo!("Support lookup")
    }
}

// === NsName ===

impl NsName {
    fn from_resource<T: Resource>(t: &T) -> Self {
        let ns = t.namespace().unwrap_or_else(|| "default".into());
        Self::from(ns)
    }
}

impl<T> From<T> for NsName
where
    Arc<str>: From<T>,
{
    fn from(t: T) -> Self {
        Self(t.into())
    }
}

impl fmt::Display for NsName {
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
