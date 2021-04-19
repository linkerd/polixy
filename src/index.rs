use crate::{authz::Authorization, grpc::proto, server::Server};
use anyhow::Error;
use dashmap::DashMap;
use futures::prelude::*;
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::{reflector, watcher};
use serde::de::DeserializeOwned;
use std::{pin::Pin, sync::Arc};
use tokio::sync::{watch, Notify};
use tracing::{info, info_span, Instrument};

pub type Lookup = watch::Receiver<proto::InboundProxyConfig>;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NsName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SrvName(Arc<str>);

#[derive(Clone, Debug)]
pub struct Handle {
    servers: reflector::Store<Server>,
    authorizations: reflector::Store<Authorization>,
    configs: Arc<DashMap<NsName, Arc<Ns>>>,
}

#[derive(Debug)]
pub struct Ns {
    configs: DashMap<SrvName, Config>,
    notify: Arc<Notify>,
}

#[derive(Debug)]
struct Config {}

pub fn spawn(client: kube::Client) -> (Handle, tokio::task::JoinHandle<Error>) {
    let servers = Watch::new(watcher(
        Api::<Server>::all(client.clone()),
        ListParams::default(),
    ));

    let authorizations = Watch::new(watcher(
        Api::<Authorization>::all(client),
        ListParams::default(),
    ));

    let configs = Arc::new(DashMap::new());

    let handle = Handle {
        configs: configs.clone(),
        authorizations: authorizations.cache.clone(),
        servers: servers.cache.clone(),
    };

    let task =
        tokio::spawn(index(authorizations, servers, configs).instrument(info_span!("index")));

    (handle, task)
}

struct Watch<T>
where
    T: Resource + 'static,
    T::DynamicType: Eq + std::hash::Hash,
{
    cache: reflector::Store<T>,
    rx: Pin<Box<dyn Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static>>,
}

async fn index(
    mut authorizations: Watch<Authorization>,
    mut servers: Watch<Server>,
    _configs: Arc<DashMap<NsName, Arc<Ns>>>,
) -> Error {
    loop {
        tokio::select! {
            up = servers.rx.next() => match up.expect("Watch stream must not terminate") {
                Ok(watcher::Event::Applied(srv)) => {
                    let _ns = NsName::from_resource(&srv);
                    todo!("Handle srv apply")
                }

                Ok(watcher::Event::Deleted(srv)) => {
                    let _ns = NsName::from_resource(&srv);
                    todo!("Handle srv delete")
                }

                Err(error) => info!(%error, "Disconnected"),
                Ok(watcher::Event::Restarted(srvs)) => {
                    for srv in srvs.into_iter() {
                        let _ns = NsName::from_resource(&srv);
                        todo!("Handle srv delete")
                    }
                }
            },

            up = authorizations.rx.next() => match up.expect("Watch stream must not terminate") {
                Ok(watcher::Event::Applied(authz)) => {
                    let _ns = NsName::from_resource(&authz);
                    todo!("Handle authz apply")
                }

                Ok(watcher::Event::Deleted(authz)) => {
                    let _ns = NsName::from_resource(&authz);
                    todo!("Handle authz delete")
                }

                Err(error) => info!(%error, "Disconnected"),
                Ok(watcher::Event::Restarted(authzs)) => {
                    for authz in authzs.into_iter() {
                        let _ns = NsName::from_resource(&authz);
                        todo!("Handle authz delete")
                    }
                }
            },
        }
    }
}

// === impl Handle ===

impl Handle {
    pub async fn lookup(&self, _ns: &str, _name: &str, _port: u16) -> Option<Lookup> {
        todo!("Support lookup")
    }
}

// === impl Watch ===

impl<T> Watch<T>
where
    T: Resource + Clone + DeserializeOwned + std::fmt::Debug + Send + Sync + 'static,
    T::DynamicType: Clone + Eq + std::hash::Hash + Default,
{
    fn new<W>(watch: W) -> Self
    where
        W: Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static,
    {
        let store = reflector::store::Writer::<T>::default();
        let cache = store.as_reader();
        let rx = reflector(store, watch).boxed();
        Self { cache, rx }
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

impl std::fmt::Display for NsName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

// === SrvName ===

impl std::fmt::Display for SrvName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
