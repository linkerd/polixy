use crate::{
    authz::Authorization,
    grpc::proto,
    labels::{self, Labels},
    server::{self, Server},
};
use anyhow::{anyhow, Error, Result};
use dashmap::DashMap;
use futures::prelude::*;
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::watcher::{watcher, Event};
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::watch;
use tracing::{info, info_span, Instrument};

#[derive(Clone, Debug, Default)]
pub struct Index {
    namespaces: Arc<DashMap<NsName, Arc<RwLock<Ns>>>>,
}

pub type Lookup = watch::Receiver<proto::InboundProxyConfig>;

///
#[derive(Debug, Default)]
struct Ns {
    //authorizations: HashMap<AuthzName, Arc<Authorization>>,
    servers: HashMap<SrvName, SrvState>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NsName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PortName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SrvName(Arc<str>);

#[derive(Debug)]
struct SrvState {
    meta: SrvMeta,
    //authorizations: HashSet<AuthzName>,
    rx: watch::Receiver<proto::InboundProxyConfig>,
    tx: watch::Sender<proto::InboundProxyConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SrvMeta {
    port: SrvPort,
    labels: Labels,
    pod_selector: labels::Selector,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SrvPort {
    Name(PortName),
    Number(u16),
}

trait IndexResource<T> {
    fn apply(&mut self, resource: T) -> Result<()>;

    fn remove(&mut self, resource: T) -> Result<()>;

    fn reset(&mut self, resources: Vec<T>) -> Result<()>;
}

// === impl Index ===

impl Index {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn run(self, client: kube::Client) -> Error {
        let srv = self
            .clone()
            .index(Api::<Server>::all(client.clone()), ListParams::default())
            .instrument(info_span!("srv"));

        let authz = self
            .index(Api::<Authorization>::all(client), ListParams::default())
            .instrument(info_span!("authz"));

        tokio::select! {
            err = srv => err,
            err = authz => err,
        }
    }

    async fn index<T>(mut self, api: Api<T>, params: ListParams) -> Error
    where
        T: Resource + Clone + DeserializeOwned + std::fmt::Debug + Send + 'static,
        Self: IndexResource<T>,
    {
        let mut watch = watcher(api, params).boxed();
        loop {
            match watch.next().await.expect("Stream must not end") {
                Ok(ev) => {
                    let res = match ev {
                        Event::Applied(t) => self.apply(t),
                        Event::Deleted(t) => self.remove(t),
                        Event::Restarted(ts) => self.reset(ts),
                    };
                    if let Err(e) = res {
                        return e;
                    }
                }

                // Watcher streams surface errors when the response fails, but they
                // recover automatically.
                Err(error) => info!(%error, "Disconnected"),
            }
        }
    }

    pub async fn lookup(
        &self,
        _ns: impl Into<NsName>,
        _name: impl Into<Arc<str>>,
        _port: u16,
    ) -> Option<Lookup> {
        todo!("Lookup pod->server")
    }
}

// === impl State ===

impl IndexResource<Server> for Index {
    fn apply(&mut self, srv: Server) -> Result<()> {
        let ns_name = srv
            .namespace()
            .map(NsName::from)
            .ok_or_else(|| anyhow!("resource must have a namespace"))?;
        let ns = self.namespaces.entry(ns_name).or_default();
        let mut ns = ns.write();

        let srv_name = SrvName(srv.name().into());
        let meta = Self::mk_srv_meta(&srv);

        // If the server already exists, just update its indexes.
        if let Some(_srv) = ns.servers.get_mut(&srv_name) {
            todo!("Update server indexes");
        }

        // Otherwise, index a new server.

        let (tx, rx) = watch::channel(Default::default()); // FIXME
        ns.servers.insert(srv_name, SrvState { meta, tx, rx });

        Ok(())
    }

    fn remove(&mut self, _srv: Server) -> Result<()> {
        todo!("Remove authorization")
    }

    fn reset(&mut self, _srvs: Vec<Server>) -> Result<()> {
        todo!("Reset authorization")
    }
}

impl IndexResource<Authorization> for Index {
    fn apply(&mut self, _az: Authorization) -> Result<()> {
        todo!("Index authorization")
    }

    fn remove(&mut self, _az: Authorization) -> Result<()> {
        todo!("Remove authorization")
    }

    fn reset(&mut self, _azs: Vec<Authorization>) -> Result<()> {
        todo!("Reset authorization")
    }
}

impl Index {
    fn mk_srv_meta(srv: &Server) -> SrvMeta {
        let port = match srv.spec.port {
            server::Port::Number(ref p) => SrvPort::Number(*p),
            server::Port::Name(ref n) => SrvPort::Name(PortName(Arc::from(n.as_str()))),
        };
        SrvMeta {
            port,
            labels: srv.metadata.labels.clone().into(),
            pod_selector: srv.spec.pod_selector.clone(),
        }
    }
}

// === NsName ===

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

// === PortName ===

impl std::fmt::Display for PortName {
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

// === SrvPort ===

impl std::fmt::Display for SrvPort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Name(n) => n.fmt(f),
            Self::Number(n) => n.fmt(f),
        }
    }
}
