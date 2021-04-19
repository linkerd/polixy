use crate::{
    authz::Authorization,
    grpc::proto,
    labels::{self, Labels},
    server::{self, Server},
};
use anyhow::{anyhow, Error, Result};
use futures::prelude::*;
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::watcher::{watcher, Event};
use serde::de::DeserializeOwned;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{watch, Mutex};
use tracing::{info, info_span, Instrument};

#[derive(Clone, Debug)]
pub struct Index(Arc<Mutex<State>>);

pub type Lookup = watch::Receiver<proto::InboundProxyConfig>;

/// Indexes pods, servers, and authorizations.
///
/// State is tracked per-namespace, since it's never necessary to correlate data
/// across namespaces.
///
/// TODO This means we could probably establishes watches in namespace-scopes as
/// well to reduce locking/contention in busy clusters; but this is overly
/// complicated for now.
#[derive(Debug, Default)]
struct State {
    namespaces: HashMap<NsName, NsState>,
}

///
#[derive(Debug, Default)]
struct NsState {
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

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(State::default())))
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

    async fn index<T>(self, api: Api<T>, params: ListParams) -> Error
    where
        T: Resource + Clone + DeserializeOwned + std::fmt::Debug + Send + 'static,
        State: IndexResource<T>,
    {
        let mut watch = watcher(api, params).boxed();
        while let Some(res) = watch.next().await {
            match res {
                Ok(ev) => {
                    let mut state = self.0.lock().await;
                    let res = match ev {
                        Event::Applied(t) => state.apply(t),
                        Event::Deleted(t) => state.remove(t),
                        Event::Restarted(ts) => state.reset(ts),
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

        // Watcher streams never end.
        unreachable!("Stream must not end");
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

impl IndexResource<Server> for State {
    fn apply(&mut self, srv: Server) -> Result<(), Error> {
        let ns_name = NsName(
            srv.namespace()
                .ok_or_else(|| anyhow!("resource must have a namespace"))?
                .into(),
        );
        let ns = self.namespaces.entry(ns_name).or_default();

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

    fn remove(&mut self, _srv: Server) -> Result<(), Error> {
        todo!("Remove authorization")
    }

    fn reset(&mut self, _srvs: Vec<Server>) -> Result<(), Error> {
        todo!("Reset authorization")
    }
}

impl IndexResource<Authorization> for State {
    fn apply(&mut self, _az: Authorization) -> Result<(), Error> {
        todo!("Index authorization")
    }

    fn remove(&mut self, _az: Authorization) -> Result<(), Error> {
        todo!("Remove authorization")
    }

    fn reset(&mut self, _azs: Vec<Authorization>) -> Result<(), Error> {
        todo!("Reset authorization")
    }
}

impl State {
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
