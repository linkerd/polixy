use crate::{
    authz::Authorization,
    grpc::proto,
    labels::{self, Labels},
    server::Server,
    Error,
};
use futures::prelude::*;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::watcher::{watcher, Event};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{watch, Mutex};
use tracing::{debug, info, info_span, warn, Instrument};

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
    pods: HashMap<PodName, PodState>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct NsName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct AuthzName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SrvName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PodName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ContainerName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PortName(Arc<str>);

#[derive(Debug)]
struct SrvState {
    meta: SrvMeta,
    //authorizations: HashSet<AuthzName>,
    pods: HashSet<PodName>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct PodState {
    labels: Labels,
    ports: HashMap<u16, ContainerPort>,
    port_names: HashMap<PortName, u16>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ContainerPort {
    container_name: ContainerName,
    server: Option<SrvName>,
}

#[derive(Debug)]
enum DuplicatePort {
    Name(PortName),
    Number(u16),
}

#[derive(Debug)]
struct AmbiguousServer(NsName, PodName, SrvName);

#[derive(Debug)]
struct MissingNamespace(());

// === impl Index ===

impl Index {
    // Returns an index handle and spawns a background task that updates it.
    pub fn spawn(
        client: kube::Client,
        drain: linkerd_drain::Watch,
    ) -> (Self, impl std::future::Future<Output = ()>) {
        let state = Arc::new(Mutex::new(State::default()));

        // Update the index from pod updates.
        let pods = {
            let state = state.clone();
            let api = Api::<Pod>::all(client.clone());
            async move {
                let mut watch = watcher(api, ListParams::default()).boxed();
                loop {
                    match watch.next().await {
                        Some(Ok(ev)) => state.lock().await.handle_pod(ev),
                        Some(Err(error)) => info!(%error, "Disconnected"),
                        None => return,
                    }
                }
            }
            .instrument(info_span!("pod"))
        };

        // Update the index from server updates.
        let srvs = {
            let state = state.clone();
            let api = Api::<Server>::all(client.clone());
            async move {
                let mut watch = watcher(api, ListParams::default()).boxed();
                loop {
                    match watch.next().await {
                        Some(Ok(ev)) => state.lock().await.handle_srv(ev),
                        Some(Err(error)) => info!(%error, "Disconnected"),
                        None => return,
                    }
                }
            }
            .instrument(info_span!("srv"))
        };

        // Update the index from authorization updates.
        let authzs = {
            let state = state.clone();
            let api = Api::<Authorization>::all(client);
            async move {
                let mut watch = watcher(api, ListParams::default()).boxed();
                loop {
                    match watch.next().await {
                        Some(Ok(ev)) => state.lock().await.handle_authz(ev),
                        Some(Err(error)) => info!(%error, "Disconnected"),
                        None => return,
                    }
                }
            }
            .instrument(info_span!("authz"))
        };

        // Process all index updates on a background task
        let index_task = tokio::spawn(async move {
            tokio::select! {
                // Stop indexing immediately when the controller is shutdown.
                _ = drain.signaled() => debug!("Shutdown"),
                // If any of the index tasks complete, stop processing all indexes,
                _ = pods => warn!("Pod index terminated"),
                _ = srvs => warn!("Server index terminated"),
                _ = authzs => warn!("Authorization index terminated"),
            }
        });

        (Self(state), index_task.map(|_| {}))
    }

    pub async fn lookup(
        &self,
        ns: impl Into<String>,
        name: impl Into<String>,
        port: u16,
    ) -> Option<Lookup> {
        let ns = NsName(Arc::from(ns.into()));
        let name = PodName(Arc::from(name.into()));

        let _state = self.0.lock().await.namespaces.get(&ns)?.pods.get(&name)?;
        let _ = port;

        todo!("Lookup pod->server")
    }
}

// === impl State ===

impl State {
    fn handle_pod(&mut self, ev: Event<Pod>) {
        let res = match ev {
            Event::Applied(p) => self.update_pod(p),
            Event::Deleted(p) => self.remove_pod(p),
            Event::Restarted(p) => self.reset_pods(p),
        };

        if let Err(error) = res {
            warn!(%error, "Failed to index");
        }
    }

    fn handle_srv(&mut self, ev: Event<Server>) {
        let res = match ev {
            Event::Applied(s) => self.update_server(s),
            Event::Deleted(s) => self.remove_server(s),
            Event::Restarted(s) => self.reset_servers(s),
        };

        if let Err(error) = res {
            warn!(%error, "Failed to index");
        }
    }

    fn handle_authz(&mut self, ev: Event<Authorization>) {
        let res = match ev {
            Event::Applied(a) => self.update_authz(a),
            Event::Deleted(a) => self.remove_authz(a),
            Event::Restarted(a) => self.reset_authz(a),
        };

        if let Err(error) = res {
            warn!(%error, "Failed to index");
        }
    }

    // TODO test
    fn update_pod(&mut self, pod: Pod) -> Result<(), Error> {
        let ns_name = NsName(pod.namespace().ok_or(MissingNamespace(()))?.into());
        let ns = self.namespaces.entry(ns_name.clone()).or_default();

        let pod_name = PodName(pod.name().into());
        let new_pod = Self::mk_pod(&pod)?;

        // If the pod already exists, the update may be changing its labels, so
        // we may need to re-index. We remove the pod initially and, if the
        // labels are the same, it is re-inserted.
        if let Some(pod) = ns.pods.get_mut(&pod_name) {
            if new_pod.labels == pod.labels {
                // If the labels are unchanged, no part of the pod metadata can
                // impact our indexing, as the port information is immutable.
                debug!(?ns_name, ?pod_name, "Ingoring update");
                return Ok(());
            }

            debug!(?ns_name, ?pod_name, "Labels updated");
            pod.labels = new_pod.labels;
            // Clear the pod->server index so that we can recreate it with
            // updated labels. This allows us to return `AmbiguousServer` errors
            // when more than one server matches a port.
            for cp in pod.ports.values_mut() {
                cp.server = None;
            }
            for (srv_name, srv) in ns.servers.iter_mut() {
                // Lookup the container port on the pod. If it exists, match
                // the pod's labels against the server's pod selector.
                if let Some(port) = pod.get_port(&srv.meta.port) {
                    if let Some(cp) = pod.ports.get_mut(&port) {
                        if srv.meta.pod_selector.matches(&pod.labels) {
                            // If this container port has already been matched to a
                            // server, we have overlapping servers.
                            if cp.server.is_some() {
                                let e = AmbiguousServer(ns_name, pod_name, srv_name.clone());
                                return Err(e.into());
                            }
                            cp.server = Some(srv_name.clone());

                            srv.pods.insert(pod_name.clone());
                        } else {
                            srv.pods.remove(&pod_name);
                        }
                    }
                }
            }
        } else {
            // The pod is being added anew, so we don't need to bother clearing
            // out prior state before updating the index.
            let mut pod = new_pod;

            for (srv_name, srv) in ns.servers.iter_mut() {
                // Lookup the container port on the pod. If it exists, match the
                // pod's labels against the server's pod selector.
                if let Some(port) = pod.get_port(&srv.meta.port) {
                    if let Some(cp) = pod.ports.get_mut(&port) {
                        if srv.meta.pod_selector.matches(&pod.labels) {
                            // If this container port has already been matched to a
                            // server, we have overlapping servers.
                            if cp.server.is_some() {
                                let e = AmbiguousServer(ns_name, pod_name, srv_name.clone());
                                return Err(e.into());
                            }
                            cp.server = Some(srv_name.clone());
                            srv.pods.insert(pod_name.clone());
                        }
                    }
                }
            }

            debug!(?ns_name, ?pod_name, "Adding a pod to the index");
            ns.pods.insert(pod_name, pod);
        }

        Ok(())
    }

    fn update_server(&mut self, srv: Server) -> Result<(), Error> {
        let ns_name = NsName(srv.namespace().ok_or(MissingNamespace(()))?.into());
        let ns = self.namespaces.entry(ns_name.clone()).or_default();

        let srv_name = SrvName(srv.name().into());
        let meta = Self::mk_srv_meta(&srv)?;

        if let Some(srv) = ns.servers.get_mut(&srv_name) {
            // If the labels have changed, then we should reindex authorizations
            // against this server.
            if srv.meta.labels != meta.labels {
                //srv.authorizations.clear();
                todo!("Reindex authorizations");
            }

            // If the port or pod selection is changed, then we should reindex
            // pod-server relationships.
            if srv.meta.port != meta.port || srv.meta.pod_selector != meta.pod_selector {
                srv.pods.clear();
                for (pod_name, pod) in ns.pods.iter_mut() {
                    if let Some(port) = pod.get_port(&meta.port) {
                        if let Some(cp) = pod.ports.get_mut(&port) {
                            if meta.pod_selector.matches(&pod.labels) {
                                let pod_name = pod_name.clone();
                                if cp.server.is_some() {
                                    let e = AmbiguousServer(ns_name, pod_name, srv_name);
                                    return Err(e.into());
                                }
                                cp.server = Some(srv_name.clone());
                                srv.pods.insert(pod_name.clone());
                            }
                        }
                    }
                }

                todo!("Notify lookups");
            }

            return Ok(());
        }

        let mut pods = HashSet::new();
        for (pod_name, pod) in ns.pods.iter_mut() {
            if let Some(port) = pod.get_port(&meta.port) {
                if let Some(cp) = pod.ports.get_mut(&port) {
                    if meta.pod_selector.matches(&pod.labels) {
                        let pod_name = pod_name.clone();
                        if cp.server.is_some() {
                            let e = AmbiguousServer(ns_name, pod_name, srv_name);
                            return Err(e.into());
                        }
                        cp.server = Some(srv_name.clone());
                        pods.insert(pod_name.clone());
                    }
                }
            }
        }

        // TODO index authorizations.

        let (tx, rx) = watch::channel(Default::default()); // FIXME
        ns.servers.insert(srv_name, SrvState { meta, pods, tx, rx });

        Ok(())
    }

    fn update_authz(&mut self, _authz: Authorization) -> Result<(), Error> {
        todo!("Update authorization")
    }

    fn remove_pod(&mut self, pod: Pod) -> Result<(), Error> {
        let ns = NsName(pod.namespace().ok_or(MissingNamespace(()))?.into());
        let name = PodName(pod.name().into());
        debug!(?ns, ?name, "Remove pod");
        if let Some(ns) = self.namespaces.get_mut(&ns) {
            ns.pods.remove(&name);
            for (_, srv) in ns.servers.iter_mut() {
                srv.pods.remove(&name);
            }
            // TODO terminate active lookups.
        }
        Ok(())
    }

    fn remove_server(&mut self, _srv: Server) -> Result<(), Error> {
        todo!("Remove server; notify lookups");
    }

    fn remove_authz(&mut self, _authz: Authorization) -> Result<(), Error> {
        todo!("Remove authorization")
    }

    fn reset_pods(&mut self, _pods: Vec<Pod>) -> Result<(), Error> {
        todo!("Reset pods");
    }

    fn reset_servers(&mut self, _srvs: Vec<Server>) -> Result<(), Error> {
        debug!("Restarted servers");
        todo!("Track deletions and notify lookups");
    }

    fn reset_authz(&mut self, _authz: Vec<Authorization>) -> Result<(), Error> {
        todo!("Reset authorization")
    }

    fn mk_pod(pod: &Pod) -> Result<PodState, DuplicatePort> {
        let mut ports = HashMap::default();
        let mut port_names = HashMap::default();

        if let Some(spec) = pod.spec.as_ref() {
            for container in &spec.containers {
                let cname = ContainerName(container.name.as_str().into());
                if let Some(ps) = container.ports.as_ref() {
                    for p in ps {
                        let port = p.container_port as u16;
                        let cp = ContainerPort {
                            container_name: cname.clone(),
                            server: None,
                        };
                        if ports.contains_key(&port) {
                            return Err(DuplicatePort::Number(port));
                        }
                        if let Some(n) = p.name.as_ref() {
                            let name = PortName(n.clone().into());
                            if port_names.contains_key(&name) {
                                return Err(DuplicatePort::Name(name));
                            }
                            port_names.insert(name, port);
                        }
                        ports.insert(port, cp);
                    }
                }
            }
        }

        Ok(PodState {
            labels: pod.metadata.labels.clone().into(),
            ports,
            port_names,
        })
    }

    fn mk_srv_meta(srv: &Server) -> Result<SrvMeta, Error> {
        todo!()
    }
}

// === PodState ===

impl PodState {
    /// Gets the port number for the server. If the server port is named, resolve
    /// the number from the pod.
    fn get_port(&self, sp: &SrvPort) -> Option<u16> {
        match sp {
            SrvPort::Number(p) => Some(*p),
            SrvPort::Name(ref n) => self.port_names.get(&n).copied(),
        }
    }
}

// === Names ===

impl std::fmt::Display for NsName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for PodName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for SrvName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for PortName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

// === impl AmbiguousServer ===

impl std::fmt::Display for AmbiguousServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Pod port matched by more than one server: ns={} pod={} srv={}",
            self.0, self.1, self.2
        )
    }
}

impl std::error::Error for AmbiguousServer {}

// === impl DuplicatePort ===

impl std::fmt::Display for DuplicatePort {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Name(p) => write!(f, "Duplicate port named {}", p),
            Self::Number(p) => write!(f, "Duplicate port {}", p),
        }
    }
}

impl std::error::Error for DuplicatePort {}

// === impl MissingNamespace ===

impl std::fmt::Display for MissingNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        "Resource must have a namespace".fmt(f)
    }
}

impl std::error::Error for MissingNamespace {}
