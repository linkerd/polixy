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

#[derive(Debug, Default)]
struct State {
    namespaces: HashMap<NsName, NsState>,
}

#[derive(Debug, Default)]
struct NsState {
    authorizations: HashMap<AuthzName, Arc<Authorization>>,
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
    port: SrvPort,
    labels: Labels,
    pod_selector: labels::Selector,
    authorizations: HashSet<AuthzName>,
    pods: HashSet<PodName>,
    tx: watch::Receiver<proto::InboundProxyConfig>,
    rx: watch::Sender<proto::InboundProxyConfig>,
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
            let api = Api::<Server>::all(client);
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

        // Process all index updates on a background task
        let index_task = tokio::spawn(async move {
            tokio::select! {
                // Stop indexing immediately when the controller is shutdown.
                _ = drain.signaled() => debug!("Shutdown"),
                // If any of the index tasks complete, stop processing all indexes,
                _ = pods => warn!("Pod index terminated"),
                _ = srvs => warn!("Server index terminated"),
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
            warn!(%error, "Failed to index pod");
        }
    }

    fn update_pod(&mut self, pod: Pod) -> Result<(), Error> {
        let ns_name = NsName(pod.namespace().ok_or(MissingNamespace(()))?.into());
        let pod_name = PodName(pod.name().into());
        let mut pod = Self::mk_pod(&pod)?;

        let ns = self.namespaces.entry(ns_name.clone()).or_default();

        // If the pod already exists, the update may be changing its labels, so
        // we may need to re-index.
        if let Some(old_pod) = ns.pods.remove(&pod_name) {
            if pod.labels == old_pod.labels {
                // If the labels are unchanged, don't bother re-indexing; just
                // restore the pod in the index.
                debug!(?ns_name, ?pod_name, "Ingoring update");
                ns.pods.insert(pod_name, old_pod);
                return Ok(());
            }

            // Clear all server refernces to the pod before re-indexing.
            debug!(?ns_name, ?pod_name, "Updating pod's labels");
            for (_, srv) in ns.servers.iter_mut() {
                srv.pods.remove(&pod_name);
            }
        }
        debug!(?ns_name, ?pod_name, "Updating index");

        // Check all servers in the same namespace as the pod.
        for (srv_name, srv) in ns.servers.iter_mut() {
            // Get the port number for the server. If the port is named, look it
            // up
            let port = match srv.port {
                SrvPort::Number(p) => p,
                SrvPort::Name(ref n) => match pod.port_names.get(&n) {
                    Some(p) => *p,
                    // This pod does not have a port matching the server's port
                    // name; check the rest of the srevers.
                    None => continue,
                },
            };
            // Lookup the container port on the pod. If it exists, match the
            // pod's labels against the server's pod selector.
            if let Some(cp) = pod.ports.get_mut(&port) {
                if srv.pod_selector.matches(&pod.labels) {
                    // If this container port has already been matched to a
                    // server, we have overlapping servers.
                    if cp.server.is_some() {
                        return Err(AmbiguousServer(ns_name, pod_name, srv_name.clone()).into());
                    }
                    cp.server = Some(srv_name.clone());
                    srv.pods.insert(pod_name.clone());
                }
            }
        }

        ns.pods.insert(pod_name, pod);

        Ok(())
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

    fn reset_pods(&mut self, _pods: Vec<Pod>) -> Result<(), Error> {
        todo!("Reset pods");
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

    fn handle_srv(&mut self, ev: Event<Server>) {
        match ev {
            Event::Applied(s) => self.update_server(s),
            Event::Deleted(s) => self.remove_server(s),
            Event::Restarted(s) => self.reset_servers(s),
        }
    }

    fn update_server(&mut self, _srv: Server) {
        todo!();
        // TODO index against pods.
        // TODO notify lookups. (There shouldn't be any, but...)
    }

    fn remove_server(&mut self, _srv: Server) {
        todo!();
        // TODO notify lookups
    }

    fn reset_servers(&mut self, _srvs: Vec<Server>) {
        debug!("Restarted servers");
        todo!();
        // TODO track deletions and notify lookups.
    }
}

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
