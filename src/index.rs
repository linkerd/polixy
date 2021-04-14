use super::{
    authz::Authorization,
    grpc::proto,
    labels::{self, Labels},
    server::Server,
};
use futures::prelude::*;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::watcher::{watcher, Event};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{watch, Mutex};
use tracing::{debug, info, info_span, warn, Instrument};

#[derive(Clone, Debug)]
pub struct Index(Arc<Mutex<State>>);

pub type Lookup = watch::Receiver<proto::InboundProxyConfig>;

#[derive(Debug, Default)]
struct State {
    authorizations: HashMap<AuthzKey, Arc<Authorization>>,
    servers: HashMap<SrvKey, SrvState>,
    pods: HashMap<PodKey, PodState>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct Namespace(String);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct AuthzKey {
    ns: Namespace,
    name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SrvKey {
    ns: Namespace,
    name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PodKey {
    ns: Namespace,
    name: String,
}

#[derive(Debug)]
struct SrvState {
    authorizations: HashSet<AuthzKey>,
    server_labels: Labels,
    pod_selector: labels::Selector,
    pods: HashSet<PodKey>,
    tx: watch::Receiver<proto::InboundProxyConfig>,
    rx: watch::Sender<proto::InboundProxyConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PodState {
    labels: Labels,
    ports: Ports,
}

type ContainerName = String;
type PortName = Option<String>;

#[derive(Clone, Debug, Eq, Default)]
struct Ports(Arc<BTreeMap<u16, ContainerPort>>);

#[derive(Clone, Debug, PartialEq, Eq)]
struct ContainerPort {
    container_name: ContainerName,
    port_name: PortName,
    server: Option<SrvKey>,
}

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
        let key = PodKey {
            ns: Namespace(ns.into()),
            name: name.into(),
        };

        let _state = self.0.lock().await.pods.get(&key)?;
        let _ = port;

        todo!("Lookup pod->server")
    }
}

// === impl State ===

impl State {
    fn handle_pod(&mut self, ev: Event<Pod>) {
        match ev {
            Event::Applied(p) => self.update_pod(p),
            Event::Deleted(p) => self.remove_pod(p),
            Event::Restarted(p) => self.reset_pods(p),
        }
    }

    fn update_pod(&mut self, pod: Pod) {
        let (key, state) = self.mk_pod(&pod);
        debug!(?key, "Update pod");
        // TODO index against servers.
        self.pods.insert(key, state);
        // TODO notify lookups. (There shouldn't be any, but...)
    }

    fn remove_pod(&mut self, pod: Pod) {
        let key = Self::pod_key(&pod);
        debug!(?key, "Remove pod");
        self.pods.remove(&key);
        // TODO notify lookups
    }

    fn reset_pods(&mut self, pods: Vec<Pod>) {
        debug!("Restarted");
        // TODO track deletions and notify lookups.
        self.pods.clear();
        for pod in pods.into_iter() {
            self.update_pod(pod)
        }
    }

    fn pod_key(pod: &Pod) -> PodKey {
        let ns = Namespace(pod.namespace().expect("pods must have a namespace"));
        let name = pod.name();
        PodKey { ns, name }
    }

    fn mk_pod(&self, pod: &Pod) -> (PodKey, PodState) {
        let mut ports = BTreeMap::default();
        if let Some(spec) = pod.spec.as_ref() {
            for container in &spec.containers {
                let cname = container.name.clone();
                if let Some(ps) = container.ports.as_ref() {
                    for p in ps {
                        let port = p.container_port as u16;
                        let cp = ContainerPort {
                            container_name: cname.clone(),
                            port_name: p.name.clone(),
                            server: None,
                        };
                        ports.insert(port, cp);
                    }
                }
            }
        }

        let key = Self::pod_key(&pod);
        let state = PodState {
            ports: ports.into(),
            labels: pod.metadata.labels.clone().into(),
        };

        (key, state)
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

// === Ports ===

impl From<BTreeMap<u16, ContainerPort>> for Ports {
    #[inline]
    fn from(ports: BTreeMap<u16, ContainerPort>) -> Self {
        Self(ports.into())
    }
}

impl AsRef<BTreeMap<u16, ContainerPort>> for Ports {
    #[inline]
    fn as_ref(&self) -> &BTreeMap<u16, ContainerPort> {
        self.0.as_ref()
    }
}

impl<T: AsRef<BTreeMap<u16, ContainerPort>>> std::cmp::PartialEq<T> for Ports {
    #[inline]
    fn eq(&self, t: &T) -> bool {
        self.0.as_ref().eq(t.as_ref())
    }
}
