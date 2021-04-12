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
use kube_runtime::{watcher, watcher::Event};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Weak},
};
use tokio::sync::{watch, Mutex, Notify};
use tracing::{debug, info_span, Instrument};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct AuthzKey {
    ns: String,
    name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct SrvKey {
    ns: String,
    name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct PodKey {
    ns: String,
    name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct LookupKey {
    pod: PodKey,
    port: u16,
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

#[derive(Debug, Default)]
struct State {
    authorizations: HashMap<AuthzKey, Arc<Authorization>>,
    servers: HashMap<SrvKey, SrvState>,
    pods: HashMap<PodKey, PodState>,
    lookups: HashMap<LookupKey, Vec<Weak<Notify>>>,
}

#[derive(Clone, Debug)]
pub struct Watcher(Arc<Mutex<State>>);

pub type Watch = watch::Receiver<proto::InboundProxyConfig>;

impl Watcher {
    pub async fn watch(
        &self,
        _ns: impl Into<String>,
        _name: impl Into<String>,
        _port: u16,
    ) -> Watch {
        // let ns = ns.into();
        // let name = name.into();
        todo!("watch stream")
    }
}

pub fn spawn(
    client: kube::Client,
    drain: linkerd_drain::Watch,
) -> (Watcher, tokio::task::JoinHandle<()>) {
    let state = Arc::new(Mutex::new(State::default()));

    let pods = tokio::spawn(
        index(client.clone(), state.clone(), |s, ev| s.handle_pod(ev))
            .instrument(info_span!("pods")),
    );

    let srvs = tokio::spawn(
        index(client, state.clone(), |s, ev| s.handle_srv(ev)).instrument(info_span!("servers")),
    );

    let watcher = Watcher(state);
    let task = tokio::spawn(async move {
        tokio::select! {
            _ = pods => {}
            _ = srvs => {}
            _ = drain.signaled() => {}
        };
    });
    (watcher, task)
}

async fn index<T>(
    client: kube::Client,
    state: Arc<Mutex<State>>,
    idx: impl Fn(&mut State, Event<T>),
) -> kube_runtime::watcher::Result<()>
where
    T: Resource + serde::de::DeserializeOwned + std::fmt::Debug + Clone + Send + 'static,
    T::DynamicType: Default,
{
    let api = Api::<T>::all(client);
    let mut watch = watcher(api, ListParams::default()).boxed();
    loop {
        debug!("Waiting for updates");
        match watch.try_next().await? {
            Some(ev) => {
                let mut s = state.lock().await;
                idx(&mut s, ev);
            }
            None => return Ok(()),
        }
    }
}

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
        let ns = pod.namespace().expect("pods must have a namespace");
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
}

impl State {
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
