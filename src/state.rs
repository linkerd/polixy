use super::{authz::Authorization, grpc::proto};
use futures::prelude::*;
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::{watcher, watcher::Event};
use std::{
    collections::{HashMap, HashSet},
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
struct ServerState {
    authorizations: HashSet<AuthzKey>,
    pods: HashSet<PodKey>,
    tx: watch::Receiver<proto::InboundProxyConfig>,
    rx: watch::Sender<proto::InboundProxyConfig>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ContainerPort {
    container_name: ContainerName,
    port_name: PortName,
    server: Option<SrvKey>,
}

type ContainerName = String;
type PortName = Option<String>;

#[derive(Debug, Default)]
pub struct State {
    authorizations: HashMap<AuthzKey, Arc<Authorization>>,
    servers: HashMap<SrvKey, ServerState>,
    pods: HashMap<PodKey, HashMap<u16, ContainerPort>>,
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
    let watcher = Watcher(state.clone());
    let task = tokio::spawn(async move {
        tokio::select! {
            _ = index(client, state) => {}
            _ = drain.signaled() => {}
        };
    });
    (watcher, task)
}

async fn index(
    client: kube::Client,
    state: Arc<Mutex<State>>,
) -> kube_runtime::watcher::Result<()> {
    // let authz_api = Api::<authz::Authorization>::all(client.clone());
    // let srv_api = Api::<server::Server>::all(client);

    // let authz_task = tokio::spawn(
    //     async move {
    //         let mut authz = watcher(authz_api, ListParams::default()).boxed();
    //         while let some(ev) = authz.try_next().await? {
    //             match ev {
    //                 event::applied(authz) => {
    //                     info!(
    //                         ns = %authz.namespace().unwrap_or_default(),
    //                         name = %authz.name(),
    //                         "applied",
    //                     );
    //                     debug!("{:#?}", authz);
    //                 }
    //                 event::deleted(authz) => {
    //                     info!(
    //                         ns = %authz.namespace().unwrap_or_default(),
    //                         name = %authz.name(),
    //                         "deleted",
    //                     );
    //                     debug!("{:#?}", authz);
    //                 }
    //                 event::restarted(authzs) => {
    //                     info!("restarted");
    //                     for authz in authzs.into_iter() {
    //                         info!(
    //                             ns = %authz.namespace().unwrap_or_default(),
    //                             name = %authz.name(),
    //                         );
    //                         debug!("{:#?}", authz);
    //                     }
    //                 }
    //             }
    //         }
    //         ok::<(), kube_runtime::watcher::error>(())
    //     }
    //     .instrument(info_span!("authz")),
    // );

    // let srv_task = tokio::spawn(
    //     async move {
    //         let mut srvs = watcher(srv_api, ListParams::default()).boxed();
    //         while let Some(ev) = srvs.try_next().await? {
    //             match ev {
    //                 Event::Applied(srv) => {
    //                     info!(ns = %srv.namespace().unwrap(), name = %srv.name(), "Applied");
    //                     debug!("{:#?}", srv);
    //                 }
    //                 Event::Deleted(srv) => {
    //                     info!(ns = %srv.namespace().unwrap(), name = %srv.name(), "Deleted");
    //                     debug!("{:#?}", srv);
    //                 }
    //                 Event::Restarted(srvs) => {
    //                     info!("Restarted");
    //                     for srv in srvs.into_iter() {
    //                         info!(ns = %srv.namespace().unwrap(), name = %srv.name());
    //                         debug!("{:#?}", srv);
    //                     }
    //                 }
    //             }
    //         }
    //         Ok::<(), kube_runtime::watcher::Error>(())
    //     }
    //     .instrument(info_span!("srv")),
    // );

    tokio::spawn(index_pod_ports(client, state).instrument(info_span!("pods")))
        .await
        .expect("Spawn must succeed")?;

    Ok(())
}

async fn index_pod_ports(
    client: kube::Client,
    state: Arc<Mutex<State>>,
) -> kube_runtime::watcher::Result<()> {
    let api = Api::<Pod>::all(client);
    let mut watch = watcher(api, ListParams::default()).boxed();
    loop {
        debug!("Waiting for pods updates");
        match watch.try_next().await? {
            Some(Event::Applied(pod)) => {
                let key = pod_key(&pod);
                let ports = ports(&pod);
                debug!(?key, ports = ports.len(), "Applied");
                let mut state = state.lock().await;
                // TODO index against servers.
                // TODO notify lookups. (There shouldn't be any, but...)
                state.pods.insert(key, ports);
            }
            Some(Event::Deleted(pod)) => {
                let key = pod_key(&pod);
                debug!(?key, "Deleted");
                let mut state = state.lock().await;
                // TODO notify lookups.
                state.pods.remove(&key);
            }
            Some(Event::Restarted(pods)) => {
                debug!("Restarted");
                let mut state = state.lock().await;
                // TODO track deletions and notify lookups.
                state.pods.clear();
                for pod in pods.into_iter() {
                    let key = pod_key(&pod);
                    let ports = ports(&pod);
                    // TODO index against servers.
                    debug!(?key, ports = ports.len());
                    state.pods.insert(key, ports);
                }
            }
            None => return Ok(()),
        }
    }
}

fn pod_key(pod: &Pod) -> PodKey {
    let ns = pod.namespace().expect("pods must have a namespace");
    let name = pod.name();
    PodKey { ns, name }
}

fn ports(pod: &Pod) -> HashMap<u16, ContainerPort> {
    let mut ports = HashMap::default();

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

    ports
}
