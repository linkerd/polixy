#![allow(warnings)]

use futures::{future, prelude::*};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Weak},
};
use tokio::sync::{mpsc, watch, Mutex, Notify};
use tracing::{debug, error, info, info_span, Instrument};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (drain_tx, drain_rx) = linkerd_drain::channel();

    let client = kube::Client::try_default()
        .await
        .expect("Failed to initialize client");
    let (watcher, indexer) = state::spawn(client);

    let addr = ([0, 0, 0, 0], 8090).into();
    let server = grpc::Grpc::new(watcher, drain_rx.clone());
    let grpc = tokio::spawn(
        async move {
            let (close_tx, close_rx) = tokio::sync::oneshot::channel();
            tokio::pin! {
                let srv = server.serve(addr, close_rx.map(|_| {}));
            }
            let res = tokio::select! {
                res = (&mut srv) => res,
                handle = drain_rx.signaled() => {
                    let _ = close_tx.send(());
                    handle.release_after(srv).await
                }
            };
            match res {
                Ok(()) => debug!("shutdown"),
                Err(error) => error!(%error),
            }
        }
        .instrument(info_span!("grpc")),
    );

    let ctrl_c = tokio::signal::ctrl_c();
    let term = async move {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => term.recv().await,
            _ => future::pending().await,
        }
    };

    tokio::join!(ctrl_c, term);
    drain_tx.drain().await;
    grpc.await.expect("Spawn must succeed");
    indexer.await.expect("Spawn must succeed");
}

mod state {
    use super::{authz::Authorization, grpc::proto, server::Server};
    use k8s_openapi::api::core::v1::Pod;
    use kube::{
        api::{DynamicObject, ListParams, Resource},
        Api,
    };
    use kube_runtime::{watcher, watcher::Event};
    use std::{
        collections::{HashMap, HashSet},
        pin::Pin,
        sync::{Arc, Weak},
    };
    use tokio::sync::{mpsc, watch, Mutex, Notify};
    use tracing::{debug, info};

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
    struct PortKey {
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

    #[derive(Debug, Default)]
    pub struct State {
        authorizations: HashMap<AuthzKey, Arc<Authorization>>,
        servers: HashMap<SrvKey, ServerState>,
        pods: HashMap<PodKey, HashMap<u16, Option<SrvKey>>>,
        lookups: HashMap<PortKey, Vec<Weak<Notify>>>,
    }

    #[derive(Clone, Debug)]
    pub struct Watcher(Arc<Mutex<State>>);

    pub type Watch = watch::Receiver<proto::InboundProxyConfig>;

    impl Watcher {
        pub async fn watch(
            &self,
            ns: impl Into<String>,
            name: impl Into<String>,
            port: u16,
        ) -> Watch {
            let ns = ns.into();
            let name = name.into();
            todo!("watch stream")
        }
    }

    pub fn spawn(client: kube::Client) -> (Watcher, tokio::task::JoinHandle<()>) {
        let state = Arc::new(Mutex::new(State::default()));
        let watcher = Watcher(state.clone());
        let task = tokio::spawn(index(client, state));
        (watcher, task)
    }

    async fn index(client: kube::Client, state: Arc<Mutex<State>>) {
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

        todo!("indexing task")
    }

    async fn index_pods(
        client: kube::Client,
        state: Arc<Mutex<State>>,
    ) -> kube_runtime::watcher::Result<()> {
        let api = Api::<Pod>::all(client);
        let mut watch = watcher(api, ListParams::default()).boxed();
        while let Some(ev) = watch.try_next().await? {
            match ev {
                Event::Applied(pod) => {
                    let ns = pod.namespace().expect("Pods must be namespaced");
                    let name = pod.name();
                    info!(%ns, %name, "Applied");
                    debug!("{:#?}", pod);

                    let key = PodKey { ns, name };
                    let state = state.lock().await;
                    state.pods.entry(key)
                }
                Event::Deleted(pod) => {
                    info!(
                        ns = %pod.namespace().unwrap_or_default(),
                        name = %pod.name(),
                        "Deleted",
                    );
                    debug!("{:#?}", pod);
                }
                Event::Restarted(pods) => {
                    info!(pods = %pods.len(), "Restarted");
                    for pod in pods.into_iter() {
                        info!(
                            ns = %pod.namespace().unwrap_or_default(),
                            name = %pod.name(),
                        );
                        debug!("{:#?}", pod);
                    }
                }
            }
        }
        Ok(())
    }
}

mod grpc {
    use super::state::{Watch, Watcher};
    use futures::prelude::*;

    pub mod proto {
        tonic::include_proto!("polixy.olix0r.net");

        pub use self::proxy_config_service_server::{
            ProxyConfigService as Service, ProxyConfigServiceServer as Server,
        };
    }

    #[derive(Clone, Debug)]
    pub struct Grpc {
        watcher: Watcher,
        drain: linkerd_drain::Watch,
    }

    impl Grpc {
        pub fn new(watcher: Watcher, drain: linkerd_drain::Watch) -> Self {
            Self { watcher, drain }
        }

        pub async fn serve(
            self,
            addr: std::net::SocketAddr,
            shutdown: impl std::future::Future<Output = ()>,
        ) -> Result<(), tonic::transport::Error> {
            tonic::transport::Server::builder()
                .add_service(proto::Server::new(self))
                .serve_with_shutdown(addr.into(), shutdown)
                .await
        }
    }

    #[async_trait::async_trait]
    impl proto::Service for Grpc {
        type WatchInboundStream = std::pin::Pin<
            Box<dyn Stream<Item = Result<proto::InboundProxyConfig, tonic::Status>> + Send + Sync>,
        >;

        async fn watch_inbound(
            &self,
            req: tonic::Request<proto::InboundProxyPort>,
        ) -> Result<tonic::Response<Self::WatchInboundStream>, tonic::Status> {
            let proto::InboundProxyPort { workload, port } = req.into_inner();

            // Parse a workload name in the form namespace:name.
            let (ns, name) = {
                let parts = workload.splitn(2, ':').collect::<Vec<_>>();
                if parts.len() != 2 {
                    return Err(tonic::Status::invalid_argument(format!(
                        "Invalid workload: {}",
                        workload
                    )));
                }
                (parts[0], parts[1])
            };

            // Ensure that the port is in the valid range.
            let port = {
                if port == 0 || port > std::u16::MAX as u32 {
                    return Err(tonic::Status::invalid_argument(format!(
                        "Invalid port: {}",
                        port
                    )));
                }
                port as u16
            };

            let mut watch = self.watcher.watch(ns, name, port).await;
            let updates = async_stream::try_stream! {
                loop {
                    // Send the current config on the stream.
                    let update = (*watch.borrow()).clone();
                    yield update;

                    // Wait until the watch is updated before sending another
                    // update. If the sender is dropped, then end the stream.
                    if watch.changed().await.is_err() {
                        return;
                    }
                }
            };

            Ok(tonic::Response::new(Box::pin(updates)))
        }
    }
}

mod server {
    use super::labels;
    use kube::CustomResource;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    /// Describes a server interface exposed by a set of pods.
    #[kube(
        group = "polixy.olix0r.net",
        version = "v1",
        kind = "Server",
        namespaced
    )]
    #[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct ServerSpec {
        pod_selector: PodSelector,
        container_name: Option<String>,
        port: Port,
    }

    /// Selects a set of pods that expose a server.
    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct PodSelector {
        match_labels: Option<labels::Map>,
        match_expressions: Option<labels::Expressions>,
    }

    /// References a pod spec's port by name or number.
    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(untagged)]
    enum Port {
        Number(u16),
        Name(String),
    }
}

mod authz {
    use super::labels;
    use kube::CustomResource;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    /// Authorizes clients to connect to a Server.
    #[kube(
        group = "polixy.olix0r.net",
        version = "v1",
        kind = "Authorization",
        namespaced
    )]
    #[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct AuthorizationSpec {
        server: Server,
        authenticated: Option<Authenticated>,
        unauthenticated: Option<Unauthenticated>,
    }

    /// Selects one or more server instances in the same namespace as the `Authorization`.
    ///
    /// Exactly one of `name`, `match_labels`, and `match_expressions` should be set.
    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct Server {
        /// References a server instance by name.
        name: Option<String>,
        /// Selects an arbitrary number of `Server` instances by label key-value.
        match_labels: Option<labels::Map>,
        /// Selects an arbitrary number of `Server` instances by label expression.
        match_expressions: Option<labels::Expressions>,
    }

    /// Describes an authenticated client.
    ///
    /// Exactly one of `any`, `identity`, and `service_account` should be set.
    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct Authenticated {
        /// Indicates a Linkerd identity that is authorized to access a server.
        identities: Option<Vec<String>>,
        /// Identifies a `ServiceAccount` authorized to access a server.
        service_accounts: Option<Vec<ServiceAccount>>,
    }

    /// References Kubernetes `ServiceAccount` instances.
    ///
    /// If no namespace is specified, the `Authorization`'s namespace is used.
    ///
    /// Exactly one of `name`, `match_labels`, or `match_expressions` should be set.
    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct ServiceAccount {
        namespace: String,
        name: Option<String>,
        match_labels: Option<labels::Map>,
        match_expressions: Option<labels::Expressions>,
    }

    /// Describes an unauthenticated client.
    ///
    /// Exactly one of `any`, `node`, and `network` should be set.
    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    pub struct Unauthenticated {
        kubelet: Option<bool>,
        networks: Option<Vec<String>>,
    }
}

mod labels {
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    pub type Map = HashMap<String, String>;

    pub type Expressions = Vec<Expression>;

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    pub struct Expression {
        key: String,
        operator: Operator,
        values: Vec<String>,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    pub enum Operator {
        In,
        NotIn,
    }
}
