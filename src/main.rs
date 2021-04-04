#![allow(warnings)]

use futures::{future, prelude::*};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{DynamicObject, ListParams, Resource},
    Api,
};
use kube_runtime::{watcher, watcher::Event};
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

    let grpc = tokio::spawn(
        async move {
            let addr = ([0, 0, 0, 0], 8090).into();
            match grpc::start(addr, future::pending()).await {
                Ok(()) => debug!("shutdown"),
                Err(error) => error!(%error),
            }
        }
        .instrument(info_span!("grpc")),
    );

    let client = kube::Client::try_default()
        .await
        .expect("Failed to initialize client");

    // let authz_api = Api::<authz::Authorization>::all(client.clone());
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

    //let srv_api = Api::<server::Server>::all(client);
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

    let (drain_tx, drain_rx) = linkerd_drain::channel();

    let ctrl_c = tokio::signal::ctrl_c();
    let term = async move {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => term.recv().await,
            _ => future::pending().await,
        }
    };

    let _ = drain_rx.signaled();

    tokio::join!(ctrl_c, term);
    drain_tx.drain().await;
    grpc.await.expect("Spawn must succeed")
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct WatchKey {
    ns: Option<String>,
    name: String,
}

#[derive(Debug)]
struct WatchUpdater<T> {
    rx: watch::Receiver<T>,
    tx: watch::Sender<T>,
}

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

struct ServerState {
    authorizations: HashSet<AuthzKey>,
    pods: HashSet<PodKey>,
    tx: watch::Sender<grpc::proto::InboundProxyConfig>,
    rx: watch::Sender<grpc::proto::InboundProxyConfig>,
}

struct State {
    authorizations: HashMap<AuthzKey, Arc<authz::Authorization>>,
    servers: HashMap<SrvKey, ServerState>,
    pods: HashMap<PodKey, HashMap<u16, SrvKey>>,
    lookups: HashMap<PortKey, Vec<Weak<Notify>>>,
}

mod grpc {
    use futures::Stream;
    use std::pin::Pin;

    pub mod proto {
        tonic::include_proto!("polixy.olix0r.net");

        pub use self::proxy_config_service_server::{
            ProxyConfigService as Service, ProxyConfigServiceServer as Server,
        };
    }

    pub struct Grpc;

    pub async fn start(
        addr: std::net::SocketAddr,
        drain: impl std::future::Future<Output = linkerd_drain::ReleaseShutdown> + Unpin,
    ) -> Result<(), super::Error> {
        let notify = std::sync::Arc::new(tokio::sync::Notify::new());
        let notify_rx = notify.clone();
        tokio::pin! {
            let srv = tonic::transport::Server::builder()
                .add_service(Grpc::server())
                .serve_with_shutdown(addr.into(), notify_rx.notified());
        }
        tokio::select! {
            res = (&mut srv) => res?,
            handle = drain => {
                let _ = notify.notify_waiters();
                handle.release_after(srv).await?;
            }
        }
        Ok::<_, super::Error>(())
    }

    impl Grpc {
        pub fn server() -> proto::Server<Self> {
            proto::Server::new(Self)
        }
    }

    #[async_trait::async_trait]
    impl proto::Service for Grpc {
        type WatchInboundStream = Pin<
            Box<dyn Stream<Item = Result<proto::InboundProxyConfig, tonic::Status>> + Send + Sync>,
        >;

        async fn watch_inbound(
            &self,
            req: tonic::Request<proto::InboundProxyPort>,
        ) -> Result<tonic::Response<Self::WatchInboundStream>, tonic::Status> {
            todo!("Lookup server on pod ")
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
