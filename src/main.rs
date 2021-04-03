#![allow(warnings)]

use futures::{future, prelude::*};
use kube::{
    api::{ListParams, Resource},
    Api,
};
use kube_runtime::{watcher, watcher::Event};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{watch, Mutex};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let client = kube::Client::try_default()
        .await
        .expect("Failed to initialize client");

    let authz_api = Api::<authz::Authorization>::all(client.clone());
    let srv_api = Api::<server::Server>::all(client);

    let authz_task = tokio::spawn(async move {
        let mut authz = watcher(authz_api, ListParams::default()).boxed();
        while let Some(ev) = authz.try_next().await? {
            match ev {
                Event::Applied(authz) => {
                    println!("+ {} {}", authz.namespace().unwrap(), authz.name());
                    println!("{:#?}", authz);
                }
                Event::Deleted(authz) => {
                    println!("- {} {}", authz.namespace().unwrap(), authz.name());
                    println!("{:#?}", authz);
                }
                Event::Restarted(authzs) => {
                    for authz in authzs.into_iter() {
                        println!("* {} {}", authz.namespace().unwrap(), authz.name());
                        println!("{:#?}", authz);
                    }
                }
            }
        }
        Ok::<(), kube_runtime::watcher::Error>(())
    });

    let srv_task = tokio::spawn(async move {
        let mut srvs = watcher(srv_api, ListParams::default()).boxed();
        while let Some(ev) = srvs.try_next().await? {
            match ev {
                Event::Applied(srv) => {
                    println!("+ {} {}", srv.namespace().unwrap(), srv.name());
                    println!("{:#?}", srv);
                }
                Event::Deleted(srv) => {
                    println!("- {} {}", srv.namespace().unwrap(), srv.name());
                    println!("{:#?}", srv);
                }
                Event::Restarted(srvs) => {
                    for srv in srvs.into_iter() {
                        println!("* {} {}", srv.namespace().unwrap(), srv.name());
                        println!("{:#?}", srv);
                    }
                }
            }
        }
        Ok::<(), kube_runtime::watcher::Error>(())
    });

    let ctrl_c = tokio::signal::ctrl_c();
    let term = async move {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => term.recv().await,
            _ => future::pending().await,
        }
    };

    tokio::select! {
        res = authz_task => match res.expect("Spawn must succeed") {
            Ok(()) => eprintln!("authz watch completed"),
            Err(e) => eprintln!("authz error: {}", e),
        },
        res = srv_task => match res.expect("Spawn must succeed") {
            Ok(()) => eprintln!("srv watch completed"),
            Err(e) => eprintln!("srv error: {}", e),
        },
        _ = ctrl_c => {}
        _ = term => {}
    };
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
        identities: Option<String>,
        /// Identifies a `ServiceAccount` authorized to access a server.
        service_accounts: Vec<ServiceAccount>,
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
        networks: Vec<String>,
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
