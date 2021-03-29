#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    #[allow(unused_variables)]
    let client = kube::Client::try_default()
        .await
        .expect("Failed to initialize client");

    println!("Hello, world!");
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
        clients: Vec<Client>,
        server: Server,
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

    /// Describes a client authorized to connect to a server.
    ///
    /// Either `authenticated` or `unauthenticated` should be set.
    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    pub struct Client {
        authenticated: Option<Authenticated>,
        unauthenticated: Option<Unauthenticated>,
    }

    /// Describes an authenticated client.
    ///
    /// Exactly one of `any`, `identity`, and `service_account` should be set.
    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct Authenticated {
        /// Indicates that all authenticated clients are authorized to access a server.
        ///
        /// If set, must be true.
        any: Option<bool>,
        /// Indicates a Linkerd identity that is authorized to access a server.
        identity: Option<String>,
        /// Identifies a `ServiceAccount` authorized to access a server.
        service_account: Option<ServiceAccount>,
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
        any: Option<bool>,
        node: Option<bool>,
        network: Option<String>,
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
