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

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct PodSelector {
        match_labels: labels::Map,
        match_expressions: labels::Expressions,
    }

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
        clients: Vec<Client>,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct Server {
        name: String,
        match_labels: labels::Map,
        match_expressions: labels::Expressions,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    pub struct Client {
        authenticated: Authenticated,
        unauthenticated: Unauthenticated,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct Authenticated {
        any: bool,
        identity: String,
        service_account: ServiceAccount,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct ServiceAccount {
        name: String,
        namespace: String,
        match_labels: labels::Map,
        match_expressions: labels::Expressions,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    pub struct Unauthenticated {
        any: bool,
        node: bool,
        network: String,
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
