use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type MatchLabels = HashMap<String, String>;

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
struct MatchExpr {
    key: String,
    operator: MatchExprOperator,
    values: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
enum MatchExprOperator {
    In,
    NotIn,
}

mod server {
    use super::{MatchExpr, MatchLabels};
    use kube::CustomResource;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[kube(
        group = "polixy.olix0r.net",
        version = "v1",
        kind = "Server",
        namespaced
    )]
    #[serde(rename_all = "camelCase")]
    pub struct ServerSpec {
        pod_selector: PodSelector,
        port: Port,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct PodSelector {
        match_labels: MatchLabels,
        match_expressions: Vec<MatchExpr>,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(untagged)]
    enum Port {
        Number(u16),
        Name(String),
    }
}

mod authz {
    use super::{MatchExpr, MatchLabels};
    use kube::CustomResource;
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    #[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[kube(
        group = "polixy.olix0r.net",
        version = "v1",
        kind = "Authorization",
        namespaced
    )]
    #[serde(rename_all = "camelCase")]
    pub struct AuthorizationSpec {
        server: Server,
        clients: Vec<Client>,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct Server {
        name: String,
        match_labels: MatchLabels,
        match_expressions: Vec<MatchExpr>,
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
        match_labels: MatchLabels,
        match_expressions: Vec<MatchExpr>,
    }

    #[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
    pub struct Unauthenticated {
        any: bool,
        node: bool,
        network: String,
    }
}

fn main() {
    println!("Hello, world!");
}
