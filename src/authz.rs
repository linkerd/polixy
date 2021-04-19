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
    server_selector: labels::Selector,
    authenticated: Option<Authenticated>,
    unauthenticated: Option<Unauthenticated>,
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
