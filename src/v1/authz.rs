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
    sever: Server,
    authenticated: Option<Authenticated>,
    unauthenticated: Option<Unauthenticated>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct Server {
    name: Option<String>,
    selector: Option<labels::Selector>,
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
    service_account_refs: Option<Vec<ServiceAccountRef>>,
}

/// References a Kubernetes `ServiceAccount` instance.
///
/// If no namespace is specified, the `Authorization`'s namespace is used.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountRef {
    namespace: Option<String>,
    name: String,
}

/// Describes an unauthenticated client.
///
/// Exactly one of `any`, `node`, and `network` should be set.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct Unauthenticated {
    networks: Option<Vec<String>>,
}
