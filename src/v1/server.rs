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
#[derive(Clone, Debug, CustomResource, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ServerSpec {
    pub pod_selector: labels::Selector,
    pub port: Port,
    pub proxy_protocol: Option<ProxyProtocol>,
}

/// References a pod spec's port by name or number.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum Port {
    Number(u16),
    Name(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub enum ProxyProtocol {
    Detect,
    Opaque,
    Http,
    Grpc,
}
