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
    pub pod_selector: labels::Match,
    pub container_name: Option<String>,
    pub port: Port,
}

/// References a pod spec's port by name or number.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(untagged)]
pub enum Port {
    Number(u16),
    Name(String),
}
