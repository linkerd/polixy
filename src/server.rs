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
