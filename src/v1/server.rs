use super::labels;
use crate::FromResource;
use kube::{api::Resource, CustomResource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub struct Name(String);

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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
pub struct PortName(String);

/// References a pod spec's port by name or number.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Deserialize, Serialize, JsonSchema)]
#[serde(untagged)]
pub enum Port {
    Number(u16),
    Name(PortName),
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
pub enum ProxyProtocol {
    Detect,
    Opaque,
    Http,
    Grpc,
}

// === Name ===

impl FromResource<Server> for Name {
    fn from_resource(s: &Server) -> Self {
        Self(s.name())
    }
}

impl fmt::Display for Name {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === PortName ===

impl<T: Into<String>> From<T> for PortName {
    fn from(p: T) -> Self {
        Self(p.into())
    }
}

impl fmt::Display for PortName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
