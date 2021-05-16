use super::super::labels;
use kube::{api::Resource, CustomResource};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(
    Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema,
)]
pub struct Name(String);

/// Describes a server interface exposed by a set of pods.
#[derive(Clone, Debug, CustomResource, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "polixy.linkerd.io",
    version = "v1alpha1",
    kind = "Server",
    namespaced
)]
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
    #[serde(rename = "unknown")]
    Unknown,
    #[serde(rename = "HTTP/1")]
    Http1,
    #[serde(rename = "HTTP/2")]
    Http2,
    #[serde(rename = "gRPC")]
    Grpc,
    #[serde(rename = "opaque")]
    Opaque,
    #[serde(rename = "TLS")]
    Tls,
}

// === Name ===

impl Name {
    pub fn from_server(s: &Server) -> Self {
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
