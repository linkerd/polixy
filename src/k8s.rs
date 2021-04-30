use crate::FromResource;
pub use k8s_openapi::api::core::v1::{Node, Pod};
use kube::Resource;
use std::fmt;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NodeName(String);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NsName(String);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PodName(String);

// === NodeName ===

impl FromResource<Node> for NodeName {
    fn from_resource(n: &Node) -> Self {
        Self(n.name())
    }
}

impl<T: Into<String>> From<T> for NodeName {
    fn from(ns: T) -> Self {
        Self(ns.into())
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === NsName ===

impl<T: Resource> FromResource<T> for NsName {
    fn from_resource(t: &T) -> Self {
        t.namespace().unwrap_or_else(|| "default".into()).into()
    }
}

impl<T: Into<String>> From<T> for NsName {
    fn from(ns: T) -> Self {
        Self(ns.into())
    }
}

impl fmt::Display for NsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === PodName ===

impl FromResource<Pod> for PodName {
    fn from_resource(p: &Pod) -> Self {
        Self(p.name())
    }
}

impl<T: Into<String>> From<T> for PodName {
    fn from(pod: T) -> Self {
        Self(pod.into())
    }
}

impl fmt::Display for PodName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
