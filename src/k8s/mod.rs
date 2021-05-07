use crate::FromResource;
use kube::{api::ListParams, Api, Resource};
use kube_runtime::watcher;
use std::fmt;

pub mod labels;
pub mod polixy;
mod watch;

pub use self::{
    labels::Labels,
    watch::{Event, Watch},
};
pub use k8s_openapi::api::core::v1::{Namespace, Node, Pod};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NodeName(String);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NsName(String);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PodName(String);

/// Resource watches.
pub(crate) struct ResourceWatches {
    //pub namespaces: Watch<Namespace>,
    pub nodes: Watch<Node>,
    pub pods: Watch<Pod>,
    pub servers: Watch<polixy::Server>,
    pub authorizations: Watch<polixy::ServerAuthorization>,
}

// === impl ResourceWatches ===

impl ResourceWatches {
    pub fn new(client: kube::Client) -> Self {
        Self {
            //namespaces: watcher(Api::all(client.clone()), ListParams::default()).into(),
            nodes: watcher(Api::all(client.clone()), ListParams::default()).into(),
            pods: watcher(
                Api::all(client.clone()),
                ListParams::default().labels("linkerd.io/control-plane-ns"),
            )
            .into(),
            servers: watcher(Api::all(client.clone()), ListParams::default()).into(),
            authorizations: watcher(Api::all(client), ListParams::default()).into(),
        }
    }
}

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

impl NsName {
    pub fn from_ns(ns: &Namespace) -> Self {
        ns.name().into()
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
