use kube::api::{Api, ListParams};
use kube_runtime::watcher;
use std::{fmt, sync::Arc};

pub mod labels;
pub mod polixy;
mod watch;

pub use self::{
    labels::Labels,
    watch::{Event, Watch},
};
pub use k8s_openapi::{
    api::core::v1::{Namespace, Node, Pod, PodSpec, PodStatus},
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
pub use kube::api::ResourceExt;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NodeName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct NsName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PodName(Arc<str>);

/// Resource watches.
pub struct ResourceWatches {
    pub(crate) namespaces: Watch<Namespace>,
    pub(crate) nodes: Watch<Node>,
    pub(crate) pods: Watch<Pod>,
    pub(crate) servers: Watch<polixy::Server>,
    pub(crate) authorizations: Watch<polixy::ServerAuthorization>,
}

// === impl ResourceWatches ===

impl From<kube::Client> for ResourceWatches {
    fn from(client: kube::Client) -> Self {
        Self {
            namespaces: watcher(Api::all(client.clone()), ListParams::default()).into(),
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

impl NodeName {
    pub fn from_node(n: &Node) -> Self {
        Self::from(n.name())
    }
}

impl<T: Into<Arc<str>>> From<T> for NodeName {
    fn from(ns: T) -> Self {
        Self(ns.into())
    }
}

impl std::borrow::Borrow<str> for NodeName {
    fn borrow(&self) -> &str {
        self.0.as_ref()
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === NsName ===

impl NsName {
    pub fn from_ns(ns: &Namespace) -> Self {
        Self(ns.name().into())
    }

    pub fn from_pod(p: &Pod) -> Self {
        let ns = p.namespace().expect("Pods must be namespaced");
        Self::from_string(ns)
    }

    pub fn from_srv(s: &polixy::Server) -> Self {
        let ns = s.namespace().expect("Servers must be namespaced");
        Self::from_string(ns)
    }

    pub fn from_authz(s: &polixy::ServerAuthorization) -> Self {
        let ns = s
            .namespace()
            .expect("ServerAuthorizations must be namespaced");
        Self::from_string(ns)
    }

    pub fn from_string(ns: String) -> Self {
        if ns.is_empty() {
            panic!("namespaces must not be empty")
        }
        Self(ns.into())
    }
}

impl std::borrow::Borrow<str> for NsName {
    fn borrow(&self) -> &str {
        self.0.as_ref()
    }
}

impl fmt::Display for NsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// === PodName ===

impl PodName {
    pub fn from_pod(p: &Pod) -> Self {
        Self::from(p.name())
    }
}

impl std::borrow::Borrow<str> for PodName {
    fn borrow(&self) -> &str {
        self.0.as_ref()
    }
}

impl<T: Into<Arc<str>>> From<T> for PodName {
    fn from(pod: T) -> Self {
        Self(pod.into())
    }
}

impl fmt::Display for PodName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
