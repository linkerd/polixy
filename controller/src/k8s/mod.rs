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
pub struct NsName(Arc<str>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PodName(Arc<str>);

/// Resource watches.
pub struct ResourceWatches {
    pub(crate) nodes_rx: Watch<Node>,
    pub(crate) pods_rx: Watch<Pod>,
    pub(crate) servers_rx: Watch<polixy::Server>,
    pub(crate) authorizations_rx: Watch<polixy::ServerAuthorization>,
}

// === impl ResourceWatches ===

impl From<kube::Client> for ResourceWatches {
    fn from(client: kube::Client) -> Self {
        Self {
            nodes_rx: watcher(Api::all(client.clone()), ListParams::default()).into(),
            pods_rx: watcher(
                Api::all(client.clone()),
                ListParams::default().labels("linkerd.io/control-plane-ns"),
            )
            .into(),
            servers_rx: watcher(Api::all(client.clone()), ListParams::default()).into(),
            authorizations_rx: watcher(Api::all(client), ListParams::default()).into(),
        }
    }
}

// === NsName ===

impl NsName {
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

impl AsRef<str> for NsName {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
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

impl AsRef<str> for PodName {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
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
