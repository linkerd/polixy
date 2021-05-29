use kube::api::{Api, ListParams};
use kube_runtime::watcher;

pub mod labels;
pub mod polixy;
mod watch;

pub use self::{
    labels::Labels,
    watch::{Event, Watch},
};
pub use k8s_openapi::{
    api::{
        self,
        core::v1::{Namespace, Node, NodeSpec, Pod, PodSpec, PodStatus},
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
pub use kube::api::ResourceExt;

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
