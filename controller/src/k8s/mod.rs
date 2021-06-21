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

impl ResourceWatches {
    const DEFAULT_TIMEOUT_SECS: u32 = 5 * 60;
}

impl From<kube::Client> for ResourceWatches {
    fn from(client: kube::Client) -> Self {
        let params = ListParams::default().timeout(Self::DEFAULT_TIMEOUT_SECS);
        Self {
            nodes_rx: watcher(Api::all(client.clone()), params.clone()).into(),
            pods_rx: watcher(
                Api::all(client.clone()),
                params.clone().labels("linkerd.io/control-plane-ns"),
            )
            .into(),
            servers_rx: watcher(Api::all(client.clone()), params.clone()).into(),
            authorizations_rx: watcher(Api::all(client), params).into(),
        }
    }
}
