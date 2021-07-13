//! Linkerd Policy Controller
//!
//! The policy controller serves discovery requests from inbound proxies, indicating how the proxy
//! should admit connections into a Pod. It watches cluster resources (Namespaces, Nodes, Pods,
//! Servers, and ServerAuthorizations).

#![deny(warnings, rust_2018_idioms)]
#![forbid(unsafe_code)]

mod index;
mod lookup;

pub use self::{index::DefaultAllow, lookup::Reader};
use polixy_controller_core::{InboundServer, IpNet};
use polixy_controller_k8s_api as k8s;
use tokio::{sync::watch, time};

/// Watches a server's configuration for server/authorization changes.
type ServerRx = watch::Receiver<InboundServer>;

/// Publishes updates for a server's configuration for server/authorization changes.
type ServerTx = watch::Sender<InboundServer>;

type ServerRxRx = watch::Receiver<ServerRx>;

/// Watches a pod port's for a new `ServerRx`.
type ServerRxTx = watch::Sender<ServerRx>;

pub fn index(
    watches: impl Into<k8s::ResourceWatches>,
    ready: watch::Sender<bool>,
    cluster_networks: Vec<IpNet>,
    identity_domain: String,
    default_mode: DefaultAllow,
    detect_timeout: time::Duration,
) -> (
    lookup::Reader,
    impl std::future::Future<Output = anyhow::Error>,
) {
    let (writer, reader) = lookup::pair();

    // Watches Nodes, Pods, Servers, and Authorizations to update the lookup map
    // with an entry for each linkerd-injected pod.
    let idx = index::Index::new(
        writer,
        cluster_networks,
        identity_domain,
        default_mode,
        detect_timeout,
    );
    let task = idx.index(watches.into(), ready);

    (reader, task)
}
