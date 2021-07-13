#![deny(warnings, rust_2018_idioms)]
#![forbid(unsafe_code)]

//! Linkerd Policy Controller
//!
//! The policy controller serves discovery requests from inbound proxies, indicating how the proxy
//! should admit connections into a Pod. It watches cluster resources (Namespaces, Nodes, Pods,
//! Servers, and ServerAuthorizations).

pub mod admin;
pub mod grpc;
mod index;
mod k8s;
pub mod lookup;

pub use self::index::DefaultAllow;
use polixy_controller_core::InboundServer;
use std::{net::IpAddr, sync::Arc};
use tokio::{sync::watch, time};

/// Watches a server's configuration for server/authorization changes.
type ServerRx = watch::Receiver<InboundServer>;

/// Publishes updates for a server's configuration for server/authorization changes.
type ServerTx = watch::Sender<InboundServer>;

/// Watches a pod port's for a new `ServerRx`.
pub type ServerRxRx = watch::Receiver<ServerRx>;
type ServerRxTx = watch::Sender<ServerRx>;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceAccountRef {
    ns: String,
    name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct KubeletIps(Arc<[IpAddr]>);

pub fn index(
    watches: impl Into<k8s::ResourceWatches>,
    ready: watch::Sender<bool>,
    cluster_networks: Vec<ipnet::IpNet>,
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

// === impl KubeletIps ===

impl std::ops::Deref for KubeletIps {
    type Target = [IpAddr];

    fn deref(&self) -> &[IpAddr] {
        &*self.0
    }
}
