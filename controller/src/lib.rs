//! Linkerd Policy Controller
//!
//! The policy controller serves discovery requests from inbound proxies, indicating how the proxy
//! should admit connections into a Pod. It watches cluster resources (Namespaces, Nodes, Pods,
//! Servers, and ServerAuthorizations).

pub mod grpc;
mod index;
mod k8s;

pub use self::index::DefaultAllow;
use dashmap::DashMap;
use ipnet::IpNet;
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    net::IpAddr,
    sync::Arc,
};
use tokio::{sync::watch, time};

#[derive(Clone, Debug)]
pub struct LookupHandle(SharedLookupMap);

type SharedLookupMap = Arc<DashMap<(k8s::NsName, k8s::PodName), Arc<HashMap<u16, Lookup>>>>;

/// Watches a server's configuration for server/authorization changes.
type ServerRx = watch::Receiver<InboundServerConfig>;
type ServerTx = watch::Sender<InboundServerConfig>;

/// Watches a pod port's for a new `ServerRx`.
pub type ServerRxRx = watch::Receiver<ServerRx>;
type ServerRxTx = watch::Sender<ServerRx>;

#[derive(Clone, Debug)]
pub struct Lookup {
    pub name: Option<k8s::polixy::server::PortName>,
    pub pod_ips: PodIps,
    pub kubelet_ips: KubeletIps,
    pub rx: ServerRxRx,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundServerConfig {
    pub protocol: ProxyProtocol,
    pub authorizations: BTreeMap<k8s::polixy::authz::Name, ClientAuthz>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyProtocol {
    Detect { timeout: time::Duration },
    Http1,
    Http2,
    Grpc,
    Opaque,
    Tls,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientAuthz {
    pub networks: Arc<[ClientNetwork]>,
    pub authentication: ClientAuthn,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientNetwork {
    pub net: IpNet,
    pub except: Vec<IpNet>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientAuthn {
    Unauthenticated,
    TlsUnauthenticated,
    TlsAuthenticated {
        service_accounts: Vec<ServiceAccountRef>,
        identities: Vec<Identity>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Identity {
    Name(Arc<str>),
    Suffix(Arc<[String]>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServiceAccountRef {
    ns: k8s::NsName,
    name: Arc<str>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct PodIps(Arc<[IpAddr]>);

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct KubeletIps(Arc<[IpAddr]>);

// === impl LookupHandle ===

impl LookupHandle {
    pub fn run(
        watches: impl Into<k8s::ResourceWatches>,
        cluster_networks: Vec<ipnet::IpNet>,
        default_mode: DefaultAllow,
        detect_timeout: time::Duration,
    ) -> (Self, impl std::future::Future<Output = anyhow::Error>) {
        let lookups = SharedLookupMap::default();

        // Watches Nodes, Pods, Servers, and Authorizations to update the lookup map
        // with an entry for each linkerd-injected pod.
        let idx = index::Index::new(
            lookups.clone(),
            cluster_networks,
            default_mode,
            detect_timeout,
        );

        (Self(lookups), idx.index(watches.into()))
    }

    pub fn lookup(&self, ns: k8s::NsName, name: k8s::PodName, port: u16) -> Option<Lookup> {
        self.0.get(&(ns, name))?.get(&port).cloned()
    }
}

// === impl Identity ===

impl fmt::Display for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Name(name) => name.fmt(f),
            Self::Suffix(suffix) => {
                write!(f, "*")?;
                for part in suffix.iter() {
                    write!(f, ".{}", part)?;
                }
                Ok(())
            }
        }
    }
}

// === impl PodIps ===

impl PodIps {
    pub fn iter(&self) -> std::slice::Iter<'_, IpAddr> {
        self.0.iter()
    }
}

// === impl KubeletIps ===

impl KubeletIps {
    pub fn to_nets(&self) -> Vec<IpNet> {
        self.0.iter().copied().map(IpNet::from).collect()
    }
}
