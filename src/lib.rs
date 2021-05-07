pub mod grpc;
mod index;
mod k8s;

use anyhow::{anyhow, Error, Result};
use dashmap::DashMap;
use ipnet::IpNet;
use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    net::IpAddr,
    sync::Arc,
};
use tokio::{sync::watch, time};

trait FromResource<T> {
    fn from_resource(resource: &T) -> Self;
}

#[derive(Copy, Clone, Debug)]
pub enum DefaultMode {
    AllowExternal,
    AllowCluster,
    AllowAuthenticated,
    Deny,
}

#[derive(Clone, Debug)]
pub struct LookupHandle(SharedLookupMap);

type SharedLookupMap = Arc<DashMap<(k8s::NsName, k8s::PodName), Arc<HashMap<u16, Lookup>>>>;

type ServerRx = watch::Receiver<InboundServerConfig>;
type ServerTx = watch::Sender<InboundServerConfig>;
pub type ServerRxRx = watch::Receiver<ServerRx>;
type ServerRxTx = watch::Sender<ServerRx>;

#[derive(Clone, Debug)]
pub struct Lookup {
    pub name: Option<k8s::polixy::server::PortName>,
    pub pod_ips: PodIps,
    pub kubelet_ips: KubeletIps,
    pub rx: ServerRxRx,
}

#[derive(Clone, Debug)]
pub struct InboundServerConfig {
    pub protocol: ProxyProtocol,
    pub authorizations: BTreeMap<Option<k8s::polixy::authz::Name>, ClientAuthz>,
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
    Authenticated {
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
        client: kube::Client,
        cluster_networks: Vec<ipnet::IpNet>,
        default_mode: DefaultMode,
    ) -> (Self, impl std::future::Future<Output = anyhow::Error>) {
        let lookups = SharedLookupMap::default();

        // Watches Nodes, Pods, Servers, and Authorizations to update the lookup map
        // with an entry for each linkerd-injected pod.
        let idx = index::Index::new(lookups.clone(), cluster_networks, default_mode);

        (Self(lookups), idx.index(k8s::ResourceWatches::new(client)))
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

// === impl DefaultMode ===

impl std::str::FromStr for DefaultMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "allow-external" => Ok(Self::AllowExternal),
            "allow-cluster" => Ok(Self::AllowCluster),
            "allow-authenticated" => Ok(Self::AllowAuthenticated),
            "deny" => Ok(Self::Deny),
            s => Err(anyhow!("invalid mode: {}", s)),
        }
    }
}
