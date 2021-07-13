#![deny(warnings, rust_2018_idioms)]
#![forbid(unsafe_code)]

use anyhow::Result;
use futures::prelude::*;
pub use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::{collections::BTreeMap, net::IpAddr, pin::Pin, time::Duration};

#[async_trait::async_trait]
pub trait DiscoverInboundServer<T> {
    type Rx: InboundServerRx;

    async fn discover_inbound_server(&self, target: T) -> Result<Option<Self::Rx>>;
}

pub trait InboundServerRx {
    fn get(&self) -> InboundServer;

    fn into_stream(self) -> InboundServerRxStream;
}

pub type InboundServerRxStream = Pin<Box<dyn Stream<Item = InboundServer> + Send + Sync + 'static>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundServer {
    pub protocol: ProxyProtocol,
    pub authorizations: BTreeMap<String, ClientAuthorization>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientAuthentication {
    Unauthenticated,
    TlsUnauthenticated,
    TlsAuthenticated(Vec<ClientIdentityMatch>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientAuthorization {
    pub networks: Vec<ClientNetwork>,
    pub authentication: ClientAuthentication,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ClientIdentityMatch {
    Name(String),
    Suffix(Vec<String>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientNetwork {
    pub net: IpNet,
    pub except: Vec<IpNet>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyProtocol {
    Detect { timeout: Duration },
    Http1,
    Http2,
    Grpc,
    Opaque,
    Tls,
}

// === impl ClientIdentityMatch ===

impl std::fmt::Display for ClientIdentityMatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

// === impl ClientNetwork ===

impl From<IpNet> for ClientNetwork {
    fn from(net: IpNet) -> Self {
        Self {
            net,
            except: vec![],
        }
    }
}

impl From<IpAddr> for ClientNetwork {
    fn from(net: IpAddr) -> Self {
        IpNet::from(net).into()
    }
}

impl From<Ipv4Net> for ClientNetwork {
    fn from(net: Ipv4Net) -> Self {
        IpNet::from(net).into()
    }
}

impl From<Ipv6Net> for ClientNetwork {
    fn from(net: Ipv6Net) -> Self {
        IpNet::from(net).into()
    }
}
