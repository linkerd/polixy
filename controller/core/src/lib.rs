#![deny(warnings, rust_2018_idioms)]
#![forbid(unsafe_code)]

mod network_match;

pub use self::network_match::NetworkMatch;
use anyhow::Result;
use futures::prelude::*;
pub use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::{collections::BTreeMap, pin::Pin, time::Duration};

/// Models inbound server configuration discovery.
#[async_trait::async_trait]
pub trait DiscoverInboundServer<T> {
    type Rx: InboundServerRx;

    async fn discover_inbound_server(&self, target: T) -> Result<Option<Self::Rx>>;
}

/// A discovered server configuration may be queried or streamed.
pub trait InboundServerRx {
    /// Query server configuration.
    fn get(&self) -> InboundServer;

    /// Stream server configuration updates.
    fn into_stream(self) -> InboundServerRxStream;
}

pub type InboundServerRxStream = Pin<Box<dyn Stream<Item = InboundServer> + Send + Sync + 'static>>;

/// Inbound server configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboundServer {
    pub protocol: ProxyProtocol,
    pub authorizations: BTreeMap<String, ClientAuthorization>,
}

/// Describes how a proxy should handle inbound connections.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyProtocol {
    /// Indicates that the protocol should be discovered dynamically.
    Detect {
        timeout: Duration,
    },

    Http1,
    Http2,
    Grpc,

    /// Indicates that connections should be handled opaquely.
    Opaque,

    /// Indicates that connections should be handled as application-terminated TLS.
    Tls,
}

/// Describes a class of authorized clients.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientAuthorization {
    /// Limits which source networks this authorization applies to.
    pub networks: Vec<NetworkMatch>,

    /// Describes the client's authentication requirements.
    pub authentication: ClientAuthentication,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientAuthentication {
    /// Indicates that clients need not be authenticated.
    Unauthenticated,

    /// Indicates that clients must use TLS bu need not provide a client identity.
    TlsUnauthenticated,

    /// Indicates that clients must use mutually-authenticated TLS.
    TlsAuthenticated(Vec<ClientIdentityMatch>),
}

/// Matches a client's mesh identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ClientIdentityMatch {
    /// An exact match.
    Name(String),

    /// A suffix match..
    Suffix(Vec<String>),
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
