use futures::Stream;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::{collections::BTreeMap, time::Duration};
use tokio::sync::watch;

#[derive(Clone, Debug)]
pub struct InboundServerRx(watch::Receiver<watch::Receiver<InboundServer>>);

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

// === impl InboundServerRx ===

impl From<watch::Receiver<watch::Receiver<InboundServer>>> for InboundServerRx {
    fn from(rx: watch::Receiver<watch::Receiver<InboundServer>>) -> Self {
        Self(rx)
    }
}

impl InboundServerRx {
    pub fn into_stream(self) -> impl Stream<Item = InboundServer> + Send + Sync {
        let mut watch_rx = self.0;
        let mut server_rx = (*watch_rx.borrow()).clone();
        let mut server = (*server_rx.borrow()).clone();

        async_stream::stream! {
            yield server.clone();

            loop {
                tokio::select! {
                    res = watch_rx.changed() => {
                        if res.is_err() {
                            return;
                        }
                        server_rx = watch_rx.borrow().clone();
                    }

                    res = server_rx.changed() => {
                        if res.is_err() {
                            server_rx = watch_rx.borrow().clone();
                        }
                    }
                }

                let s = (*server_rx.borrow()).clone();
                if s != server {
                    server = s;
                    yield server.clone();
                }
            }
        }
    }
}
