use super::proto;
use anyhow::{anyhow, bail, Context, Error, Result};
use futures::prelude::*;
use ipnet::IpNet;
use std::{collections::HashMap, convert::TryInto};
use tokio::time;

#[derive(Clone, Debug)]
pub struct Client {
    client: proto::client::PolixyClient<tonic::transport::Channel>,
}

#[derive(Clone, Debug)]
pub struct Inbound {
    pub authorizations: Vec<Authz>,
    pub labels: HashMap<String, String>,
    pub protocol: Protocol,
}

#[derive(Copy, Clone, Debug)]
pub enum Protocol {
    Detect { timeout: time::Duration },
    Opaque,
    Http,
    Grpc,
}

#[derive(Clone, Debug)]
pub struct Authz {
    networks: Vec<Network>,
    authn: Authn,
    labels: HashMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct Network {
    net: IpNet,
    except: Vec<IpNet>,
}

#[derive(Clone, Debug)]
pub enum Authn {
    Unauthenticated,
    Authenticated {
        identities: Vec<String>,
        suffixes: Vec<Vec<String>>,
    },
}

// === impl Client ===

impl Client {
    pub async fn connect<D>(dst: D) -> Result<Self>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let client = proto::client::PolixyClient::connect(dst).await?;
        Ok(Client { client })
    }

    pub async fn get_inbound_port(
        &mut self,
        ns: String,
        pod: String,
        port: u16,
    ) -> Result<Inbound> {
        let req = tonic::Request::new(proto::InboundPort {
            workload: format!("{}:{}", ns, pod),
            port: port.into(),
        });

        self.client
            .get_inbound_port(req)
            .await?
            .into_inner()
            .try_into()
    }

    pub async fn watch_inbound_port(
        &mut self,
        ns: String,
        pod: String,
        port: u16,
    ) -> Result<impl Stream<Item = Result<Inbound>>> {
        let req = tonic::Request::new(proto::InboundPort {
            workload: format!("{}:{}", ns, pod),
            port: port.into(),
        });

        let rsp = self.client.watch_inbound_port(req).await?;

        let updates = rsp
            .into_inner()
            .map_err(Into::into)
            .and_then(|c| future::ready(c.try_into()));

        Ok(updates)
    }
}

// === impl Inbound ===

impl std::convert::TryFrom<proto::InboundServer> for Inbound {
    type Error = Error;

    fn try_from(proto: proto::InboundServer) -> Result<Self> {
        let protocol = match proto.protocol {
            Some(proto::ProxyProtocol { kind: Some(k) }) => match k {
                proto::proxy_protocol::Kind::Detect(proto::proxy_protocol::Detect { timeout }) => {
                    Protocol::Detect {
                        timeout: match timeout {
                            Some(t) => t
                                .try_into()
                                .map_err(|t| anyhow!("negative detect timeout: {:?}", t))?,
                            None => bail!("protocol missing detect timeout"),
                        },
                    }
                }
                proto::proxy_protocol::Kind::Opaque(_) => Protocol::Opaque,
                proto::proxy_protocol::Kind::Http(_) => Protocol::Http,
                proto::proxy_protocol::Kind::Grpc(_) => Protocol::Grpc,
            },
            _ => bail!("proxy protocol missing"),
        };

        let authorizations = proto
            .authorizations
            .into_iter()
            .map(
                |proto::Authz {
                     labels,
                     authentication,
                     networks,
                 }| {
                    if networks.is_empty() {
                        bail!("networks missing");
                    }
                    let networks = networks
                        .iter()
                        .map(|proto::Network { cidr, except }| {
                            let net = cidr
                                .parse()
                                .with_context(|| format!("invalid network CIDR {}", cidr))?;
                            let except = except
                                .iter()
                                .map(|cidr| {
                                    cidr.parse()
                                        .with_context(|| format!("invalid network CIDR {}", cidr))
                                })
                                .collect::<Result<Vec<IpNet>>>()?;
                            Ok(Network { net, except })
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let authn = match authentication.and_then(|proto::Authn { permit }| permit) {
                        Some(proto::authn::Permit::Unauthenticated(_)) => Authn::Unauthenticated,
                        Some(proto::authn::Permit::ProxyIdentities(
                            proto::authn::PermitProxyIdentities {
                                identities,
                                suffixes,
                            },
                        )) => Authn::Authenticated {
                            identities: identities
                                .into_iter()
                                .map(|proto::Identity { name }| name)
                                .collect(),
                            suffixes: suffixes
                                .into_iter()
                                .map(|proto::Suffix { parts }| parts)
                                .collect(),
                        },
                        authn => bail!("no authentication provided: {:?}", authn),
                    };

                    Ok(Authz {
                        networks,
                        labels,
                        authn,
                    })
                },
            )
            .collect::<Result<Vec<_>>>()?;

        Ok(Inbound {
            labels: proto.labels,
            authorizations,
            protocol,
        })
    }
}
