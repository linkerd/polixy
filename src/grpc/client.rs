use super::proto;
use anyhow::{anyhow, bail, Context, Error, Result};
use futures::prelude::*;
use ipnet::IpNet;
use std::{collections::HashMap, convert::TryInto, net::IpAddr};
use tokio::time;
use tracing::trace;

#[derive(Clone, Debug)]
pub struct Client {
    client: proto::polixy_client::PolixyClient<tonic::transport::Channel>,
}

#[derive(Clone, Debug)]
pub struct Inbound {
    pub authorizations: Vec<Authz>,
    pub labels: HashMap<String, String>,
    pub protocol: Protocol,
    pub server_ips: Vec<IpAddr>,
}

#[derive(Copy, Clone, Debug)]
pub enum Protocol {
    Detect { timeout: time::Duration },
    Http1,
    Http2,
    Grpc,
    Opaque,
    Tls,
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
        let client = proto::polixy_client::PolixyClient::connect(dst).await?;
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

        let proto = self.client.get_inbound_port(req).await?.into_inner();
        trace!(?proto);
        proto.try_into()
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
                proto::proxy_protocol::Kind::Http1(_) => Protocol::Http1,
                proto::proxy_protocol::Kind::Http2(_) => Protocol::Http2,
                proto::proxy_protocol::Kind::Grpc(_) => Protocol::Grpc,
                proto::proxy_protocol::Kind::Opaque(_) => Protocol::Opaque,
                proto::proxy_protocol::Kind::Tls(_) => Protocol::Tls,
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
                        .into_iter()
                        .map(|proto::Network { net, except }| {
                            let net = net
                                .ok_or_else(|| anyhow!("network missing"))?
                                .try_into()
                                .context("invalid network")?;
                            let except = except
                                .into_iter()
                                .map(|net| net.try_into().context("invalid network"))
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
                        authn,
                        labels,
                    })
                },
            )
            .collect::<Result<Vec<_>>>()?;

        let server_ips = proto
            .server_ips
            .into_iter()
            .map(|ip| ip.try_into().map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;

        Ok(Inbound {
            labels: proto.labels,
            authorizations,
            protocol,
            server_ips,
        })
    }
}
