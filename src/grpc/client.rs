use super::proto;
use anyhow::Result;
use futures::prelude::*;
use ipnet::IpNet;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct Client {
    client: proto::Client<tonic::transport::Channel>,
}

#[derive(Clone, Debug)]
pub struct Inbound {
    pub authorizations: Vec<Authz>,
    pub labels: HashMap<String, String>,
    pub protocol: Protocol,
}

#[derive(Copy, Clone, Debug)]
pub enum Protocol {
    Detect,
    Opaque,
    Http,
    Grpc,
}

#[derive(Clone, Debug)]
pub enum Authz {
    Unauthenticated {
        networks: Vec<IpNet>,
        labels: HashMap<String, String>,
    },
    Authenticated {
        identities: Vec<String>,
        suffixes: Vec<Vec<String>>,
        labels: HashMap<String, String>,
    },
}

impl Client {
    pub async fn connect<D>(dst: D) -> Result<Self>
    where
        D: std::convert::TryInto<tonic::transport::Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let client = proto::Client::connect(dst).await?;
        Ok(Client { client })
    }

    pub async fn watch_inbound(
        &mut self,
        ns: String,
        pod: String,
        port: u16,
    ) -> Result<impl Stream<Item = Result<Inbound>>> {
        let req = tonic::Request::new(proto::InboundProxyPort {
            workload: format!("{}:{}", ns, pod),
            port: port.into(),
        });

        let rsp = self.client.watch_inbound(req).await?;

        let updates = rsp.into_inner().map_err(Into::into).map_ok(|c| {
            let authorizations = c
                .authorizations
                .into_iter()
                .map(
                    |proto::Authorization {
                         labels,
                         tls_terminated,
                         networks,
                     }| match tls_terminated {
                        Some(proto::authorization::Tls {
                            client_id:
                                Some(proto::IdMatch {
                                    identities,
                                    suffixes,
                                }),
                        }) => Authz::Authenticated {
                            labels,
                            identities,
                            suffixes: suffixes
                                .into_iter()
                                .map(|proto::Suffix { parts }| parts)
                                .collect(),
                        },
                        _ => Authz::Unauthenticated {
                            labels,
                            networks: networks
                                .into_iter()
                                .filter_map(|n| n.cidr.parse().ok())
                                .collect(),
                        },
                    },
                )
                .collect();

            let protocol = c
                .protocol
                .and_then(|proto::ProxyProtocol { kind }| {
                    kind.map(|k| match k {
                        proto::proxy_protocol::Kind::Detect(_) => Protocol::Detect,
                        proto::proxy_protocol::Kind::Opaque(_) => Protocol::Opaque,
                        proto::proxy_protocol::Kind::Http(_) => Protocol::Http,
                        proto::proxy_protocol::Kind::Grpc(_) => Protocol::Grpc,
                    })
                })
                .unwrap_or(Protocol::Detect);

            Inbound {
                labels: c.labels,
                authorizations,
                protocol,
            }
        });

        Ok(updates)
    }
}
