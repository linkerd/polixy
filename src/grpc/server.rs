use super::proto;
use crate::{
    index::{Clients, Handle, KubeletIps, Lookup, ProxyProtocol, ServerConfig},
    k8s::{NsName, PodName},
};
use futures::prelude::*;
use std::{collections::HashMap, iter::FromIterator, sync::Arc};
use tokio_stream::wrappers::WatchStream;

#[derive(Clone, Debug)]
pub struct Server {
    index: Handle,
    drain: linkerd_drain::Watch,
    identity_domain: Arc<str>,
}

impl Server {
    pub fn new(
        index: Handle,
        drain: linkerd_drain::Watch,
        identity_domain: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            index,
            drain,
            identity_domain: identity_domain.into(),
        }
    }

    pub async fn serve(
        self,
        addr: std::net::SocketAddr,
        shutdown: impl std::future::Future<Output = ()>,
    ) -> Result<(), tonic::transport::Error> {
        tonic::transport::Server::builder()
            .add_service(proto::Server::new(self))
            .serve_with_shutdown(addr, shutdown)
            .await
    }
}

#[async_trait::async_trait]
impl proto::Service for Server {
    type WatchInboundStream = std::pin::Pin<
        Box<dyn Stream<Item = Result<proto::InboundProxyConfig, tonic::Status>> + Send + Sync>,
    >;

    async fn watch_inbound(
        &self,
        req: tonic::Request<proto::InboundProxyPort>,
    ) -> Result<tonic::Response<Self::WatchInboundStream>, tonic::Status> {
        let proto::InboundProxyPort { workload, port } = req.into_inner();

        // Parse a workload name in the form namespace:name.
        let (ns, name) = {
            let parts = workload.splitn(2, ':').collect::<Vec<_>>();
            if parts.len() != 2 {
                return Err(tonic::Status::invalid_argument(format!(
                    "Invalid workload: {}",
                    workload
                )));
            }
            (NsName::from(parts[0]), PodName::from(parts[1]))
        };

        // Ensure that the port is in the valid range.
        let port = {
            if port == 0 || port > std::u16::MAX as u32 {
                return Err(tonic::Status::invalid_argument(format!(
                    "Invalid port: {}",
                    port
                )));
            }
            port as u16
        };

        // Lookup the configuration for an inbound port. If the pod hasn't (yet)
        // been indexed, return a Not Found error.
        //
        // XXX Should we try waiting for the pod to be created? Practically, it
        // seems unlikely for a pod to request its config for the pod's
        // existence has been observed; but it's certainly possible (especially
        // if the index is recovering).
        let Lookup {
            name: _,
            kubelet_ips,
            server,
        } = self
            .index
            .lookup(ns.clone(), name.clone(), port)
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "unknown pod ns={} name={} port={}",
                    ns, name, port
                ))
            })?;

        // TODO deduplicate redundant updates.
        // TODO end streams on drain.
        let domain = self.identity_domain.clone();
        let watch = WatchStream::new(server)
            .map(WatchStream::new)
            .flat_map(move |updates| {
                let ips = kubelet_ips.clone();
                let domain = domain.clone();
                updates
                    .map(move |c| to_config(&ips, c, domain.as_ref()))
                    .map(Ok)
            });

        Ok(tonic::Response::new(Box::pin(watch)))
    }
}

fn to_config(
    kubelet_ips: &KubeletIps,
    srv: ServerConfig,
    identity_domain: &str,
) -> proto::InboundProxyConfig {
    // Convert the protocol object into a protobuf response.
    let protocol = proto::ProxyProtocol {
        kind: match srv.protocol {
            ProxyProtocol::Detect { timeout } => Some(proto::proxy_protocol::Kind::Detect(
                proto::proxy_protocol::Detect {
                    timeout: Some(timeout.into()),
                },
            )),
            ProxyProtocol::Http => Some(proto::proxy_protocol::Kind::Http(
                proto::proxy_protocol::Http {},
            )),
            ProxyProtocol::Grpc => Some(proto::proxy_protocol::Kind::Grpc(
                proto::proxy_protocol::Grpc {},
            )),
            ProxyProtocol::Opaque => Some(proto::proxy_protocol::Kind::Opaque(
                proto::proxy_protocol::Opaque {},
            )),
        },
    };

    let authorizations = srv
        .authorizations
        .into_iter()
        .map(|a| match *a {
            Clients::Unauthenticated(ref nets) => proto::Authorization {
                networks: nets
                    .iter()
                    .map(|net| proto::Network {
                        cidr: net.to_string(),
                    })
                    .collect(),
                labels: HashMap::from_iter(Some(("authn".to_string(), "false".to_string()))),
                ..Default::default()
            },

            // Authenticated connections must have TLS and apply to all
            // networks.
            Clients::Authenticated {
                ref service_accounts,
                ref identities,
                ref suffixes,
            } => proto::Authorization {
                networks: vec![
                    proto::Network {
                        cidr: "0.0.0.0/0".to_string(),
                    },
                    proto::Network {
                        cidr: "::/0".to_string(),
                    },
                ],
                tls_terminated: Some(proto::authorization::Tls {
                    client_id: Some(proto::IdMatch {
                        identities: identities
                            .iter()
                            .map(ToString::to_string)
                            .chain(
                                service_accounts
                                    .iter()
                                    .map(|sa| sa.identity(identity_domain)),
                            )
                            .collect(),
                        suffixes: suffixes
                            .iter()
                            .map(|sfx| proto::Suffix {
                                parts: sfx.iter().map(ToString::to_string).collect(),
                            })
                            .collect(),
                    }),
                }),
                labels: HashMap::from_iter(Some(("authn".to_string(), "true".to_string()))),
            },
        })
        // Traffic is always permitted from the pod's Kubelet IPs.
        .chain(Some(proto::Authorization {
            networks: kubelet_ips
                .to_nets()
                .into_iter()
                .map(|net| net.to_string())
                .map(|cidr| proto::Network { cidr })
                .collect(),
            labels: HashMap::from_iter(Some(("authorization".to_string(), "kubelet".to_string()))),
            ..Default::default()
        }))
        .collect();

    proto::InboundProxyConfig {
        authorizations,
        protocol: Some(protocol),
        ..Default::default()
    }
}
