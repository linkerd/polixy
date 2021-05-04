use super::proto;
use crate::{
    k8s::{NsName, PodName},
    ClientAuthn, ClientAuthz, ClientNetwork, Identity, InboundServerConfig, Lookup, LookupHandle,
    ProxyProtocol, ServiceAccountRef,
};
use futures::prelude::*;
use std::{collections::HashMap, iter::FromIterator, sync::Arc};
use tokio_stream::wrappers::WatchStream;
use tracing::trace;

#[derive(Clone, Debug)]
pub struct Server {
    lookup: LookupHandle,
    drain: linkerd_drain::Watch,
    identity_domain: Arc<str>,
}

impl Server {
    pub fn new(
        lookup: LookupHandle,
        drain: linkerd_drain::Watch,
        identity_domain: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            lookup,
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
            rx,
        } = self
            .lookup
            .lookup(ns.clone(), name.clone(), port)
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "unknown pod ns={} name={} port={}",
                    ns, name, port
                ))
            })?;

        // Traffic is always permitted from the pod's Kubelet IPs.
        let kubelet_authz = proto::Authz {
            networks: kubelet_ips
                .to_nets()
                .into_iter()
                .map(|net| proto::Network {
                    cidr: net.to_string(),
                    except: vec![],
                })
                .collect(),
            authentication: Some(proto::Authn {
                permit: Some(proto::authn::Permit::Unauthenticated(
                    proto::authn::PermitUnauthenticated {},
                )),
            }),
            labels: Some(("authn".to_string(), "false".to_string()))
                .into_iter()
                .chain(Some(("name".to_string(), "_kubelet".to_string())))
                .collect(),
        };

        // TODO deduplicate redundant updates.
        // TODO end streams on drain.
        let domain = self.identity_domain.clone();
        let watch = WatchStream::new(rx)
            .map(WatchStream::new)
            .flat_map(move |updates| {
                let kubelet = kubelet_authz.clone();
                let domain = domain.clone();
                updates
                    .map(move |c| to_config(&kubelet, c, domain.as_ref()))
                    .map(Ok)
            });

        Ok(tonic::Response::new(Box::pin(watch)))
    }
}

fn to_config(
    kubelet_authz: &proto::Authz,
    srv: InboundServerConfig,
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
    trace!(?protocol);

    let server_authzs = srv
        .authorizations
        .into_iter()
        .map(|(n, c)| to_authz(n, c, identity_domain));
    trace!(?kubelet_authz);
    trace!(?server_authzs);

    proto::InboundProxyConfig {
        authorizations: Some(kubelet_authz.clone())
            .into_iter()
            .chain(server_authzs)
            .collect(),
        protocol: Some(protocol),
        ..Default::default()
    }
}

fn to_authz(
    name: Option<impl ToString>,
    ClientAuthz {
        networks,
        authentication,
    }: ClientAuthz,
    identity_domain: &str,
) -> proto::Authz {
    let networks = if networks.is_empty() {
        // TODO use cluster networks (from config).
        vec![
            proto::Network {
                cidr: "0.0.0.0/0".to_string(),
                except: vec![],
            },
            proto::Network {
                cidr: "::/0".to_string(),
                except: vec![],
            },
        ]
    } else {
        networks
            .iter()
            .map(|ClientNetwork { net, except }| proto::Network {
                cidr: net.to_string(),
                except: except.iter().map(ToString::to_string).collect(),
            })
            .collect()
    };

    match authentication {
        ClientAuthn::Unauthenticated => proto::Authz {
            networks,
            labels: HashMap::from_iter(Some(("authn".to_string(), "false".to_string()))),
            ..Default::default()
        },

        // Authenticated connections must have TLS and apply to all
        // networks.
        ClientAuthn::Authenticated {
            service_accounts,
            identities,
        } => {
            let labels = Some(("authn".to_string(), "true".to_string()))
                .into_iter()
                .chain(Some((
                    "name".to_string(),
                    name.map(|n| n.to_string()).unwrap_or_default(),
                )))
                .collect();

            let authn = {
                let suffixes = identities
                    .iter()
                    .filter_map(|i| match i {
                        Identity::Suffix(s) => Some(proto::Suffix {
                            parts: s.iter().cloned().collect(),
                        }),
                        _ => None,
                    })
                    .collect();

                let identities = identities
                    .iter()
                    .filter_map(|i| match i {
                        Identity::Name(n) => Some(proto::Identity {
                            name: n.to_string(),
                        }),
                        _ => None,
                    })
                    .chain(
                        service_accounts
                            .into_iter()
                            .map(|sa| to_identity(sa, identity_domain)),
                    )
                    .collect();

                proto::Authn {
                    permit: Some(proto::authn::Permit::ProxyIdentities(
                        proto::authn::PermitProxyIdentities {
                            identities,
                            suffixes,
                        },
                    )),
                }
            };

            proto::Authz {
                labels,
                networks,
                authentication: Some(authn),
            }
        }
    }
}

fn to_identity(ServiceAccountRef { ns, name }: ServiceAccountRef, domain: &str) -> proto::Identity {
    proto::Identity {
        name: format!("{}.{}.serviceaccount.linkerd.{}", ns, name, domain),
    }
}
