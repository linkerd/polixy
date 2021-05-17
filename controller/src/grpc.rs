use crate::{
    k8s::{NsName, PodName},
    ClientAuthn, ClientAuthz, ClientNetwork, Identity, InboundServerConfig, KubeletIps, Lookup,
    LookupHandle, PodIps, ProxyProtocol, ServerRxRx, ServiceAccountRef,
};
use futures::prelude::*;
use polixy_grpc as proto;
use std::{collections::HashMap, iter::FromIterator, sync::Arc};
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
            .add_service(proto::polixy_server::PolixyServer::new(self))
            .serve_with_shutdown(addr, shutdown)
            .await
    }

    fn lookup(&self, workload: String, port: u32) -> Result<Lookup, tonic::Status> {
        // Parse a workload name in the form namespace:name.
        let (ns, name) = match workload.split_once(':') {
            None => {
                return Err(tonic::Status::invalid_argument(format!(
                    "Invalid workload: {}",
                    workload
                )));
            }
            Some((ns, pod)) if ns.is_empty() || pod.is_empty() => {
                return Err(tonic::Status::invalid_argument(format!(
                    "Invalid workload: {}",
                    workload
                )));
            }
            Some((ns, pod)) => (
                NsName::from_string(ns.to_string()),
                PodName::from(pod.to_string()),
            ),
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
        self.lookup
            .lookup(ns.clone(), name.clone(), port)
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "unknown pod ns={} name={} port={}",
                    ns, name, port
                ))
            })
    }
}

#[async_trait::async_trait]
impl proto::polixy_server::Polixy for Server {
    async fn get_inbound_port(
        &self,
        req: tonic::Request<proto::InboundPort>,
    ) -> Result<tonic::Response<proto::InboundServer>, tonic::Status> {
        let proto::InboundPort { workload, port } = req.into_inner();
        let Lookup {
            name: _,
            pod_ips,
            kubelet_ips,
            rx,
        } = self.lookup(workload, port)?;

        let kubelet = kubelet_authz(kubelet_ips);

        let server = to_server(
            &pod_ips,
            &kubelet,
            rx.borrow().borrow().clone(),
            self.identity_domain.as_ref(),
        );
        Ok(tonic::Response::new(server))
    }

    type WatchInboundPortStream = BoxWatchStream;

    async fn watch_inbound_port(
        &self,
        req: tonic::Request<proto::InboundPort>,
    ) -> Result<tonic::Response<BoxWatchStream>, tonic::Status> {
        let proto::InboundPort { workload, port } = req.into_inner();
        let Lookup {
            name: _,
            pod_ips,
            kubelet_ips,
            rx,
        } = self.lookup(workload, port)?;

        Ok(tonic::Response::new(response_stream(
            pod_ips,
            kubelet_ips,
            self.identity_domain.clone(),
            self.drain.clone(),
            rx,
        )))
    }
}

type BoxWatchStream = std::pin::Pin<
    Box<dyn Stream<Item = Result<proto::InboundServer, tonic::Status>> + Send + Sync>,
>;

fn response_stream(
    pod_ips: PodIps,
    kubelet_ips: KubeletIps,
    domain: Arc<str>,
    drain: linkerd_drain::Watch,
    mut port_rx: ServerRxRx,
) -> BoxWatchStream {
    let kubelet = kubelet_authz(kubelet_ips);

    Box::pin(async_stream::try_stream! {
        tokio::pin! {
            let shutdown = drain.signaled();
        }

        let mut server_rx = port_rx.borrow().clone();
        let mut prior = None;
        loop {
            let server = server_rx.borrow().clone();

            // Deduplicate identical updates (i.e., especially after the controller reconnects to
            // the k8s API).
            if prior.as_ref() != Some(&server) {
                prior = Some(server.clone());
                yield to_server(&pod_ips, &kubelet, server, domain.as_ref());
            }

            tokio::select! {
                // When the port is updated with a new server, update the server watch.
                res = port_rx.changed() => {
                    // If the port watch closed, end the stream.
                    if res.is_err() {
                        return;
                    }
                    // Otherwise, update the server watch.
                    server_rx = port_rx.borrow().clone();
                }

                // Wait for the current server watch to update.
                res = server_rx.changed() => {
                    // If the server was deleted (the server watch closes), get an updated server
                    // watch.
                    if res.is_err() {
                        server_rx = port_rx.borrow().clone();
                    }
                }

                // If the server starts shutting down, close the stream so that it doesn't hold the
                // server open.
                _ = (&mut shutdown) => {
                    return;
                }
            }
        }
    })
}

fn to_server(
    pod_ips: &PodIps,
    kubelet_authz: &proto::Authz,
    srv: InboundServerConfig,
    identity_domain: &str,
) -> proto::InboundServer {
    // Convert the protocol object into a protobuf response.
    let protocol = proto::ProxyProtocol {
        kind: match srv.protocol {
            ProxyProtocol::Detect { timeout } => Some(proto::proxy_protocol::Kind::Detect(
                proto::proxy_protocol::Detect {
                    timeout: Some(timeout.into()),
                },
            )),
            ProxyProtocol::Http1 => Some(proto::proxy_protocol::Kind::Http1(
                proto::proxy_protocol::Http1::default(),
            )),
            ProxyProtocol::Http2 => Some(proto::proxy_protocol::Kind::Http2(
                proto::proxy_protocol::Http2::default(),
            )),
            ProxyProtocol::Grpc => Some(proto::proxy_protocol::Kind::Grpc(
                proto::proxy_protocol::Grpc::default(),
            )),
            ProxyProtocol::Opaque => Some(proto::proxy_protocol::Kind::Opaque(
                proto::proxy_protocol::Opaque {},
            )),
            ProxyProtocol::Tls => Some(proto::proxy_protocol::Kind::Tls(
                proto::proxy_protocol::Tls {},
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

    proto::InboundServer {
        protocol: Some(protocol),
        server_ips: pod_ips.iter().copied().map(Into::into).collect(),
        authorizations: Some(kubelet_authz.clone())
            .into_iter()
            .chain(server_authzs)
            .collect(),
        ..Default::default()
    }
}

fn kubelet_authz(ips: KubeletIps) -> proto::Authz {
    // Traffic is always permitted from the pod's Kubelet IPs.
    proto::Authz {
        networks: ips
            .to_nets()
            .into_iter()
            .map(|net| proto::Network {
                net: Some(net.into()),
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
    }
}

fn to_authz(
    name: impl ToString,
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
                net: Some(ipnet::IpNet::V4(Default::default()).into()),
                except: vec![],
            },
            proto::Network {
                net: Some(ipnet::IpNet::V6(Default::default()).into()),
                except: vec![],
            },
        ]
    } else {
        networks
            .iter()
            .map(|ClientNetwork { net, except }| proto::Network {
                net: Some(net.clone().into()),
                except: except.iter().cloned().map(Into::into).collect(),
            })
            .collect()
    };

    match authentication {
        ClientAuthn::Unauthenticated => proto::Authz {
            networks,
            authentication: Some(proto::Authn {
                permit: Some(proto::authn::Permit::Unauthenticated(
                    proto::authn::PermitUnauthenticated {},
                )),
            }),
            labels: HashMap::from_iter(Some(("authn".to_string(), "false".to_string()))),
        },

        // Authenticated connections must have TLS and apply to all
        // networks.
        ClientAuthn::Authenticated {
            service_accounts,
            identities,
        } => {
            let labels = Some(("authn".to_string(), "true".to_string()))
                .into_iter()
                .chain(Some(("name".to_string(), name.to_string())))
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
