use crate::index::{Handle, KubeletIps, Lookup, NsName, PodName, ServerConfig};
use futures::prelude::*;
use tokio_stream::wrappers::WatchStream;

pub mod proto {
    tonic::include_proto!("polixy.olix0r.net");

    pub use self::proxy_config_service_server::{
        ProxyConfigService as Service, ProxyConfigServiceServer as Server,
    };
}

#[derive(Clone, Debug)]
pub struct Grpc {
    index: Handle,
    drain: linkerd_drain::Watch,
}

impl Grpc {
    pub fn new(index: Handle, drain: linkerd_drain::Watch) -> Self {
        Self { index, drain }
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
impl proto::Service for Grpc {
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

        let watch = WatchStream::new(server)
            .map(WatchStream::new)
            .flat_map(move |updates| {
                let ips = kubelet_ips.clone();
                updates.map(move |c| to_config(&ips, c)).map(Ok)
            });

        Ok(tonic::Response::new(Box::pin(watch)))
    }
}

fn to_config(_k: &KubeletIps, _s: ServerConfig) -> proto::InboundProxyConfig {
    todo!("Convert server to config")
}
