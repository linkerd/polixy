mod client;
mod server;

pub use self::client::Client;
pub use self::server::Server;

pub mod proto {
    tonic::include_proto!("polixy.olix0r.net");

    pub use self::{
        proxy_config_service_client::ProxyConfigServiceClient as Client,
        proxy_config_service_server::{
            ProxyConfigService as Service, ProxyConfigServiceServer as Server,
        },
    };
}
