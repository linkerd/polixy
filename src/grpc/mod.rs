mod client;
mod server;

pub use self::client::Client;
pub use self::server::Server;

pub mod proto {
    tonic::include_proto!("polixy.olix0r.net");

    pub use self::{polixy_client as client, polixy_server as server};
}
