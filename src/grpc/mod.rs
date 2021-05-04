mod client;
mod server;

pub use self::client::Client;
pub use self::server::Server;

pub mod proto {
    pub mod io {
        pub mod linkerd {
            pub mod proxy {
                pub mod net {
                    pub use linkerd2_proxy_api::net::*;
                }
            }
        }
    }

    pub mod net {
        pub mod olix0r {
            pub mod polixy {
                tonic::include_proto!("net.olix0r.polixy");
            }
        }
    }

    pub use self::net::olix0r::polixy::*;
    pub use self::net::olix0r::polixy::*;
}
