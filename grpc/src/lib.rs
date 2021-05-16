pub mod io {
    pub mod linkerd {
        pub mod polixy {
            tonic::include_proto!("io.linkerd.polixy");
        }

        pub mod proxy {
            pub mod net {
                pub use linkerd2_proxy_api::net::*;
            }
        }
    }
}

pub use self::io::linkerd::polixy::*;
