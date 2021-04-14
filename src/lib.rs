pub mod authz;
pub mod grpc;
pub mod index;
pub mod labels;
pub mod server;

pub use self::grpc::Grpc;

//type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
