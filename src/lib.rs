pub mod authz;
pub mod grpc;
mod index;
pub mod labels;
pub mod server;

pub use self::{grpc::Grpc, index::Index};

//type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
