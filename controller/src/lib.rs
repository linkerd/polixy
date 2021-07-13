#![deny(warnings, rust_2018_idioms)]
#![forbid(unsafe_code)]

pub mod admin;

pub use polixy_controller_grpc as grpc;
pub use polixy_controller_k8s_index as k8s;
