pub mod grpc;
pub mod index;
mod k8s;
mod v1;
mod watch;

pub use self::grpc::Grpc;

trait FromResource<T> {
    fn from_resource(resource: &T) -> Self;
}
