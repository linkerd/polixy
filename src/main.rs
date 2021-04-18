use futures::{future, prelude::*};
use polixy::Index;
use tracing::{debug, error, info, info_span, Instrument};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (drain_tx, drain_rx) = linkerd_drain::channel();

    let client = kube::Client::try_default()
        .await
        .expect("Failed to initialize client");
    let index = Index::new();
    let index_task = index.clone().run(client);

    let addr = ([0, 0, 0, 0], 8090).into();
    let server = polixy::Grpc::new(index, drain_rx.clone());
    let grpc = tokio::spawn(
        async move {
            let (close_tx, close_rx) = tokio::sync::oneshot::channel();
            tokio::pin! {
                let srv = server.serve(addr, close_rx.map(|_| {}));
            }
            let res = tokio::select! {
                res = (&mut srv) => res,
                handle = drain_rx.signaled() => {
                    let _ = close_tx.send(());
                    handle.release_after(srv).await
                }
            };
            match res {
                Ok(()) => debug!("shutdown"),
                Err(error) => error!(%error),
            }
        }
        .instrument(info_span!("grpc")),
    );

    let sigterm = async move {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => term.recv().await,
            _ => future::pending().await,
        }
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = sigterm => {}
        _ = grpc => {
            error!("gRPC server terminated");
        }
        error = index_task => {
            error!(%error, "indexer terminated");
        }
    };
    info!("Shutting down");
    drain_tx.drain().await;
}
