use futures::{future, prelude::*};
use polixy::index;
use tracing::{error, info, instrument};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (drain_tx, drain_rx) = linkerd_drain::channel();

    let client = kube::Client::try_default()
        .await
        .expect("Failed to initialize client");
    let (index, index_task) = index::run(client);

    let index_task = tokio::spawn(index_task);
    let grpc = tokio::spawn(grpc(8090, index, drain_rx));

    tokio::select! {
        _ = shutdown(drain_tx) => {}
        res = grpc => match res {
            Ok(()) => {}
            Err(e) => {
                if !e.is_cancelled() {
                    error!("Server panicked");
                }
            }
        },
        res = index_task => match res {
            Ok(error) => error!(%error, "Indexer failed"),
            Err(e) => {
                if !e.is_cancelled() {
                    error!("Indexer panicked")
                }
            }
        },
    }
}

#[instrument(skip(index, drain))]
async fn grpc(port: u16, index: index::Handle, drain: linkerd_drain::Watch) {
    let addr = ([0, 0, 0, 0], port).into();
    let server = polixy::Grpc::new(index, drain.clone());
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    tokio::pin! {
        let srv = server.serve(addr, close_rx.map(|_| {}));
    }
    let res = tokio::select! {
        res = (&mut srv) => res,
        handle = drain.signaled() => {
            let _ = close_tx.send(());
            handle.release_after(srv).await
        }
    };
    if let Err(error) = res {
        error!(%error, "Grpc server failed");
    }
}

async fn shutdown(drain: linkerd_drain::Signal) {
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = sigterm() => {}
    }
    info!("Shutting down");
    drain.drain().await;
}

async fn sigterm() {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::terminate()) {
        Ok(mut term) => term.recv().await,
        _ => future::pending().await,
    };
}
