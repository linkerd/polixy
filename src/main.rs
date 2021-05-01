use anyhow::{Context, Result};
use futures::{future, prelude::*};
use polixy::index;
use structopt::StructOpt;
use tracing::{info, instrument};

#[derive(Debug, StructOpt)]
#[structopt(name = "polixy", about = "A policy resource prototype")]
enum Command {
    Controller {
        #[structopt(short, long, default_value = "8910")]
        port: u16,
    },
    Client {
        #[structopt(long, default_value = "localhost.:8910")]
        server: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    match Command::from_args() {
        Command::Controller { port } => {
            let (drain_tx, drain_rx) = linkerd_drain::channel();

            let client = kube::Client::try_default()
                .await
                .context("failed to initialize kubernetes client")?;
            let (index, index_task) = index::run(client);

            let index_task = tokio::spawn(index_task);
            let grpc = tokio::spawn(grpc(port, index, drain_rx));

            tokio::select! {
                _ = shutdown(drain_tx) => Ok(()),
                res = grpc => match res {
                    Ok(res) => res.context("grpc server failed"),
                    Err(e) if e.is_cancelled() => Ok(()),
                    Err(e) => Err(e).context("grpc server panicked"),
                },
                res = index_task => match res {
                    Ok(e) => Err(e).context("indexer failed"),
                    Err(e) if e.is_cancelled() => Ok(()),
                    Err(e) => Err(e).context("indexer panicked"),
                },
            }
        }
        Command::Client { server: _ } => todo!("client"),
    }
}

#[instrument(skip(index, drain))]
async fn grpc(port: u16, index: index::Handle, drain: linkerd_drain::Watch) -> Result<()> {
    let addr = ([0, 0, 0, 0], port).into();
    let server = polixy::grpc::Server::new(index, drain.clone());
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    tokio::pin! {
        let srv = server.serve(addr, close_rx.map(|_| {}));
    }
    tokio::select! {
        res = (&mut srv) => res?,
        handle = drain.signaled() => {
            let _ = close_tx.send(());
            handle.release_after(srv).await?
        }
    }
    Ok(())
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
