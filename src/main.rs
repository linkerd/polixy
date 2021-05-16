use anyhow::{Context, Result};
use futures::{future, prelude::*};
use polixy::DefaultAllow;
use structopt::StructOpt;
use tokio::time;
use tracing::{debug, info, instrument};

#[derive(Debug, StructOpt)]
#[structopt(name = "polixy", about = "A policy resource prototype")]
enum Command {
    Controller {
        #[structopt(short, long, default_value = "8910")]
        port: u16,
        #[structopt(long, default_value = "cluster.local")]
        identity_domain: String,

        /// Network CIDRs of pod IPs
        #[structopt(long, default_value = "10.42.0.0/16")]
        cluster_networks: Vec<ipnet::IpNet>,

        #[structopt(long, default_value = "allow-external")]
        default_mode: DefaultAllow,
    },
    Client {
        #[structopt(long, default_value = "http://127.0.0.1:8910")]
        server: String,
        #[structopt(subcommand)]
        command: ClientCommand,
    },
}

#[derive(Debug, StructOpt)]
enum ClientCommand {
    Watch {
        #[structopt(short, long, default_value = "default")]
        namespace: String,
        pod: String,
        port: u16,
    },
    Get {
        #[structopt(short, long, default_value = "default")]
        namespace: String,
        pod: String,
        port: u16,
    },
    /*
    ProxySim {
        #[structopt(short, long, default_value = "default")]
        namespace: String,
        pod: String,
        ports: Vec<u16>,
    },
    */
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    match Command::from_args() {
        Command::Controller {
            port,
            identity_domain,
            cluster_networks,
            default_mode,
        } => {
            let (drain_tx, drain_rx) = linkerd_drain::channel();

            let client = kube::Client::try_default()
                .await
                .context("failed to initialize kubernetes client")?;

            const DETECT_TIMEOUT: time::Duration = time::Duration::from_secs(10);
            let (handle, index_task) =
                polixy::LookupHandle::run(client, cluster_networks, default_mode, DETECT_TIMEOUT);
            let index_task = tokio::spawn(index_task);

            let grpc = tokio::spawn(grpc(port, handle, drain_rx, identity_domain));

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

        Command::Client { server, command } => match command {
            ClientCommand::Watch {
                namespace,
                pod,
                port,
            } => {
                let mut client = polixy::grpc::Client::connect(server).await?;
                let mut updates = client.watch_inbound_port(namespace, pod, port).await?;
                while let Some(res) = updates.next().await {
                    match res {
                        Ok(config) => println!("{:#?}", config),
                        Err(error) => eprintln!("Update failed: {}", error),
                    }
                }
                eprintln!("Stream closed");
                Ok(())
            }

            ClientCommand::Get {
                namespace,
                pod,
                port,
            } => {
                let mut client = polixy::grpc::Client::connect(server).await?;
                let server = client.get_inbound_port(namespace, pod, port).await?;
                println!("{:#?}", server);
                Ok(())
            } /*
              ClientCommand::ProxySim {
                  namespace,
                  pod,
                  ports,
              } => {
                  let client = polixy::grpc::Client::connect(server).await?;

                  let _watches = ports
                      .into_iter()
                      .collect::<indexmap::IndexSet<_>>()
                      .into_iter()
                      .map(|port| {
                          let (tx, rx) = watch::channel(None);
                          let mut client = client.clone();
                          let namespace = namespace.clone();
                          let pod = pod.clone();
                          let task = tokio::spawn(async move {
                              loop {
                                  match client
                                      .watch_inbound_port(namespace.clone(), pod.clone(), port)
                                      .await
                                  {
                                      Ok(mut updates) => {
                                          while let Some(res) = updates.next().await {
                                              match res {
                                                  Ok(config) => {
                                                      if tx.send(Some(config)).is_err() {
                                                          return;
                                                      }
                                                  }
                                                  Err(error) => eprintln!("Update failed: {}", error),
                                              }
                                          }
                                          info!("Stream closed (port={})", port);
                                      }
                                      Err(error) => {
                                          eprintln!("Lookup failed: {}", error);
                                      }
                                  }

                                  time::sleep(time::Duration::from_secs(5)).await;
                                  debug!("Reconnecting");
                              }
                          });
                          (port, (rx, task))
                      })
                      .collect::<indexmap::IndexMap<_, _>>();

                  todo!("simulate proxy behavior")
              }*/
        },
    }
}

#[instrument(skip(handle, drain, identity_domain))]
async fn grpc(
    port: u16,
    handle: polixy::LookupHandle,
    drain: linkerd_drain::Watch,
    identity_domain: String,
) -> Result<()> {
    let addr = ([0, 0, 0, 0], port).into();
    let server = polixy::grpc::Server::new(handle, drain.clone(), identity_domain);
    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    tokio::pin! {
        let srv = server.serve(addr, close_rx.map(|_| {}));
    }
    info!(%addr, "gRPC server listening");
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
        _ = tokio::signal::ctrl_c() => {
            debug!("Received ctrl-c");
        },
        _ = sigterm() => {
            debug!("Received SIGTERM");
        }
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
