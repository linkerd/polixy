use anyhow::Result;
use futures::prelude::*;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use structopt::StructOpt;
use tracing::info;

#[derive(Debug, StructOpt)]
#[structopt(name = "polixy", about = "A policy resource prototype")]
struct Args {
    #[structopt(long, default_value = "http://127.0.0.1:8910")]
    grpc_addr: String,

    #[structopt(subcommand)]
    command: Command,
}

#[derive(Debug, StructOpt)]
enum Command {
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
    HttpApi {
        #[structopt(long, default_value = "127.0.0.1:0")]
        listen_addr: SocketAddr,

        #[structopt(short, long, default_value = "default")]
        namespace: String,

        pod: String,

        ports: Vec<u16>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let Args { grpc_addr, command } = Args::from_args();

    let mut client = polixy_client::Client::connect(grpc_addr).await?;

    match command {
        Command::Watch {
            namespace,
            pod,
            port,
        } => {
            let workload = format!("{}:{}", namespace, pod);
            let mut updates = client.watch_inbound_port(workload, port).await?;
            while let Some(res) = updates.next().await {
                match res {
                    Ok(config) => println!("{:#?}", config),
                    Err(error) => eprintln!("Update failed: {}", error),
                }
            }
            eprintln!("Stream closed");
            Ok(())
        }

        Command::Get {
            namespace,
            pod,
            port,
        } => {
            let workload = format!("{}:{}", namespace, pod);
            let server = client.get_inbound_port(workload, port).await?;
            println!("{:#?}", server);
            Ok(())
        }

        Command::HttpApi {
            listen_addr,
            namespace,
            pod,
            ports,
        } => {
            let workload = format!("{}:{}", namespace, pod);

            let watches = polixy_client::watch_ports(client, workload, ports)
                .await
                .expect("Failed to watch ports");

            let ports = Arc::new(
                watches
                    .iter()
                    .map(|(p, w)| (*p, w.rx.clone()))
                    .collect::<HashMap<_, _>>(),
            );

            let server = hyper::server::Server::bind(&listen_addr).serve(
                hyper::service::make_service_fn(move |_conn| {
                    let ports = ports.clone();
                    future::ok::<_, hyper::Error>(hyper::service::service_fn(
                        move |req: hyper::Request<hyper::Body>| {
                            let ports = ports.clone();
                            async move { polixy_client::http_api::serve(ports.as_ref(), req).await }
                        },
                    ))
                }),
            );
            let addr = server.local_addr();
            info!(%addr, "Listening");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = server => {}
            }
            Ok(())
        }
    }
}
