use anyhow::Result;
use futures::prelude::*;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "polixy", about = "A policy resource prototype")]
struct Command {
    #[structopt(long, default_value = "http://127.0.0.1:8910")]
    server: String,
    #[structopt(subcommand)]
    command: ClientCommand,
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

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let Command { server, command } = Command::from_args();

    let mut client = polixy_client::Client::connect(server).await?;

    match command {
        ClientCommand::Watch {
            namespace,
            pod,
            port,
        } => {
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
            let server = client.get_inbound_port(namespace, pod, port).await?;
            println!("{:#?}", server);
            Ok(())
        }
    }
}

// TODO we should more fully simulate the proxy's behavior.
/*
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
