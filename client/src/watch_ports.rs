use crate::{Client, Inbound};
use anyhow::Result;
use futures::{future, prelude::*};
use std::collections::HashMap;
use tokio::sync::watch;

#[derive(Debug)]
pub struct PortWatch {
    pub rx: watch::Receiver<Inbound>,
    pub task: tokio::task::JoinHandle<Result<()>>,
}

pub async fn watch_ports(
    client: Client,
    workload: String,
    ports: Vec<u16>,
) -> Result<HashMap<u16, PortWatch>> {
    let futures = ports.into_iter().map(move |port| {
        watch_port(client.clone(), workload.clone(), port).map_ok(move |pw| (port, pw))
    });
    let watches = future::try_join_all(futures).await?;
    Ok(watches.into_iter().collect::<HashMap<_, _>>())
}

async fn watch_port(mut client: Client, workload: String, port: u16) -> Result<PortWatch> {
    let (inbound, mut updates) = start_watch(&mut client, workload.clone(), port).await?;
    let (tx, rx) = watch::channel(inbound);

    let task = tokio::spawn(async move {
        loop {
            let res = tokio::select! {
                _ = tx.closed() => {
                    return Ok(());
                }
                res = updates.try_next() => res,
            };

            match res {
                Ok(Some(inbound)) => {
                    let _ = tx.send(inbound);
                }

                Ok(None) => {
                    let (inbound, stream) = tokio::select! {
                        res = start_watch(&mut client, workload.clone(), port) => res?,
                        _ = tx.closed() => {
                            return Ok(());
                        }
                    };

                    let _ = tx.send(inbound);
                    updates = stream;
                }

                Err(error) => {
                    tracing::debug!(%error);
                    let (inbound, stream) = tokio::select! {
                        res = start_watch(&mut client, workload.clone(), port) => res?,
                        _ = tx.closed() => {
                            return Ok(());
                        }
                    };

                    let _ = tx.send(inbound);
                    updates = stream;
                }
            }
        }
    });

    return Ok(PortWatch { rx, task });
}

async fn start_watch(
    client: &mut Client,
    workload: String,
    port: u16,
) -> Result<(Inbound, impl Stream<Item = Result<Inbound>>)> {
    loop {
        match client.watch_inbound_port(workload.clone(), port).await {
            Ok(mut updates) => match updates.try_next().await {
                Ok(Some(inbound)) => {
                    return Ok((inbound, updates));
                }

                Ok(None) => {}

                Err(error) => {
                    tracing::debug!(%error);
                    // TODO surface unrecoverable errors?
                    // TODO backoff
                }
            },

            Err(error) => {
                tracing::debug!(%error);
                // TODO surface unrecoverable errors?
                // TODO backoff
            }
        }
    }
}
