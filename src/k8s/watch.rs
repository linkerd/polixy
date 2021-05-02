use futures::prelude::*;
use kube::api::Resource;
use serde::de::DeserializeOwned;
use std::{fmt, hash::Hash, pin::Pin};
use tokio::time;
use tracing::info;

pub use kube_runtime::watcher::{Event, Result};

pub struct Watch<T>(Pin<Box<dyn Stream<Item = Result<Event<T>>> + Send + 'static>>);

// === impl Watch ===

impl<T, W> From<W> for Watch<T>
where
    W: Stream<Item = Result<Event<T>>> + Send + 'static,
{
    fn from(watch: W) -> Self {
        Watch(watch.boxed())
    }
}

impl<T> Watch<T>
where
    T: Resource + Clone + DeserializeOwned + fmt::Debug + Send + Sync + 'static,
    T::DynamicType: Clone + Eq + Hash + Default,
{
    pub async fn recv(&mut self) -> Event<T> {
        loop {
            match self
                .0
                .next()
                .await
                .expect("watch stream must not terminate")
            {
                Ok(ev) => return ev,
                Err(error) => {
                    info!(%error, "Disconnected");
                    time::sleep(time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}
