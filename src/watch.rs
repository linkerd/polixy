use futures::prelude::*;
use kube::api::Resource;
use kube_runtime::watcher;
use serde::de::DeserializeOwned;
use std::{fmt, hash::Hash, pin::Pin};
use tracing::info;

pub(crate) struct Watch<T>(
    Pin<Box<dyn Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static>>,
);

// === impl Watch ===

impl<T, W> From<W> for Watch<T>
where
    W: Stream<Item = watcher::Result<watcher::Event<T>>> + Send + 'static,
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
    pub async fn recv(&mut self) -> watcher::Event<T> {
        loop {
            match self
                .0
                .next()
                .await
                .expect("watch stream must not terminate")
            {
                Ok(ev) => return ev,
                Err(error) => info!(%error, "Disconnected"),
            }
        }
    }
}
