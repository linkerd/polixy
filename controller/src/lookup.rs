use crate::{KubeletIps, PodIps, ServerRxRx};
use anyhow::{anyhow, Result};
use dashmap::{mapref::entry::Entry, DashMap};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Default)]
pub(crate) struct Index(Map);

#[derive(Clone, Debug)]
pub struct Reader(Map);

type Map = Arc<DashMap<String, DashMap<String, PodPorts>>>;

pub type PodPorts = Box<HashMap<u16, PodPort>>;

#[derive(Clone, Debug)]
pub struct PodPort {
    pub kubelet_ips: KubeletIps,
    pub pod_ips: PodIps,
    pub rx: ServerRxRx,
}

// === impl Reader ===

impl Reader {
    pub fn lookup(&self, ns: &str, name: &str, port: u16) -> Option<PodPort> {
        self.0.get(ns)?.get(name)?.get(&port).cloned()
    }
}

// === impl Index ===

impl Index {
    pub(crate) fn pair() -> (Self, Reader) {
        let map = Map::default();
        let reader = Reader(map.clone());
        (Self(map), reader)
    }

    pub(crate) fn contains(&self, ns: impl AsRef<str>, pod: impl AsRef<str>) -> bool {
        self.0
            .get(ns.as_ref())
            .map(|ns| ns.contains_key(pod.as_ref()))
            .unwrap_or(false)
    }

    pub(crate) fn set(
        &mut self,
        ns: impl ToString,
        pod: impl ToString,
        ports: impl Into<PodPorts>,
    ) -> Result<()> {
        match self
            .0
            .entry(ns.to_string())
            .or_default()
            .entry(pod.to_string())
        {
            Entry::Vacant(entry) => {
                entry.insert(ports.into());
                Ok(())
            }
            Entry::Occupied(_) => Err(anyhow!(
                "pod {} already exists in namespace {}",
                pod.to_string(),
                ns.to_string()
            )),
        }
    }

    pub(crate) fn unset(&mut self, ns: impl AsRef<str>, pod: impl AsRef<str>) -> Result<PodPorts> {
        let pods = self
            .0
            .get_mut(ns.as_ref())
            .ok_or_else(|| anyhow!("missing namespace {}", ns.as_ref()))?;

        let (_, ports) = pods
            .remove(pod.as_ref())
            .ok_or_else(|| anyhow!("missing pod {} in namespace {}", pod.as_ref(), ns.as_ref()))?;

        if (*pods).is_empty() {
            drop(pods);
            self.0.remove(ns.as_ref()).expect("namespace must exist");
        }

        Ok(ports)
    }
}
