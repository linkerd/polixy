use crate::{KubeletIps, PodIps, ServerRxRx};
use anyhow::{anyhow, Result};
use dashmap::{mapref::entry::Entry, DashMap};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Default)]
pub(crate) struct Writer(ByNs);

#[derive(Clone, Debug)]
pub struct Reader(ByNs);

type ByNs = Arc<DashMap<String, ByPod>>;
type ByPod = DashMap<String, ByPort>;

// Boxed to enforce immutability.
type ByPort = Box<HashMap<u16, PodPort>>;

#[derive(Clone, Debug)]
pub struct PodPort {
    pub kubelet_ips: KubeletIps,
    pub pod_ips: PodIps,
    pub rx: ServerRxRx,
}

pub(crate) fn pair() -> (Writer, Reader) {
    let by_ns = ByNs::default();
    let w = Writer(by_ns.clone());
    let r = Reader(by_ns);
    (w, r)
}

// === impl Reader ===

impl Reader {
    pub fn lookup(&self, ns: &str, name: &str, port: u16) -> Option<PodPort> {
        self.0.get(ns)?.get(name)?.get(&port).cloned()
    }
}

// === impl Writer ===

impl Writer {
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
        ports: impl IntoIterator<Item = (u16, PodPort)>,
    ) -> Result<()> {
        match self
            .0
            .entry(ns.to_string())
            .or_default()
            .entry(pod.to_string())
        {
            Entry::Vacant(entry) => {
                entry.insert(ports.into_iter().collect::<HashMap<_, _>>().into());
                Ok(())
            }
            Entry::Occupied(_) => Err(anyhow!(
                "pod {} already exists in namespace {}",
                pod.to_string(),
                ns.to_string()
            )),
        }
    }

    pub(crate) fn unset(&mut self, ns: impl AsRef<str>, pod: impl AsRef<str>) -> Result<ByPort> {
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
