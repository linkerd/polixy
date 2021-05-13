use super::{Index, NsIndex, Pod, PodPort, PodPorts, PortNames};
use crate::{
    k8s::{self, polixy},
    FromResource, Lookup, PodIps,
};
use anyhow::{anyhow, bail, Result};
use dashmap::mapref::entry::Entry as DashEntry;
use parking_lot::Mutex;
use std::{
    collections::{hash_map::Entry as HashEntry, HashMap},
    net::IpAddr,
    sync::Arc,
};
use tokio::sync::watch;
use tracing::{debug, instrument, trace};

impl Index {
    #[instrument(
        skip(self, pod),
        fields(
            ns = ?pod.metadata.namespace,
            name = ?pod.metadata.name,
        )
    )]
    pub(super) fn apply_pod(&mut self, pod: k8s::Pod) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&pod);
        let pod_name = k8s::PodName::from_resource(&pod);
        let spec = pod.spec.ok_or_else(|| anyhow!("pod missing spec"))?;
        let status = pod.status.ok_or_else(|| anyhow!("pod missing status"))?;
        let labels = k8s::Labels::from(pod.metadata.labels);

        let NsIndex {
            default_mode,
            ref mut pods,
            ref mut servers,
            ..
        } = self.namespaces.get_or_default(ns_name.clone());

        let lookups_entry = self.lookups.entry((ns_name, pod_name.clone()));

        match (pods.index.entry(pod_name), lookups_entry) {
            (HashEntry::Vacant(pod_entry), DashEntry::Vacant(lookups_entry)) => {
                let kubelet = {
                    let name = spec
                        .node_name
                        .map(k8s::NodeName::from)
                        .ok_or_else(|| anyhow!("pod missing node name"))?;
                    self.node_ips
                        .get(&name)
                        .ok_or_else(|| anyhow!("node IP does not exist for node {}", name))?
                        .clone()
                };

                let pod_ips = {
                    let ips = if let Some(ips) = status.pod_ips {
                        ips.iter()
                            .flat_map(|ip| ip.ip.as_ref())
                            .map(|ip| ip.parse().map_err(Into::into))
                            .collect::<Result<Vec<IpAddr>>>()?
                    } else {
                        status
                            .pod_ip
                            .iter()
                            .map(|ip| ip.parse::<IpAddr>().map_err(Into::into))
                            .collect::<Result<Vec<IpAddr>>>()?
                    };
                    if ips.is_empty() {
                        bail!("pod missing IP addresses");
                    };
                    PodIps(ips.into())
                };

                let mut ports = PodPorts::default();
                let mut ports_by_name = PortNames::default();
                let mut lookups = HashMap::new();
                for container in spec.containers.into_iter() {
                    if let Some(ps) = container.ports {
                        for p in ps.into_iter() {
                            if p.protocol.map(|p| p == "TCP").unwrap_or(true) {
                                let port = p.container_port as u16;
                                if ports.contains_key(&port) {
                                    debug!(port, "Port duplicated");
                                    continue;
                                }

                                let server_rx = self.default_mode_rxs.get(*default_mode);
                                let (tx, rx) = watch::channel(server_rx);
                                let pod_port = Arc::new(PodPort {
                                    tx,
                                    server_name: Mutex::new(None),
                                });

                                let name = p.name.map(k8s::polixy::server::PortName::from);
                                if let Some(name) = name.clone() {
                                    match ports_by_name.entry(name).or_default().entry(port) {
                                        HashEntry::Vacant(entry) => {
                                            entry.insert(pod_port.clone());
                                        }
                                        HashEntry::Occupied(_) => {
                                            unreachable!("port numbers must not be duplicated");
                                        }
                                    }
                                }

                                trace!(%port, ?name, "Adding port");
                                ports.insert(port, pod_port);
                                lookups.insert(
                                    port,
                                    Lookup {
                                        rx,
                                        name,
                                        pod_ips: pod_ips.clone(),
                                        kubelet_ips: kubelet.clone(),
                                    },
                                );
                            }
                        }
                    }
                }

                for (srv_name, server) in servers.index.iter() {
                    if server.meta.pod_selector.matches(&labels) {
                        for port in
                            Self::get_ports(&server.meta.port, &ports_by_name, &ports).into_iter()
                        {
                            // TODO handle conflicts
                            *port.server_name.lock() = Some(srv_name.clone());
                            port.tx
                                .send(server.rx.clone())
                                .expect("pod config receiver must be set");
                            debug!(server = %srv_name, "Pod server udpated");
                            trace!(selector = ?server.meta.pod_selector, ?labels);
                        }
                    } else {
                        trace!(
                            server = %srv_name,
                            selector = ?server.meta.pod_selector,
                            ?labels,
                            "Does not match",
                        );
                    }
                }

                lookups_entry.insert(Arc::new(lookups));
                pod_entry.insert(Pod {
                    ports_by_name: ports_by_name.into(),
                    ports: ports.into(),
                    labels,
                });
            }

            (HashEntry::Occupied(mut pod_entry), DashEntry::Occupied(_)) => {
                if pod_entry.get().labels != labels {
                    for (srv_name, server) in servers.index.iter() {
                        let pod = pod_entry.get();
                        if server.meta.pod_selector.matches(&labels) {
                            for port in
                                Self::get_ports(&server.meta.port, &pod.ports_by_name, &pod.ports)
                            {
                                // TODO handle conflicts
                                *port.server_name.lock() = Some(srv_name.clone());
                                port.tx
                                    .send(server.rx.clone())
                                    .expect("pod config receiver must be set");
                                debug!(server = %srv_name, "Pod server udpated");
                                trace!(selector = ?server.meta.pod_selector, ?labels);
                            }
                        } else {
                            trace!(
                                server = %srv_name,
                                selector = ?server.meta.pod_selector,
                                pod = %pod_entry.key(),
                                ?labels,
                                "Does not match",
                            );
                        }
                    }

                    pod_entry.get_mut().labels = labels;
                }
            }

            _ => unreachable!("pod label and server indexes must be consistent"),
        }

        Ok(())
    }

    pub(super) fn get_ports(
        port_match: &polixy::server::Port,
        ports_by_name: &PortNames,
        ports: &PodPorts,
    ) -> Vec<Arc<PodPort>> {
        match port_match {
            polixy::server::Port::Number(ref port) => {
                ports.get(port).into_iter().cloned().collect::<Vec<_>>()
            }
            polixy::server::Port::Name(ref name) => ports_by_name
                .get(name)
                .into_iter()
                .flat_map(|p| p.values().cloned())
                .collect::<Vec<_>>(),
        }
    }

    #[instrument(
        skip(self, pod),
        fields(
            ns = ?pod.metadata.namespace,
            name = ?pod.metadata.name,
        )
    )]
    pub(super) fn delete_pod(&mut self, pod: k8s::Pod) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&pod);
        let pod_name = k8s::PodName::from_resource(&pod);
        self.rm_pod(&ns_name, &pod_name)
    }

    fn rm_pod(&mut self, ns: &k8s::NsName, pod: &k8s::PodName) -> Result<()> {
        self.namespaces
            .index
            .get_mut(ns)
            .ok_or_else(|| anyhow!("namespace {} doesn't exist", ns))?
            .pods
            .index
            .remove(pod)
            .ok_or_else(|| anyhow!("pod {} doesn't exist", pod))?;

        self.lookups
            .remove(&(ns.clone(), pod.clone()))
            .ok_or_else(|| anyhow!("pod {} doesn't exist in namespace {}", pod, ns))?;

        debug!("Removed pod");

        Ok(())
    }

    #[instrument(skip(self, pods))]
    pub(super) fn reset_pods(&mut self, pods: Vec<k8s::Pod>) -> Result<()> {
        let mut prior_pod_labels = self
            .namespaces
            .index
            .iter()
            .map(|(n, ns)| {
                let pods = ns
                    .pods
                    .index
                    .iter()
                    .map(|(n, p)| (n.clone(), p.labels.clone()))
                    .collect::<HashMap<_, _>>();
                (n.clone(), pods)
            })
            .collect::<HashMap<_, _>>();

        let mut result = Ok(());
        for pod in pods.into_iter() {
            let ns_name = k8s::NsName::from_resource(&pod);
            let pod_name = k8s::PodName::from_resource(&pod);

            if let Some(prior) = prior_pod_labels.get_mut(&ns_name) {
                if let Some(prior_labels) = prior.remove(&pod_name) {
                    let labels = k8s::Labels::from(pod.metadata.labels.clone());
                    if prior_labels == labels {
                        continue;
                    }
                }
            }

            if let Err(error) = self.apply_pod(pod) {
                result = Err(error);
            }
        }

        for (ns, pods) in prior_pod_labels.into_iter() {
            for (pod, _) in pods.into_iter() {
                if let Err(error) = self.rm_pod(&ns, &pod) {
                    result = Err(error);
                }
            }
        }

        result
    }
}
