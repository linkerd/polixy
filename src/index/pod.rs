use super::{Index, NsIndex, Pod, PodServer, PodServers, ServerRx, SrvIndex};
use crate::{
    k8s::{self, polixy},
    DefaultMode, FromResource, KubeletIps, Lookup, PodIps,
};
use anyhow::{anyhow, bail, Result};
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
        let NsIndex {
            default_mode,
            ref mut pods,
            ref mut servers,
            ..
        } = self.namespaces.get_or_default(ns_name.clone());

        let pod_name = k8s::PodName::from_resource(&pod);
        let spec = pod.spec.ok_or_else(|| anyhow!("pod missing spec"))?;
        let status = pod.status.ok_or_else(|| anyhow!("pod missing status"))?;
        let default_mode = match DefaultMode::from_annotation(&pod.metadata) {
            Ok(Some(mode)) => mode,
            Ok(None) => *default_mode,
            Err(error) => {
                tracing::warn!(%error, "Ignoring invalid default-mode annotation");
                *default_mode
            }
        };
        let labels = k8s::Labels::from(pod.metadata.labels);

        let lookup_key = (ns_name, pod_name.clone());
        match pods.index.entry(pod_name) {
            HashEntry::Vacant(pod_entry) => {
                let pod_ips = mk_pod_ips(status)?;
                let kubelet_ips = mk_kubelet_ips(&spec, &self.node_ips)?;

                let default_server = self.default_mode_rxs.get(default_mode);
                let (pod_servers, lookups) =
                    collect_pod_servers(spec, default_server, pod_ips, kubelet_ips);

                Self::link_pod_servers(&servers, &labels, &pod_servers);

                if self.lookups.insert(lookup_key, lookups).is_some() {
                    unreachable!("pod must not exist in lookups");
                }

                pod_entry.insert(Pod {
                    servers: pod_servers,
                    labels,
                });
            }

            HashEntry::Occupied(mut pod_entry) => {
                debug_assert!(
                    self.lookups.contains_key(&lookup_key),
                    "pod must exist in lookups"
                );

                let p = pod_entry.get_mut();
                if p.labels != labels {
                    Self::link_pod_servers(&servers, &labels, &p.servers);
                    p.labels = labels;
                }

                // TODO support default-mode updates at runtime.
            }
        }

        Ok(())
    }

    pub(super) fn link_pod_servers(
        servers: &SrvIndex,
        pod_labels: &k8s::Labels,
        ports: &PodServers,
    ) {
        for (srv_name, server) in servers.index.iter() {
            if server.meta.pod_selector.matches(&pod_labels) {
                for port in get_ports(&server.meta.port, ports).into_iter() {
                    // TODO handle conflicts
                    let mut sn = port.server_name.lock();
                    if let Some(sn) = sn.as_ref() {
                        debug_assert!(
                            sn == srv_name,
                            "pod port matches multiple servers: {} and {}",
                            sn,
                            srv_name
                        );
                    }
                    *sn = Some(srv_name.clone());

                    port.tx
                        .send(server.rx.clone())
                        .expect("pod config receiver must be set");
                    debug!(server = %srv_name, "Pod server udpated");
                    trace!(selector = ?server.meta.pod_selector, ?pod_labels);
                }
            } else {
                trace!(
                    server = %srv_name,
                    selector = ?server.meta.pod_selector,
                    ?pod_labels,
                    "Does not match",
                );
            }
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

fn collect_pod_servers(
    spec: k8s::PodSpec,
    server_rx: ServerRx,
    pod_ips: PodIps,
    kubelet_ips: KubeletIps,
) -> (Arc<PodServers>, Arc<HashMap<u16, Lookup>>) {
    let mut pod_servers = PodServers::default();
    let mut lookups = HashMap::new();

    for container in spec.containers.into_iter() {
        if let Some(ps) = container.ports {
            for p in ps.into_iter() {
                if p.protocol.map(|p| p == "TCP").unwrap_or(true) {
                    let port = p.container_port as u16;
                    if pod_servers.by_port.contains_key(&port) {
                        debug!(port, "Port duplicated");
                        continue;
                    }

                    let (tx, rx) = watch::channel(server_rx.clone());
                    let pod_port = Arc::new(PodServer {
                        tx,
                        server_name: Mutex::new(None),
                    });

                    let name = p.name.map(k8s::polixy::server::PortName::from);
                    if let Some(name) = name.clone() {
                        pod_servers
                            .by_name
                            .entry(name)
                            .or_default()
                            .push(pod_port.clone());
                    }

                    trace!(%port, ?name, "Adding port");
                    pod_servers.by_port.insert(port, pod_port);
                    lookups.insert(
                        port,
                        Lookup {
                            rx,
                            name,
                            pod_ips: pod_ips.clone(),
                            kubelet_ips: kubelet_ips.clone(),
                        },
                    );
                }
            }
        }
    }

    (pod_servers.into(), lookups.into())
}

fn get_ports(port_match: &polixy::server::Port, ports: &PodServers) -> Vec<Arc<PodServer>> {
    match port_match {
        polixy::server::Port::Number(ref port) => ports
            .by_port
            .get(port)
            .into_iter()
            .cloned()
            .collect::<Vec<_>>(),
        polixy::server::Port::Name(ref name) => ports
            .by_name
            .get(name)
            .into_iter()
            .flat_map(|p| p.iter())
            .cloned()
            .collect::<Vec<_>>(),
    }
}

fn mk_pod_ips(status: k8s::PodStatus) -> Result<PodIps> {
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
    Ok(PodIps(ips.into()))
}

fn mk_kubelet_ips(
    spec: &k8s::PodSpec,
    ips: &HashMap<k8s::NodeName, KubeletIps>,
) -> Result<KubeletIps> {
    let name = spec
        .node_name
        .clone()
        .map(k8s::NodeName::from)
        .ok_or_else(|| anyhow!("pod missing node name"))?;
    ips.get(&name)
        .cloned()
        .ok_or_else(|| anyhow!("node IP does not exist for node {}", name))
}
