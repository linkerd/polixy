use super::{Index, NsIndex, Server, ServerMeta};
use crate::{
    k8s::{self, polixy},
    FromResource, InboundServerConfig, ProxyProtocol,
};
use anyhow::{anyhow, bail, Result};
use std::{
    collections::{hash_map::Entry as HashEntry, HashMap},
    sync::Arc,
};
use tokio::{sync::watch, time};
use tracing::{debug, instrument, trace};

impl Index {
    #[instrument(
        skip(self, srv),
        fields(
            ns = ?srv.metadata.namespace,
            name = ?srv.metadata.name,
        )
    )]
    pub(super) fn apply_server(&mut self, srv: polixy::Server) {
        let ns_name = k8s::NsName::from_resource(&srv);
        let srv_name = polixy::server::Name::from_resource(&srv);
        let labels = k8s::Labels::from(srv.metadata.labels);
        let port = srv.spec.port;
        let protocol = Self::mk_protocol(srv.spec.proxy_protocol.as_ref());

        let NsIndex {
            ref pods,
            authzs: ref ns_authzs,
            ref mut servers,
            default_mode: _,
        } = self.namespaces.get_or_default(ns_name);

        match servers.index.entry(srv_name) {
            HashEntry::Vacant(entry) => {
                let authorizations = ns_authzs.collect_by_server(entry.key(), &labels);
                let meta = ServerMeta {
                    labels,
                    port,
                    pod_selector: srv.spec.pod_selector.into(),
                    protocol: protocol.clone(),
                };
                let (tx, rx) = watch::channel(InboundServerConfig {
                    protocol,
                    authorizations: authorizations.clone(),
                });
                debug!(authzs = ?authorizations.keys());
                entry.insert(Server {
                    meta,
                    authorizations,
                    rx,
                    tx,
                });
            }

            HashEntry::Occupied(mut entry) => {
                // If something about the server changed, we need to update the
                // config to reflect the change.
                if entry.get().meta.labels != labels || entry.get().meta.protocol == protocol {
                    // NB: Only a single task applies server updates, so it's
                    // okay to borrow a version, modify, and send it.  We don't
                    // need a lock because serialization is guaranteed.
                    let mut config = entry.get().rx.borrow().clone();

                    if entry.get().meta.labels != labels {
                        let authorizations = ns_authzs.collect_by_server(entry.key(), &labels);
                        debug!(authzs = ?authorizations.keys());
                        config.authorizations = authorizations.clone();
                        entry.get_mut().meta.labels = labels;
                        entry.get_mut().authorizations = authorizations;
                    }

                    config.protocol = protocol.clone();
                    entry.get_mut().meta.protocol = protocol;

                    debug!("Updating");
                    entry
                        .get()
                        .tx
                        .send(config)
                        .expect("server update must succeed");
                }

                // If the pod/port selector didn't change, we don't need to
                // refresh the index.
                if *entry.get().meta.pod_selector == srv.spec.pod_selector
                    && entry.get().meta.port == port
                {
                    return;
                }

                entry.get_mut().meta.pod_selector = srv.spec.pod_selector.into();
                entry.get_mut().meta.port = port;
            }
        }

        // If we've updated the server->pod selection, then we need to reindex
        // all pods and servers.
        for (pod_name, pod) in pods.index.iter() {
            for (srv_name, srv) in servers.index.iter() {
                if srv.meta.pod_selector.matches(&pod.labels) {
                    for pod_port in
                        Self::get_ports(&srv.meta.port, &*pod.ports_by_name, &*pod.ports)
                            .into_iter()
                    {
                        debug!(pod = %pod_name, port = ?srv.meta.port, "Matches");

                        // TODO handle conflicts
                        let mut sn = pod_port.server_name.lock();
                        if let Some(sn) = sn.as_ref() {
                            debug_assert!(
                                sn == srv_name,
                                "pod port matches multiple servers: {} and {}",
                                sn,
                                srv_name
                            );
                        }
                        *sn = Some(srv_name.clone());

                        // It's up to the lookup stream to de-duplicate updates.
                        pod_port
                            .tx
                            .send(srv.rx.clone())
                            .expect("pod config receiver is held");
                        debug!(server = %srv_name, "Pod server udpated");
                        trace!(selector = ?srv.meta.pod_selector, labels = ?pod.labels);
                    }
                } else {
                    trace!(
                        server = %srv_name,
                        pod = %pod_name,
                        selector = ?srv.meta.pod_selector,
                        labels = ?pod.labels,
                        "Does not match",
                    );
                }
            }
        }
    }

    fn mk_protocol(p: Option<&polixy::server::ProxyProtocol>) -> ProxyProtocol {
        match p {
            Some(polixy::server::ProxyProtocol::Unknown) | None => ProxyProtocol::Detect {
                timeout: time::Duration::from_secs(5),
            },
            Some(polixy::server::ProxyProtocol::Http1) => ProxyProtocol::Http1,
            Some(polixy::server::ProxyProtocol::Http2) => ProxyProtocol::Http2,
            Some(polixy::server::ProxyProtocol::Grpc) => ProxyProtocol::Grpc,
            Some(polixy::server::ProxyProtocol::Opaque) => ProxyProtocol::Opaque,
            Some(polixy::server::ProxyProtocol::Tls) => ProxyProtocol::Tls,
        }
    }

    #[instrument(
        skip(self, srv),
        fields(
            ns = ?srv.metadata.namespace,
            name = ?srv.metadata.name,
        )
    )]
    pub(super) fn delete_server(&mut self, srv: polixy::Server) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&srv);
        let srv_name = polixy::server::Name::from_resource(&srv);
        self.rm_server(ns_name, srv_name)
    }

    fn rm_server(&mut self, ns_name: k8s::NsName, srv_name: polixy::server::Name) -> Result<()> {
        let ns =
            self.namespaces.index.get_mut(&ns_name).ok_or_else(|| {
                anyhow!("removing server from non-existent namespace {}", ns_name)
            })?;

        if ns.servers.index.remove(&srv_name).is_none() {
            bail!("removing non-existent server {}", srv_name);
        }

        // Reset the server config for all pods that were using this server.
        for (pod_name, pod) in ns.pods.index.iter() {
            for (port_num, port) in pod.ports.iter() {
                let mut sn = port.server_name.lock();
                if sn.as_ref() == Some(&srv_name) {
                    debug!(pod = %pod_name, port = %port_num, "Removing server from pod");
                    *sn = None;
                    let rx = self.default_mode_rxs.get(ns.default_mode);
                    port.tx
                        .send(rx)
                        .expect("pod config receiver must still be held");
                } else {
                    trace!(pod = %pod_name, port = %port_num, server = ?sn, "Server does not match");
                }
            }
        }

        debug!("Removed server");
        Ok(())
    }

    #[instrument(skip(self, srvs))]
    pub(super) fn reset_servers(&mut self, srvs: Vec<polixy::Server>) -> Result<()> {
        let mut prior_servers = self
            .namespaces
            .index
            .iter()
            .map(|(n, ns)| {
                let servers = ns
                    .servers
                    .index
                    .iter()
                    .map(|(n, s)| (n.clone(), s.meta.clone()))
                    .collect::<HashMap<_, _>>();
                (n.clone(), servers)
            })
            .collect::<HashMap<_, _>>();

        let mut result = Ok(());
        for srv in srvs.into_iter() {
            let ns_name = k8s::NsName::from_resource(&srv);
            let srv_name = polixy::server::Name::from_resource(&srv);

            if let Some(prior_servers) = prior_servers.get_mut(&ns_name) {
                if let Some(prior_meta) = prior_servers.remove(&srv_name) {
                    let labels = k8s::Labels::from(srv.metadata.labels.clone());
                    let port = srv.spec.port.clone();
                    let protocol = Self::mk_protocol(srv.spec.proxy_protocol.as_ref());
                    let meta = ServerMeta {
                        labels,
                        port,
                        pod_selector: Arc::new(srv.spec.pod_selector.clone()),
                        protocol,
                    };
                    if prior_meta == meta {
                        continue;
                    }
                }
            }

            self.apply_server(srv);
        }

        for (ns_name, ns_servers) in prior_servers.into_iter() {
            for (srv_name, _) in ns_servers.into_iter() {
                if let Err(e) = self.rm_server(ns_name.clone(), srv_name) {
                    result = Err(e);
                }
            }
        }

        result
    }
}
