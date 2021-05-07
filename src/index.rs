use crate::{
    k8s::{self, polixy},
    ClientAuthn, ClientAuthz, ClientNetwork, DefaultMode, FromResource, Identity,
    InboundServerConfig, KubeletIps, Lookup, PodIps, ProxyProtocol, ServerRx, ServerRxTx, ServerTx,
    ServiceAccountRef, SharedLookupMap,
};
use anyhow::{anyhow, bail, Context, Error, Result};
use dashmap::mapref::entry::Entry as DashEntry;
use ipnet::IpNet;
use k8s_openapi::Metadata;
use parking_lot::Mutex;
use std::{
    collections::{hash_map::Entry as HashEntry, BTreeMap, HashMap, HashSet},
    net::IpAddr,
    sync::Arc,
};
use tokio::{sync::watch, time};
use tracing::{debug, instrument, trace, warn};

pub struct Index {
    /// A shared map containing watches for all pods.  API clients simply
    /// retrieve watches from this pre-populated map.
    lookups: SharedLookupMap,

    /// Holds per-namespace pod/server/authorization indexes.
    namespaces: HashMap<k8s::NsName, NsIndex>,

    /// Cached Node IPs.
    node_ips: HashMap<k8s::NodeName, KubeletIps>,

    default_mode: DefaultMode,
    defaults: Defaults,
}

/// Default server configs to use when no server matches.
struct Defaults {
    external_rx: ServerRx,
    _external_tx: ServerTx,

    cluster_rx: ServerRx,
    _cluster_tx: ServerTx,

    authenticated_rx: ServerRx,
    _authenticated_tx: ServerTx,

    deny_rx: ServerRx,
    _deny_tx: ServerTx,
}

#[derive(Debug, Default)]
struct NsIndex {
    default_mode: Option<DefaultMode>,

    /// Caches pod labels so we can differentiate innocuous updates (like status
    /// changes) from label changes that could impact server indexing.
    pods: HashMap<k8s::PodName, Pod>,

    /// Caches a watch for each server.
    servers: HashMap<polixy::server::Name, Server>,

    authzs: HashMap<polixy::authz::Name, Authz>,
}

#[derive(Debug)]
struct Pod {
    ports: Arc<PodPorts>,
    ports_by_name: Arc<PortNames>,
    labels: k8s::Labels,
}

type PodPorts = HashMap<u16, Arc<PodPort>>;

type PortNames = HashMap<polixy::server::PortName, PodPorts>;

#[derive(Debug)]
struct PodPort {
    server_name: Mutex<Option<polixy::server::Name>>,
    tx: ServerRxTx,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Authz {
    servers: ServerSelector,
    clients: ClientAuthz,
}

/// Selects servers for an authorization.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ServerSelector {
    Name(polixy::server::Name),
    Selector(Arc<k8s::labels::Selector>),
}

#[derive(Debug)]
struct Server {
    meta: ServerMeta,
    authorizations: BTreeMap<Option<polixy::authz::Name>, ClientAuthz>,
    rx: ServerRx,
    tx: ServerTx,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerMeta {
    labels: k8s::Labels,
    port: polixy::server::Port,
    pod_selector: Arc<k8s::labels::Selector>,
    protocol: ProxyProtocol,
}

// === impl Server ===

impl Server {
    fn add_authz(&mut self, name: polixy::authz::Name, authz: ClientAuthz) {
        debug!("Adding authorization to server");
        self.authorizations.insert(Some(name), authz);
        let mut config = self.rx.borrow().clone();
        config.authorizations = self.authorizations.clone();
        self.tx.send(config).expect("config must send")
    }

    fn remove_authz(&mut self, name: polixy::authz::Name) {
        if self.authorizations.remove(&Some(name)).is_some() {
            debug!("Removing authorization from server");
            let mut config = self.rx.borrow().clone();
            config.authorizations = self.authorizations.clone();
            self.tx.send(config).expect("config must send")
        }
    }
}

// === impl Defaults ===

impl Defaults {
    fn new(cluster_nets: Vec<ipnet::IpNet>) -> Self {
        let all_nets = [
            ipnet::IpNet::V4(Default::default()),
            ipnet::IpNet::V6(Default::default()),
        ];

        // A default config to be provided to pods when no matching server
        // exists.
        let (_external_tx, external_rx) = watch::channel(Self::config(
            all_nets.iter().copied(),
            ClientAuthn::Unauthenticated,
        ));

        let (_cluster_tx, cluster_rx) = watch::channel(Self::config(
            cluster_nets.iter().cloned(),
            ClientAuthn::Unauthenticated,
        ));

        let (_authenticated_tx, authenticated_rx) = watch::channel(Self::config(
            cluster_nets,
            ClientAuthn::Authenticated {
                identities: vec![Identity::Suffix(vec![].into())],
                service_accounts: vec![],
            },
        ));

        let (_deny_tx, deny_rx) = watch::channel(InboundServerConfig {
            protocol: ProxyProtocol::Detect {
                timeout: time::Duration::from_secs(5),
            },
            authorizations: Default::default(),
        });

        Self {
            external_rx,
            _external_tx,
            cluster_rx,
            _cluster_tx,
            authenticated_rx,
            _authenticated_tx,
            deny_rx,
            _deny_tx,
        }
    }

    fn config(
        nets: impl IntoIterator<Item = ipnet::IpNet>,
        authentication: ClientAuthn,
    ) -> InboundServerConfig {
        InboundServerConfig {
            protocol: ProxyProtocol::Detect {
                timeout: time::Duration::from_secs(5),
            },
            authorizations: Some((
                None,
                ClientAuthz {
                    networks: nets
                        .into_iter()
                        .map(|net| ClientNetwork {
                            net,
                            except: vec![],
                        })
                        .collect::<Vec<_>>()
                        .into(),
                    authentication,
                },
            ))
            .into_iter()
            .collect(),
        }
    }

    fn rx(&self, mode: DefaultMode) -> ServerRx {
        match mode {
            DefaultMode::AllowExternal => self.external_rx.clone(),
            DefaultMode::AllowCluster => self.cluster_rx.clone(),
            DefaultMode::AllowAuthenticated => self.authenticated_rx.clone(),
            DefaultMode::Deny => self.deny_rx.clone(),
        }
    }
}

// === impl Index ===

impl Index {
    pub(crate) fn new(
        lookups: SharedLookupMap,
        cluster_nets: Vec<ipnet::IpNet>,
        default_mode: DefaultMode,
    ) -> Self {
        Self {
            lookups,
            node_ips: HashMap::default(),
            namespaces: HashMap::default(),

            default_mode,
            defaults: Defaults::new(cluster_nets),
        }
    }

    /// Drives indexing for all resource types.
    ///
    /// This is all driven on a single task, so it's not necessary for any of the
    /// indexing logic to worry about concurrent access for the internal indexing
    /// structures.  All updates are published to the shared `lookups` map after
    /// indexing ocrurs; but the indexing task is soley responsible for mutating
    /// it. The associated `Handle` is used for reads against this.
    #[instrument(skip(self, resources), fields(result))]
    pub(crate) async fn index(mut self, resources: k8s::ResourceWatches) -> Error {
        let k8s::ResourceWatches {
            mut namespaces,
            mut nodes,
            mut pods,
            mut servers,
            mut authorizations,
        } = resources;

        loop {
            let res = tokio::select! {
                // Track namespace-level annotations
                up = namespaces.recv() => match up {
                    k8s::Event::Applied(ns) => self.apply_ns(ns).context("apply"),
                    k8s::Event::Deleted(ns) => self.delete_ns(ns).context("delete"),
                    k8s::Event::Restarted(nss) => self.reset_ns(nss).context("reset"),
                }.context("namespaces"),

                // Track the kubelet IPs for all nodes.
                up = nodes.recv() => match up {
                    k8s::Event::Applied(node) => self.apply_node(node).context("apply"),
                    k8s::Event::Deleted(node) => self.delete_node(node).context("delete"),
                    k8s::Event::Restarted(nodes) => self.reset_nodes(nodes).context("reset"),
                }.context("nodes"),

                up = pods.recv() => match up {
                    k8s::Event::Applied(pod) => self.apply_pod(pod).context("apply"),
                    k8s::Event::Deleted(pod) => self.delete_pod(pod).context("delete"),
                    k8s::Event::Restarted(pods) => self.reset_pods(pods).context("reset"),
                }.context("pods"),

                up = servers.recv() => match up {
                    k8s::Event::Applied(srv) => {
                        self.apply_server(srv);
                        Ok(())
                    }
                    k8s::Event::Deleted(srv) => self.delete_server(srv).context("delete"),
                    k8s::Event::Restarted(srvs) => self.reset_servers(srvs).context("reset"),
                }.context("servers"),

                up = authorizations.recv() => match up {
                    k8s::Event::Applied(authz) => self.apply_authz(authz).context("apply"),
                    k8s::Event::Deleted(authz) => self.delete_authz(authz).context("delete"),
                    k8s::Event::Restarted(authzs) => self.reset_authzs(authzs).context("reset"),
                }.context("authorizations"),
            };
            if let Err(error) = res {
                warn!(?error);
            }
        }
    }

    // // === Namespace indexing ===

    #[instrument(
        skip(self, ns),
        fields(ns = ?ns.metadata.name)
    )]
    fn apply_ns(&mut self, ns: k8s::Namespace) -> Result<()> {
        let name = k8s::NsName::from_ns(&ns);

        let mode = if let Some(anns) = ns.metadata().annotations.as_ref() {
            if let Some(ann) = anns.get("polixy.l5d.io/default-mode") {
                Some(ann.parse::<DefaultMode>()?)
            } else {
                None
            }
        } else {
            None
        };

        let ns = self.namespaces.entry(name).or_default();
        if mode == ns.default_mode {
            return Ok(());
        }

        ns.default_mode = mode;

        // Update the default server on all pod ports without an associated named server.
        let rx = self.defaults.rx(mode.unwrap_or(self.default_mode));
        for pod in ns.pods.values() {
            for p in pod.ports.values() {
                let srv = p.server_name.lock();
                if srv.is_none() && p.tx.send(rx.clone()).is_err() {
                    warn!(server = ?*srv, "Failed to update server");
                }
            }
        }

        Ok(())
    }

    #[instrument(
        skip(self, ns),
        fields(ns = ?ns.metadata.name)
    )]
    fn delete_ns(&mut self, ns: k8s::Namespace) -> Result<()> {
        let name = k8s::NsName::from_ns(&ns);
        self.rm_ns(&name)
    }

    fn rm_ns(&mut self, name: &k8s::NsName) -> Result<()> {
        if self.namespaces.remove(name).is_none() {
            bail!("node {} already deleted", name);
        }
        debug!("Deleted");
        Ok(())
    }

    #[instrument(skip(self, nss))]
    fn reset_ns(&mut self, nss: Vec<k8s::Namespace>) -> Result<()> {
        // Avoid rebuilding data for nodes that have not changed.
        let mut prior_names = self.namespaces.keys().cloned().collect::<HashSet<_>>();

        let mut result = Ok(());
        for ns in nss.into_iter() {
            let name = k8s::NsName::from_ns(&ns);
            prior_names.remove(&name);
            if let Err(error) = self.apply_ns(ns) {
                warn!(%name, %error, "Failed to apply namespace");
                result = Err(error);
            }
        }

        for name in prior_names.into_iter() {
            debug!(?name, "Removing defunct namespace");
            if let Err(error) = self.rm_ns(&name) {
                result = Err(error);
            }
        }

        result
    }

    // === Node->IP indexing ===

    #[instrument(
        skip(self, node),
        fields(name = ?node.metadata.name)
    )]
    fn apply_node(&mut self, node: k8s::Node) -> Result<()> {
        let name = k8s::NodeName::from_resource(&node);

        match self.node_ips.entry(name) {
            HashEntry::Vacant(entry) => {
                let ips = Self::kubelet_ips(node)
                    .with_context(|| format!("failed to load kubelet IPs for {}", entry.key()))?;
                debug!(?ips, "Adding");
                entry.insert(ips);
            }
            HashEntry::Occupied(_) => trace!("Already existed"),
        }

        Ok(())
    }

    #[instrument(
        skip(self, node),
        fields(name = ?node.metadata.name)
    )]
    fn delete_node(&mut self, node: k8s::Node) -> Result<()> {
        let name = k8s::NodeName::from_resource(&node);
        if self.node_ips.remove(&name).is_some() {
            debug!("Deleted");
            Ok(())
        } else {
            Err(anyhow!("node {} already deleted", name))
        }
    }

    #[instrument(skip(self, nodes))]
    fn reset_nodes(&mut self, nodes: Vec<k8s::Node>) -> Result<()> {
        // Avoid rebuilding data for nodes that have not changed.
        let mut prior_names = self.node_ips.keys().cloned().collect::<HashSet<_>>();

        let mut result = Ok(());
        for node in nodes.into_iter() {
            let name = k8s::NodeName::from_resource(&node);
            if !prior_names.remove(&name) {
                if let Err(error) = self.apply_node(node) {
                    warn!(%name, %error, "Failed to apply node");
                    result = Err(error);
                }
            } else {
                trace!(%name, "Already existed");
            }
        }

        for name in prior_names.into_iter() {
            debug!(?name, "Removing defunct node");
            let removed = self.node_ips.remove(&name).is_some();
            debug_assert!(removed, "node must be removable");
            if !removed {
                result = Err(anyhow!("node {} already removed", name));
            }
        }

        result
    }

    fn kubelet_ips(node: k8s::Node) -> Result<KubeletIps> {
        let spec = node.spec.ok_or_else(|| anyhow!("node missing spec"))?;

        let addrs = if let Some(nets) = spec.pod_cidrs {
            nets.into_iter()
                .map(Self::cidr_to_kubelet_ip)
                .collect::<Result<Vec<_>>>()?
        } else {
            let cidr = spec
                .pod_cidr
                .ok_or_else(|| anyhow!("node missing pod_cidr"))?;
            let ip = Self::cidr_to_kubelet_ip(cidr)?;
            vec![ip]
        };

        Ok(KubeletIps(addrs.into()))
    }

    fn cidr_to_kubelet_ip(cidr: String) -> Result<IpAddr> {
        cidr.parse::<IpNet>()
            .with_context(|| format!("invalid CIDR {}", cidr))?
            .hosts()
            .next()
            .ok_or_else(|| anyhow!("pod CIDR network is empty"))
    }

    // === Pod->Port->Server indexing ===

    #[instrument(
        skip(self, pod),
        fields(
            ns = ?pod.metadata.namespace,
            name = ?pod.metadata.name,
        )
    )]
    fn apply_pod(&mut self, pod: k8s::Pod) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&pod);
        let pod_name = k8s::PodName::from_resource(&pod);
        let spec = pod.spec.ok_or_else(|| anyhow!("pod missing spec"))?;
        let status = pod.status.ok_or_else(|| anyhow!("pod missing status"))?;

        let NsIndex {
            default_mode,
            ref mut pods,
            ref mut servers,
            ..
        } = self.namespaces.entry(ns_name.clone()).or_default();

        let lookups_entry = self.lookups.entry((ns_name, pod_name.clone()));

        match (pods.entry(pod_name), lookups_entry) {
            (HashEntry::Vacant(pod_entry), DashEntry::Vacant(lookups_entry)) => {
                let labels = k8s::Labels::from(pod.metadata.labels);

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

                                let server_rx =
                                    self.defaults.rx(default_mode.unwrap_or(self.default_mode));
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

                for (srv_name, server) in servers.iter() {
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
                let labels = k8s::Labels::from(pod.metadata.labels);

                if pod_entry.get().labels != labels {
                    for (srv_name, server) in servers.iter() {
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

    fn get_ports(
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
    fn delete_pod(&mut self, pod: k8s::Pod) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&pod);
        let pod_name = k8s::PodName::from_resource(&pod);
        self.rm_pod(&ns_name, &pod_name)
    }

    fn rm_pod(&mut self, ns: &k8s::NsName, pod: &k8s::PodName) -> Result<()> {
        self.namespaces
            .get_mut(ns)
            .ok_or_else(|| anyhow!("namespace {} doesn't exist", ns))?
            .pods
            .remove(pod)
            .ok_or_else(|| anyhow!("pod {} doesn't exist", pod))?;

        self.lookups
            .remove(&(ns.clone(), pod.clone()))
            .ok_or_else(|| anyhow!("pod {} doesn't exist in namespace {}", pod, ns))?;

        debug!("Removed pod");

        Ok(())
    }

    #[instrument(skip(self, pods))]
    fn reset_pods(&mut self, pods: Vec<k8s::Pod>) -> Result<()> {
        let mut prior_pod_labels = self
            .namespaces
            .iter()
            .map(|(n, ns)| {
                let pods = ns
                    .pods
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

    // === Server indexing ===

    #[instrument(
        skip(self, srv),
        fields(
            ns = ?srv.metadata.namespace,
            name = ?srv.metadata.name,
        )
    )]
    fn apply_server(&mut self, srv: polixy::Server) {
        let ns_name = k8s::NsName::from_resource(&srv);
        let srv_name = polixy::server::Name::from_resource(&srv);
        let labels = k8s::Labels::from(srv.metadata.labels);
        let port = srv.spec.port;

        let NsIndex {
            ref pods,
            authzs: ref ns_authzs,
            ref mut servers,
            default_mode: _,
        } = self.namespaces.entry(ns_name).or_default();

        match servers.entry(srv_name) {
            HashEntry::Vacant(entry) => {
                let protocol = Self::mk_protocol(srv.spec.proxy_protocol.as_ref());

                let mut authorizations = BTreeMap::new();
                for (authz_name, a) in ns_authzs.iter() {
                    let matches = match a.servers {
                        ServerSelector::Name(ref n) => {
                            trace!(r#ref = %n, name = %entry.key());
                            n == entry.key()
                        }
                        ServerSelector::Selector(ref s) => {
                            trace!(selector = ?s, ?labels);
                            s.matches(&labels)
                        }
                    };
                    if matches {
                        debug!(authz = %authz_name, %matches);
                        authorizations.insert(Some(authz_name.clone()), a.clients.clone());
                    } else {
                        trace!(authz = %authz_name, %matches);
                    }
                }

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
                    tx,
                    rx,
                    meta,
                    authorizations,
                });
            }

            HashEntry::Occupied(mut entry) => {
                let protocol = Self::mk_protocol(srv.spec.proxy_protocol.as_ref());

                // If something about the server changed, we need to update the
                // config to reflect the change.
                if entry.get().meta.labels != labels || entry.get().meta.protocol == protocol {
                    // NB: Only a single task applies server updates, so it's
                    // okay to borrow a version, modify, and send it.  We don't
                    // need a lock because serialization is guaranteed.
                    let mut config = entry.get().rx.borrow().clone();

                    if entry.get().meta.labels != labels {
                        let mut authorizations = BTreeMap::new();
                        for (authz_name, a) in ns_authzs.iter() {
                            let matches = match a.servers {
                                ServerSelector::Name(ref n) => {
                                    trace!(r#ref = %n, name = %entry.key());
                                    n == entry.key()
                                }
                                ServerSelector::Selector(ref s) => {
                                    trace!(selector = ?s, ?labels);
                                    s.matches(&labels)
                                }
                            };
                            if matches {
                                debug!(authz = %authz_name, %matches);
                                authorizations.insert(Some(authz_name.clone()), a.clients.clone());
                            } else {
                                trace!(authz = %authz_name, %matches);
                            }
                        }

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
        for (pod_name, pod) in pods.iter() {
            for (srv_name, srv) in servers.iter() {
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
    fn delete_server(&mut self, srv: polixy::Server) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&srv);
        let srv_name = polixy::server::Name::from_resource(&srv);
        self.rm_server(ns_name, srv_name)
    }

    fn rm_server(&mut self, ns_name: k8s::NsName, srv_name: polixy::server::Name) -> Result<()> {
        let ns = self
            .namespaces
            .get_mut(&ns_name)
            .ok_or_else(|| anyhow!("removing server from non-existent namespace {}", ns_name))?;

        if ns.servers.remove(&srv_name).is_none() {
            bail!("removing non-existent server {}", srv_name);
        }

        // Reset the server config for all pods that were using this server.
        for (pod_name, pod) in ns.pods.iter() {
            for (port_num, port) in pod.ports.iter() {
                let mut sn = port.server_name.lock();
                if sn.as_ref() == Some(&srv_name) {
                    debug!(pod = %pod_name, port = %port_num, "Removing server from pod");
                    *sn = None;
                    let rx = self
                        .defaults
                        .rx(ns.default_mode.unwrap_or(self.default_mode));
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
    fn reset_servers(&mut self, srvs: Vec<polixy::Server>) -> Result<()> {
        let mut prior_servers = self
            .namespaces
            .iter()
            .map(|(n, ns)| {
                let servers = ns
                    .servers
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

    // === Authorization indexing ===

    #[instrument(
        skip(self, authz),
        fields(
            ns = ?authz.metadata.namespace,
            name = ?authz.metadata.name,
        )
    )]
    fn apply_authz(&mut self, authz: polixy::ServerAuthorization) -> Result<()> {
        let ns_name = k8s::NsName::from_resource(&authz);
        let authz_name = polixy::authz::Name::from_resource(&authz);
        let authz = Self::mk_authz(&ns_name, authz.spec)
            .with_context(|| format!("ns={}, authz={}", ns_name, authz_name))?;

        let NsIndex {
            ref mut authzs,
            ref mut servers,
            ..
        } = self.namespaces.entry(ns_name).or_default();

        match authzs.entry(authz_name) {
            HashEntry::Vacant(entry) => {
                for (srv_name, srv) in servers.iter_mut() {
                    let matches = match authz.servers {
                        ServerSelector::Name(ref n) => n == srv_name,
                        ServerSelector::Selector(ref s) => s.matches(&srv.meta.labels),
                    };
                    if matches {
                        debug!(authz = %entry.key(), "Adding authz to server");
                        srv.add_authz(entry.key().clone(), authz.clients.clone());
                    }
                }
                entry.insert(authz);
            }

            HashEntry::Occupied(mut entry) => {
                // If the authorization changed materially, then update it in all servers.
                if entry.get() != &authz {
                    for (srv_name, srv) in servers.iter_mut() {
                        let matches = match authz.servers {
                            ServerSelector::Name(ref n) => n == srv_name,
                            ServerSelector::Selector(ref s) => s.matches(&srv.meta.labels),
                        };
                        if matches {
                            debug!(authz = %entry.key(), "Adding authz to server");
                            srv.add_authz(entry.key().clone(), authz.clients.clone());
                        } else {
                            debug!(authz = %entry.key(), "Removing authz from server");
                            srv.remove_authz(entry.key().clone());
                        }
                    }
                    entry.insert(authz);
                }
            }
        };

        Ok(())
    }

    fn mk_authz(
        ns_name: &k8s::NsName,
        spec: polixy::authz::ServerAuthorizationSpec,
    ) -> Result<Authz> {
        let servers = {
            let polixy::authz::Server { name, selector } = spec.server;
            match (name, selector) {
                (Some(n), None) => ServerSelector::Name(n),
                (None, Some(sel)) => ServerSelector::Selector(sel.into()),
                (Some(_), Some(_)) => bail!("authorization selection is ambiguous"),
                (None, None) => bail!("authorization selects no servers"),
            }
        };

        let networks = if let Some(networks) = spec.client.networks {
            let mut nets = Vec::with_capacity(networks.len());
            for polixy::authz::Network { cidr, except } in networks.into_iter() {
                let net = cidr.parse::<IpNet>()?;
                debug!(%net, "Unauthenticated");
                let except = except
                    .into_iter()
                    .flatten()
                    .map(|cidr| cidr.parse().map_err(Into::into))
                    .collect::<Result<Vec<IpNet>>>()?;
                nets.push(ClientNetwork { net, except });
            }
            nets.into()
        } else {
            // TODO this should only be cluster-local IPs.
            vec![
                ClientNetwork {
                    net: ipnet::IpNet::V4(Default::default()),
                    except: vec![],
                },
                ClientNetwork {
                    net: ipnet::IpNet::V6(Default::default()),
                    except: vec![],
                },
            ]
            .into()
        };

        let authentication = if let Some(true) = spec.client.unauthenticated {
            ClientAuthn::Unauthenticated
        } else {
            let mtls = spec
                .client
                .mesh_tls
                .ok_or_else(|| anyhow!("client mtls missing"))?;

            if let Some(true) = mtls.unauthenticated_tls {
                // XXX FIXME
                ClientAuthn::Unauthenticated
            } else {
                let mut identities = Vec::new();
                let mut service_accounts = Vec::new();

                for id in mtls.identities.into_iter().flatten() {
                    if id == "*" {
                        debug!(suffix = %id, "Authenticated");
                        identities.push(Identity::Suffix(Arc::new([])));
                    } else if id.starts_with("*.") {
                        debug!(suffix = %id, "Authenticated");
                        let mut parts = id.split('.');
                        let star = parts.next();
                        debug_assert_eq!(star, Some("*"));
                        identities.push(Identity::Suffix(
                            parts.map(|p| p.to_string()).collect::<Vec<_>>().into(),
                        ));
                    } else {
                        debug!(%id, "Authenticated");
                        identities.push(Identity::Name(id.into()));
                    }
                }

                for sa in mtls.service_accounts.into_iter().flatten() {
                    let name = sa.name;
                    let ns = sa
                        .namespace
                        .map(k8s::NsName::from)
                        .unwrap_or_else(|| ns_name.clone());
                    debug!(ns = %ns, serviceaccount = %name, "Authenticated");
                    // FIXME configurable cluster domain
                    service_accounts.push(ServiceAccountRef {
                        ns,
                        name: name.into(),
                    });
                }

                if identities.is_empty() && service_accounts.is_empty() {
                    bail!("authorization authorizes no clients");
                }

                ClientAuthn::Authenticated {
                    identities,
                    service_accounts,
                }
            }
        };

        Ok(Authz {
            servers,
            clients: ClientAuthz {
                networks,
                authentication,
            },
        })
    }

    #[instrument(
        skip(self, authz),
        fields(
            ns = ?authz.metadata.namespace,
            name = ?authz.metadata.name,
        )
    )]
    fn delete_authz(&mut self, authz: polixy::ServerAuthorization) -> Result<()> {
        let ns = k8s::NsName::from_resource(&authz);
        let authz = polixy::authz::Name::from_resource(&authz);
        self.rm_authz(ns.clone(), authz.clone())
            .with_context(|| format!("ns={}, authz={}", ns, authz))
    }

    fn rm_authz(&mut self, ns_name: k8s::NsName, authz_name: polixy::authz::Name) -> Result<()> {
        let ns = self
            .namespaces
            .get_mut(&ns_name)
            .ok_or_else(|| anyhow!("removing authz from non-existent namespace"))?;

        for srv in ns.servers.values_mut() {
            srv.remove_authz(authz_name.clone());
        }

        debug!("Removed authz");
        Ok(())
    }

    #[instrument(skip(self, authzs))]
    fn reset_authzs(&mut self, authzs: Vec<polixy::ServerAuthorization>) -> Result<()> {
        let mut prior_authzs = self
            .namespaces
            .iter()
            .map(|(n, ns)| (n.clone(), ns.authzs.clone()))
            .collect::<HashMap<_, _>>();

        let mut result = Ok(());
        for authz in authzs.into_iter() {
            let ns_name = k8s::NsName::from_resource(&authz);
            let authz_name = polixy::authz::Name::from_resource(&authz);

            if let Some(prior_ns) = prior_authzs.get_mut(&ns_name) {
                if let Some(prior_authz) = prior_ns.remove(&authz_name) {
                    match Self::mk_authz(&ns_name, authz.spec.clone()) {
                        Ok(meta) => {
                            if prior_authz == meta {
                                continue;
                            }
                        }
                        Err(e) => {
                            result = Err(e);
                            continue;
                        }
                    }
                }
            }

            if let Err(e) = self.apply_authz(authz) {
                result = Err(e);
            }
        }

        for (ns_name, ns_authzs) in prior_authzs.into_iter() {
            for (authz_name, _) in ns_authzs.into_iter() {
                if let Err(e) = self.rm_authz(ns_name.clone(), authz_name) {
                    result = Err(e);
                }
            }
        }

        result
    }
}
