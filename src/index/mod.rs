mod authz;
mod namespace;
mod node;
mod pod;
mod server;

use crate::{
    k8s::{self, polixy},
    ClientAuthn, ClientAuthz, ClientNetwork, DefaultMode, Identity, InboundServerConfig,
    KubeletIps, ProxyProtocol, ServerRx, ServerRxTx, ServerTx, SharedLookupMap,
};
use anyhow::{Context, Error};
use parking_lot::Mutex;
use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};
use tokio::{sync::watch, time};
use tracing::{debug, instrument, trace, warn};

pub struct Index {
    /// A shared map containing watches for all pods.  API clients simply
    /// retrieve watches from this pre-populated map.
    lookups: SharedLookupMap,

    /// Holds per-namespace pod/server/authorization indexes.
    namespaces: Namespaces,

    /// Cached Node IPs.
    node_ips: HashMap<k8s::NodeName, KubeletIps>,

    default_mode_rxs: DefaultModeRxs,
}

/// Default server configs to use when no server matches.
struct DefaultModeRxs {
    external_rx: ServerRx,
    _external_tx: ServerTx,

    cluster_rx: ServerRx,
    _cluster_tx: ServerTx,

    authenticated_rx: ServerRx,
    _authenticated_tx: ServerTx,

    deny_rx: ServerRx,
    _deny_tx: ServerTx,
}

#[derive(Debug)]
struct Namespaces {
    default_mode: DefaultMode,
    index: HashMap<k8s::NsName, NsIndex>,
}

#[derive(Debug)]
struct NsIndex {
    default_mode: DefaultMode,

    /// Caches pod labels so we can differentiate innocuous updates (like status
    /// changes) from label changes that could impact server indexing.
    pods: PodIndex,

    /// Caches a watch for each server.
    servers: SrvIndex,

    authzs: AuthzIndex,
}

#[derive(Debug, Default)]
struct PodIndex {
    index: HashMap<k8s::PodName, Pod>,
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

#[derive(Debug, Default)]
struct AuthzIndex {
    index: HashMap<polixy::authz::Name, Authz>,
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

#[derive(Debug, Default)]
struct SrvIndex {
    index: HashMap<polixy::server::Name, Server>,
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

// === impl DefaultModeRxs ===

impl DefaultModeRxs {
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

    fn get(&self, mode: DefaultMode) -> ServerRx {
        match mode {
            DefaultMode::AllowExternal => self.external_rx.clone(),
            DefaultMode::AllowCluster => self.cluster_rx.clone(),
            DefaultMode::AllowAuthenticated => self.authenticated_rx.clone(),
            DefaultMode::Deny => self.deny_rx.clone(),
        }
    }
}

// === impl Namespaces ===

impl Namespaces {
    fn get_or_default(&mut self, name: k8s::NsName) -> &mut NsIndex {
        let default_mode = self.default_mode;
        self.index.entry(name).or_insert_with(|| NsIndex {
            default_mode,
            pods: PodIndex::default(),
            servers: SrvIndex::default(),
            authzs: AuthzIndex::default(),
        })
    }
}

// === impl Index ===

impl Index {
    pub(crate) fn new(
        lookups: SharedLookupMap,
        cluster_nets: Vec<ipnet::IpNet>,
        default_mode: DefaultMode,
    ) -> Self {
        let namespaces = Namespaces {
            default_mode,
            index: HashMap::default(),
        };
        Self {
            lookups,
            namespaces,
            node_ips: HashMap::default(),
            default_mode_rxs: DefaultModeRxs::new(cluster_nets),
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
                    k8s::Event::Applied(ns) => self.apply_ns(ns).context("applying a namespace"),
                    k8s::Event::Deleted(ns) => self.delete_ns(ns).context("deleting a namespace"),
                    k8s::Event::Restarted(nss) => self.reset_ns(nss).context("resetting namespaces"),
                },

                // Track the kubelet IPs for all nodes.
                up = nodes.recv() => match up {
                    k8s::Event::Applied(node) => self.apply_node(node).context("applying a node"),
                    k8s::Event::Deleted(node) => self.delete_node(node).context("deleting a node"),
                    k8s::Event::Restarted(nodes) => self.reset_nodes(nodes).context("resetting nodes"),
                },

                up = pods.recv() => match up {
                    k8s::Event::Applied(pod) => self.apply_pod(pod).context("applying a pod"),
                    k8s::Event::Deleted(pod) => self.delete_pod(pod).context("deleting a pod"),
                    k8s::Event::Restarted(pods) => self.reset_pods(pods).context("resetting pods"),
                },

                up = servers.recv() => match up {
                    k8s::Event::Applied(srv) => {
                        self.apply_server(srv);
                        Ok(())
                    }
                    k8s::Event::Deleted(srv) => self.delete_server(srv).context("deleting a server"),
                    k8s::Event::Restarted(srvs) => self.reset_servers(srvs).context("resetting servers"),
                },

                up = authorizations.recv() => match up {
                    k8s::Event::Applied(authz) => self.apply_authz(authz).context("applying an authorization"),
                    k8s::Event::Deleted(authz) => self.delete_authz(authz).context("deleting an authorization"),
                    k8s::Event::Restarted(authzs) => self.reset_authzs(authzs).context("resetting authorizations"),
                },
            };
            if let Err(error) = res {
                warn!(?error);
            }
        }
    }
}

// === impl AuthzIndex ===

impl AuthzIndex {
    fn collect_by_server(
        &self,
        name: &k8s::polixy::server::Name,
        labels: &k8s::Labels,
    ) -> BTreeMap<Option<k8s::polixy::authz::Name>, ClientAuthz> {
        let mut authorizations = BTreeMap::new();

        for (authz_name, a) in self.index.iter() {
            let matches = match a.servers {
                ServerSelector::Name(ref n) => {
                    trace!(r#ref = %n, %name);
                    n == name
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

        authorizations
    }
}
