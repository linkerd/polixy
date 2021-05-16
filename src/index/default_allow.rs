use crate::{
    k8s, ClientAuthn, ClientAuthz, ClientNetwork, Identity, InboundServerConfig, ProxyProtocol,
    ServerRx, ServerTx,
};
use anyhow::Result;
use tokio::{sync::watch, time};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DefaultAllow {
    Authenticated,
    Cluster,
    External,
    None,
}

/// Default server configs to use when no server matches.
pub(super) struct DefaultAllows {
    external_rx: ServerRx,
    _external_tx: ServerTx,

    cluster_rx: ServerRx,
    _cluster_tx: ServerTx,

    authenticated_rx: ServerRx,
    _authenticated_tx: ServerTx,

    deny_rx: ServerRx,
    _deny_tx: ServerTx,
}

// === impl DefaultAllow ===

impl DefaultAllow {
    const ANNOTATION: &'static str = "polixy.l5d.io/default-mode";

    pub fn from_annotation(meta: &k8s::ObjectMeta) -> Result<Option<Self>> {
        if let Some(annotations) = meta.annotations.as_ref() {
            if let Some(v) = annotations.get(Self::ANNOTATION) {
                let mode = v.parse()?;
                Ok(Some(mode))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

// === impl DefaultAllows ===

impl DefaultAllows {
    pub fn new(cluster_nets: Vec<ipnet::IpNet>, detect_timeout: time::Duration) -> Self {
        let all_nets = [
            ipnet::IpNet::V4(Default::default()),
            ipnet::IpNet::V6(Default::default()),
        ];

        // A default config to be provided to pods when no matching server
        // exists.
        let (_external_tx, external_rx) = watch::channel(Self::mk_detect_config(
            detect_timeout,
            all_nets.iter().copied(),
            ClientAuthn::Unauthenticated,
        ));

        let (_cluster_tx, cluster_rx) = watch::channel(Self::mk_detect_config(
            detect_timeout,
            cluster_nets.iter().cloned(),
            ClientAuthn::Unauthenticated,
        ));

        let (_authenticated_tx, authenticated_rx) = watch::channel(Self::mk_detect_config(
            detect_timeout,
            cluster_nets,
            ClientAuthn::Authenticated {
                identities: vec![Identity::Suffix(vec![].into())],
                service_accounts: vec![],
            },
        ));

        let (_deny_tx, deny_rx) = watch::channel(InboundServerConfig {
            protocol: ProxyProtocol::Detect {
                timeout: detect_timeout,
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

    pub fn get(&self, mode: DefaultAllow) -> ServerRx {
        match mode {
            DefaultAllow::Authenticated => self.authenticated_rx.clone(),
            DefaultAllow::Cluster => self.cluster_rx.clone(),
            DefaultAllow::External => self.external_rx.clone(),
            DefaultAllow::None => self.deny_rx.clone(),
        }
    }

    fn mk_detect_config(
        timeout: time::Duration,
        nets: impl IntoIterator<Item = ipnet::IpNet>,
        authentication: ClientAuthn,
    ) -> InboundServerConfig {
        let networks = nets
            .into_iter()
            .map(|net| ClientNetwork {
                net,
                except: vec![],
            })
            .collect::<Vec<_>>();
        let authz = ClientAuthz {
            networks: networks.into(),
            authentication,
        };

        InboundServerConfig {
            protocol: ProxyProtocol::Detect { timeout },
            authorizations: Some((None, authz)).into_iter().collect(),
        }
    }
}
