use super::{Index, ServerSelector, SrvIndex};
use crate::{
    k8s::{self, polixy, ResourceExt},
    ClientAuthn, ClientAuthz, ClientNetwork, Identity, ServiceAccountRef,
};
use anyhow::{anyhow, bail, Result};
use ipnet::IpNet;
use std::{
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
    sync::Arc,
};
use tracing::{debug, instrument, trace};

#[derive(Debug, Default)]
pub struct AuthzIndex {
    index: HashMap<String, Authz>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Authz {
    servers: ServerSelector,
    clients: ClientAuthz,
}

// === impl AuthzIndex ===

impl AuthzIndex {
    /// Updates the authorization and server indexes with a new or updated authorization instance.
    fn apply(&mut self, authz: polixy::ServerAuthorization, servers: &mut SrvIndex) -> Result<()> {
        let name = authz.name();
        let authz = mk_authz(authz)?;

        match self.index.entry(name) {
            HashEntry::Vacant(entry) => {
                servers.add_authz(entry.key(), &authz.servers, authz.clients.clone());
                entry.insert(authz);
            }

            HashEntry::Occupied(mut entry) => {
                // If the authorization changed materially, then update it in all servers.
                if entry.get() != &authz {
                    servers.add_authz(entry.key(), &authz.servers, authz.clients.clone());
                    entry.insert(authz);
                }
            }
        }

        Ok(())
    }

    fn delete(&mut self, name: &str) {
        self.index.remove(name);
        debug!("Removed authz");
    }

    pub fn filter_selected(
        &self,
        name: impl Into<String>,
        labels: k8s::Labels,
    ) -> impl Iterator<Item = (String, &ClientAuthz)> {
        let name = name.into();
        self.index.iter().filter_map(move |(authz_name, a)| {
            let matches = match a.servers {
                ServerSelector::Name(ref n) => {
                    trace!(r#ref = %n, %name);
                    n == &name
                }
                ServerSelector::Selector(ref s) => {
                    trace!(selector = ?s, ?labels);
                    s.matches(&labels)
                }
            };
            debug!(authz = %authz_name, %matches);
            if matches {
                Some((authz_name.clone(), &a.clients))
            } else {
                None
            }
        })
    }
}

// === impl Index ===

impl Index {
    /// Constructs an `Authz` and adds it to `Servers` it selects.
    #[instrument(
        skip(self, authz),
        fields(
            ns = ?authz.metadata.namespace,
            name = ?authz.metadata.name,
        )
    )]
    pub(super) fn apply_authz(&mut self, authz: polixy::ServerAuthorization) -> Result<()> {
        let ns = self
            .namespaces
            .get_or_default(authz.namespace().expect("namespace required"));

        ns.authzs.apply(authz, &mut ns.servers)
    }

    #[instrument(
        skip(self, authz),
        fields(
            ns = ?authz.metadata.namespace,
            name = ?authz.metadata.name,
        )
    )]
    pub(super) fn delete_authz(&mut self, authz: polixy::ServerAuthorization) {
        if let Some(ns) = self
            .namespaces
            .index
            .get_mut(authz.namespace().unwrap().as_str())
        {
            let name = authz.name();
            ns.servers.remove_authz(name.as_str());
            ns.authzs.delete(name.as_str());
        }
    }

    #[instrument(skip(self, authzs))]
    pub(super) fn reset_authzs(&mut self, authzs: Vec<polixy::ServerAuthorization>) -> Result<()> {
        let mut prior = self
            .namespaces
            .index
            .iter()
            .map(|(n, ns)| {
                let authzs = ns.authzs.index.keys().cloned().collect::<HashSet<_>>();
                (n.clone(), authzs)
            })
            .collect::<HashMap<_, _>>();

        let mut result = Ok(());
        for authz in authzs.into_iter() {
            if let Some(ns) = prior.get_mut(authz.namespace().unwrap().as_str()) {
                ns.remove(authz.name().as_str());
            }

            if let Err(e) = self.apply_authz(authz) {
                result = Err(e);
            }
        }

        for (ns_name, authzs) in prior {
            if let Some(ns) = self.namespaces.index.get_mut(&ns_name) {
                for name in authzs.into_iter() {
                    ns.servers.remove_authz(&name);
                    ns.authzs.delete(&name);
                }
            }
        }

        result
    }
}

fn mk_authz(srv: polixy::authz::ServerAuthorization) -> Result<Authz> {
    let polixy::authz::ServerAuthorization { metadata, spec, .. } = srv;

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

    let authentication = if spec.client.unauthenticated {
        ClientAuthn::Unauthenticated
    } else {
        let mtls = spec
            .client
            .mesh_tls
            .ok_or_else(|| anyhow!("client mtls missing"))?;

        if mtls.unauthenticated_tls {
            ClientAuthn::TlsUnauthenticated
        } else {
            let mut identities = Vec::new();
            let mut service_accounts = Vec::new();

            for id in mtls.identities.into_iter() {
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

            for sa in mtls.service_accounts.into_iter() {
                let name = sa.name;
                let ns = sa
                    .namespace
                    .unwrap_or_else(|| metadata.namespace.clone().unwrap());
                debug!(ns = %ns, serviceaccount = %name, "Authenticated");
                service_accounts.push(ServiceAccountRef { ns, name });
            }

            if identities.is_empty() && service_accounts.is_empty() {
                bail!("authorization authorizes no clients");
            }

            ClientAuthn::TlsAuthenticated {
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
