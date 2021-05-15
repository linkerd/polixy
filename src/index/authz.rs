use super::{Authz, Index, NsIndex, ServerSelector};
use crate::{
    k8s::{self, polixy},
    ClientAuthn, ClientAuthz, ClientNetwork, Identity, ServiceAccountRef,
};
use anyhow::{anyhow, bail, Context, Result};
use ipnet::IpNet;
use std::{
    collections::{hash_map::Entry as HashEntry, HashMap, HashSet},
    sync::Arc,
};
use tracing::{debug, instrument};

impl Index {
    #[instrument(
        skip(self, authz),
        fields(
            ns = ?authz.metadata.namespace,
            name = ?authz.metadata.name,
        )
    )]
    pub(super) fn apply_authz(&mut self, authz: polixy::ServerAuthorization) -> Result<()> {
        let ns_name = k8s::NsName::from_authz(&authz);
        let authz_name = polixy::authz::Name::from_authz(&authz);
        let authz = mk_authz(&ns_name, authz.spec)?;

        let NsIndex {
            ref mut authzs,
            ref mut servers,
            ..
        } = self.namespaces.get_or_default(ns_name);

        match authzs.index.entry(authz_name) {
            HashEntry::Vacant(entry) => {
                for (srv_name, srv) in servers.index.iter_mut() {
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
                    for (srv_name, srv) in servers.index.iter_mut() {
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

    #[instrument(
        skip(self, authz),
        fields(
            ns = ?authz.metadata.namespace,
            name = ?authz.metadata.name,
        )
    )]
    pub(super) fn delete_authz(&mut self, authz: polixy::ServerAuthorization) -> Result<()> {
        let ns = k8s::NsName::from_authz(&authz);
        let authz = polixy::authz::Name::from_authz(&authz);
        self.rm_authz(ns.clone(), authz.clone())
            .with_context(|| format!("ns={}, authz={}", ns, authz))
    }

    fn rm_authz(&mut self, ns_name: k8s::NsName, authz_name: polixy::authz::Name) -> Result<()> {
        let ns = self
            .namespaces
            .index
            .get_mut(&ns_name)
            .ok_or_else(|| anyhow!("removing authz from non-existent namespace"))?;

        for srv in ns.servers.index.values_mut() {
            srv.remove_authz(authz_name.clone());
        }

        debug!("Removed authz");
        Ok(())
    }

    #[instrument(skip(self, authzs))]
    pub(super) fn reset_authzs(&mut self, authzs: Vec<polixy::ServerAuthorization>) -> Result<()> {
        let mut prior_authzs = self
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
            let ns_name = k8s::NsName::from_authz(&authz);
            if let Some(ns) = prior_authzs.get_mut(&ns_name) {
                let authz_name = polixy::authz::Name::from_authz(&authz);
                ns.remove(&authz_name);
            }

            if let Err(e) = self.apply_authz(authz) {
                result = Err(e);
            }
        }

        for (ns_name, ns_authzs) in prior_authzs.into_iter() {
            for authz_name in ns_authzs.into_iter() {
                if let Err(e) = self.rm_authz(ns_name.clone(), authz_name) {
                    result = Err(e);
                }
            }
        }

        result
    }
}

fn mk_authz(ns_name: &k8s::NsName, spec: polixy::authz::ServerAuthorizationSpec) -> Result<Authz> {
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
                    .map(k8s::NsName::from_string)
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
