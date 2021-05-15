use super::Index;
use crate::{k8s, DefaultMode};
use anyhow::{bail, Result};
use std::collections::HashSet;
use tracing::{debug, instrument, warn};

impl Index {
    #[instrument(
        skip(self, ns),
        fields(ns = ?ns.metadata.name)
    )]
    pub(super) fn apply_ns(&mut self, ns: k8s::Namespace) -> Result<()> {
        // Read the `default-mode` annotation from the ns metadata, which indicates the default
        // behavior for pod-ports in the namespace that lack a server instance.
        let mode = match DefaultMode::from_annotation(&ns.metadata) {
            Ok(Some(mode)) => mode,
            Ok(None) => self.namespaces.default_mode,
            Err(error) => {
                warn!(%error, "invalid default-mode annotation");
                self.namespaces.default_mode
            }
        };

        // Get the cached namespace index (or load the default).
        let ns = self.namespaces.get_or_default(k8s::NsName::from_ns(&ns));

        // If the mode is changed (or non-default), update all of the pod-ports that don't don't
        // have an associated server instance. This allows us to dynamically update the default
        // behavior after pods have been created.
        if mode != ns.default_mode {
            ns.default_mode = mode;

            let rx = self.default_mode_rxs.get(mode);
            for pod in ns.pods.index.values() {
                for p in pod.servers.by_port.values() {
                    let srv = p.server_name.lock();
                    if srv.is_none() && p.tx.send(rx.clone()).is_err() {
                        warn!(server = ?*srv, "Failed to update server");
                    }
                }
            }
        }

        Ok(())
    }

    #[instrument(
        skip(self, ns),
        fields(ns = ?ns.metadata.name)
    )]
    pub(super) fn delete_ns(&mut self, ns: k8s::Namespace) -> Result<()> {
        let name = k8s::NsName::from_ns(&ns);
        self.rm_ns(&name)
    }

    fn rm_ns(&mut self, name: &k8s::NsName) -> Result<()> {
        if self.namespaces.index.remove(name).is_none() {
            bail!("node {} already deleted", name);
        }

        debug!(%name, "Deleted");
        Ok(())
    }

    #[instrument(skip(self, nss))]
    pub(super) fn reset_ns(&mut self, nss: Vec<k8s::Namespace>) -> Result<()> {
        // Avoid rebuilding data for nodes that have not changed.
        let mut prior_names = self
            .namespaces
            .index
            .keys()
            .cloned()
            .collect::<HashSet<_>>();

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
}
