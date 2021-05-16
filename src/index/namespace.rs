use super::{DefaultAllow, Index};
use crate::k8s;
use anyhow::{bail, Result};
use std::collections::HashSet;
use tracing::{debug, instrument, warn};

impl Index {
    #[instrument(
        skip(self, ns),
        fields(ns = ?ns.metadata.name)
    )]
    pub(super) fn apply_ns(&mut self, ns: k8s::Namespace) -> Result<()> {
        // Read the `default-allow` annotation from the ns metadata, which indicates the default
        // behavior for pod-ports in the namespace that lack a server instance.
        let allow = match DefaultAllow::from_annotation(&ns.metadata) {
            Ok(Some(mode)) => mode,
            Ok(None) => self.namespaces.default_allow,
            Err(error) => {
                warn!(%error, "invalid default-allow annotation");
                self.namespaces.default_allow
            }
        };

        // Get the cached namespace index (or load the default).
        let ns = self.namespaces.get_or_default(k8s::NsName::from_ns(&ns));
        ns.default_allow = allow;

        // We don't update the default-allow of running pods, as it is essentially bound to the pod
        // at inject-time.

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
