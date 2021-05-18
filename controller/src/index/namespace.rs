use super::{authz::AuthzIndex, pod::PodIndex, server::SrvIndex, DefaultAllow};
use crate::k8s::{self, ResourceExt};
use anyhow::{bail, Result};
use std::collections::{HashMap, HashSet};
use tracing::{debug, instrument, warn};

#[derive(Debug)]
pub(super) struct NamespaceIndex {
    pub index: HashMap<k8s::NsName, Namespace>,

    // The global default-allow policy.
    default_allow: DefaultAllow,
}

#[derive(Debug)]
pub(super) struct Namespace {
    /// Holds the per-namespace default-allow policy, possibly from an annotation override on the
    /// namespace object.
    pub default_allow: DefaultAllow,

    pub pods: PodIndex,
    pub servers: SrvIndex,
    pub authzs: AuthzIndex,
}

// === impl Namespaces ===

impl NamespaceIndex {
    pub fn new(default_allow: DefaultAllow) -> Self {
        Self {
            default_allow,
            index: HashMap::default(),
        }
    }

    pub fn get_or_default(&mut self, name: k8s::NsName) -> &mut Namespace {
        let default_allow = self.default_allow;
        self.index.entry(name).or_insert_with(|| Namespace {
            default_allow,
            pods: PodIndex::default(),
            servers: SrvIndex::default(),
            authzs: AuthzIndex::default(),
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = (&k8s::NsName, &Namespace)> {
        self.index.iter()
    }

    #[instrument(
        skip(self, ns),
        fields(ns = ?ns.metadata.name)
    )]
    pub(super) fn apply(&mut self, ns: k8s::Namespace) -> Result<()> {
        // Read the `default-allow` annotation from the ns metadata, which indicates the default
        // behavior for pod-ports in the namespace that lack a server instance.
        let allow = match DefaultAllow::from_annotation(&ns.metadata) {
            Ok(Some(mode)) => mode,
            Ok(None) => self.default_allow,
            Err(error) => {
                warn!(%error, "invalid default-allow annotation");
                self.default_allow
            }
        };

        // Get the cached namespace index (or load the default).
        let ns = self.get_or_default(k8s::NsName::from_ns(&ns));
        ns.default_allow = allow;

        // We don't update the default-allow of running pods, as it is essentially bound to the pod
        // at inject-time.

        Ok(())
    }

    #[instrument(
        skip(self, ns),
        fields(ns = ?ns.metadata.name)
    )]
    pub(super) fn delete(&mut self, ns: k8s::Namespace) -> Result<()> {
        let name = k8s::NsName::from_ns(&ns);
        self.rm(&name)
    }

    #[instrument(skip(self, nss))]
    pub(super) fn reset(&mut self, nss: Vec<k8s::Namespace>) -> Result<()> {
        // Avoid rebuilding data for nodes that have not changed.
        let mut prior_names = self.index.keys().cloned().collect::<HashSet<_>>();

        let mut result = Ok(());
        for ns in nss.into_iter() {
            prior_names.remove(ns.name().as_str());
            if let Err(error) = self.apply(ns) {
                warn!(%error, "Failed to apply namespace");
                result = Err(error);
            }
        }

        for name in prior_names.into_iter() {
            debug!(?name, "Removing defunct namespace");
            if let Err(error) = self.rm(&name) {
                result = Err(error);
            }
        }

        result
    }

    fn rm(&mut self, name: &k8s::NsName) -> Result<()> {
        if self.index.remove(name).is_none() {
            bail!("node {} already deleted", name);
        }

        debug!(%name, "Deleted");
        Ok(())
    }
}
