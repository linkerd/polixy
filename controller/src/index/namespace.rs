use super::{authz::AuthzIndex, pod::PodIndex, server::SrvIndex, DefaultAllow};
use crate::k8s;
use std::collections::HashMap;

#[derive(Debug)]
pub(super) struct NamespaceIndex {
    pub index: HashMap<k8s::NsName, Namespace>,

    // The global default-allow policy.
    default_allow: DefaultAllow,
}

#[derive(Debug)]
pub(super) struct Namespace {
    /// Holds the global default-allow policy, which may be overridden per-workload.
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
}
