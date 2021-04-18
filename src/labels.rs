use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug, Eq, Default)]
pub struct Labels(Arc<Map>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selector(Arc<Expressions>);

pub type Map = BTreeMap<String, String>;

pub type Expressions = Vec<Expression>;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct Expression {
    key: String,
    operator: Operator,
    values: BTreeSet<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub enum Operator {
    In,
    NotIn,
}

/// Selects a set of pods that expose a server.
#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Match {
    match_labels: Option<Map>,
    match_expressions: Option<Expressions>,
}

// === Selector ===

impl From<Match> for Selector {
    fn from(m: Match) -> Self {
        let mut exprs = m.match_expressions.unwrap_or_default();

        if let Some(labels) = m.match_labels {
            for (key, v) in labels.into_iter() {
                let mut values = BTreeSet::new();
                values.insert(v);
                exprs.push(Expression {
                    key,
                    operator: Operator::In,
                    values,
                })
            }
        }

        Self(Arc::new(exprs))
    }
}

impl Selector {
    pub fn matches(&self, labels: &Labels) -> bool {
        for expr in self.0.iter() {
            if !expr.matches(labels.as_ref()) {
                return false;
            }
        }

        true
    }
}

// === Labels ===

impl From<Option<Map>> for Labels {
    #[inline]
    fn from(labels: Option<Map>) -> Self {
        Self(Arc::new(labels.unwrap_or_default()))
    }
}

impl AsRef<Map> for Labels {
    #[inline]
    fn as_ref(&self) -> &Map {
        self.0.as_ref()
    }
}

impl<T: AsRef<Map>> std::cmp::PartialEq<T> for Labels {
    #[inline]
    fn eq(&self, t: &T) -> bool {
        self.0.as_ref().eq(t.as_ref())
    }
}

// === Expression ===

impl Expression {
    fn matches(&self, labels: &Map) -> bool {
        match self.operator {
            Operator::In => {
                if let Some(v) = labels.get(&self.key) {
                    return self.values.contains(v);
                }
            }
            Operator::NotIn => {
                return match labels.get(&self.key) {
                    Some(v) => self.values.contains(v),
                    None => true,
                }
            }
        }

        false
    }
}
