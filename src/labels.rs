use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

#[derive(Clone, Debug, Eq, Default)]
pub struct Labels(Arc<Map>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selector {
    Map(Arc<Map>),
    Expr(Arc<Expressions>),
}

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

// === Selector ===

impl Selector {
    pub fn matches(&self, labels: &Labels) -> bool {
        match self {
            Self::Map(map) => labels.matches_map(map.as_ref()),
            Self::Expr(exprs) => labels.matches_exprs(exprs.as_ref()),
        }
    }
}

// === Labels ===

impl Labels {
    fn matches_map(&self, labels: &Map) -> bool {
        for (key, val) in labels.iter() {
            match self.0.get(key) {
                None => return false,
                Some(v) => {
                    if v != val {
                        return false;
                    }
                }
            }
        }
        true
    }

    fn matches_exprs(&self, exprs: &[Expression]) -> bool {
        for expr in exprs.iter() {
            if !expr.matches(&self.as_ref()) {
                return false;
            }
        }
        true
    }
}

impl From<Option<BTreeMap<String, String>>> for Labels {
    #[inline]
    fn from(labels: Option<BTreeMap<String, String>>) -> Self {
        Self(Arc::new(labels.unwrap_or_default()))
    }
}

impl AsRef<BTreeMap<String, String>> for Labels {
    #[inline]
    fn as_ref(&self) -> &BTreeMap<String, String> {
        self.0.as_ref()
    }
}

impl<T: AsRef<BTreeMap<String, String>>> std::cmp::PartialEq<T> for Labels {
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
