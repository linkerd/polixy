use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type Map = HashMap<String, String>;

pub type Expressions = Vec<Expression>;

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub struct Expression {
    key: String,
    operator: Operator,
    values: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema)]
pub enum Operator {
    In,
    NotIn,
}
