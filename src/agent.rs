use crate::domain::now_millis;
pub use crate::domain::AgentId;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AgentKind {
    Commander,
    WorkerA,
    WorkerB,
    WorkerA1,
    WorkerA2,
}

impl fmt::Display for AgentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Commander => "Commander",
            Self::WorkerA => "WorkerA",
            Self::WorkerB => "WorkerB",
            Self::WorkerA1 => "WorkerA1",
            Self::WorkerA2 => "WorkerA2",
        };
        f.write_str(label)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentDefinition {
    pub id: AgentId,
    pub name: String,
    pub kind: AgentKind,
    pub created_at: i64,
}

impl AgentDefinition {
    pub fn new(name: impl Into<String>, kind: AgentKind) -> Self {
        Self {
            id: AgentId::new(),
            name: name.into(),
            kind,
            created_at: now_millis(),
        }
    }
}
