//! Typed, model-agnostic boundary for bounded runtime decisions.
//!
//! Callers select context before constructing a request. Decision results are
//! proposals; Runtime and Scheduler invariants remain authoritative.

use crate::context::{ContextItem, ContextProvenance, ContextValue, RuntimeContext};
use crate::domain::{
    now_millis, DecisionRequestId, MemoryId, MissionId, RuntimeId, RuntimeState, Timestamp,
};
use crate::memory::{MemoryProvenance, MemoryScope, MemoryType};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq, Error, Serialize, Deserialize)]
#[serde(tag = "code", content = "detail", rename_all = "snake_case")]
pub enum DecisionError {
    #[error("invalid decision request: {0}")]
    InvalidRequest(String),
    #[error("decision request contains no valid options")]
    NoValidOptions,
    #[error("no decision engine is configured")]
    EngineUnavailable,
    #[error("decision engine timed out")]
    Timeout,
    #[error("decision engine failed: {0}")]
    InternalFailure(String),
    #[error("decision engine returned a malformed result: {0}")]
    MalformedResult(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DecisionOptionId(String);

impl DecisionOptionId {
    pub fn new(value: impl Into<String>) -> std::result::Result<Self, DecisionError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(DecisionError::InvalidRequest(
                "option IDs must not be empty".into(),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "custom_name", rename_all = "snake_case")]
pub enum DecisionType {
    TaskRouting,
    ExecutionStrategy,
    RuntimeLifecycle,
    ControlEscalation,
    FailureRecovery,
    Verification,
    ComplexityClassification,
    Custom(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DecisionOption {
    pub id: DecisionOptionId,
    pub description: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

impl DecisionOption {
    pub fn new(
        id: impl Into<String>,
        description: impl Into<String>,
    ) -> Result<Self, DecisionError> {
        Ok(Self {
            id: DecisionOptionId::new(id)?,
            description: description.into(),
            metadata: BTreeMap::new(),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DecisionRuntimeState {
    pub parent_runtime_id: Option<RuntimeId>,
    pub lifecycle_state: RuntimeState,
    pub objective: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DecisionContextFact {
    pub key: String,
    pub value: ContextValue,
    pub provenance: ContextProvenance,
    pub priority: Option<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DecisionMemory {
    pub memory_id: MemoryId,
    pub scope: MemoryScope,
    pub branch_id: Option<RuntimeId>,
    pub memory_type: MemoryType,
    pub content: String,
    pub provenance: MemoryProvenance,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct DecisionContext {
    pub facts: Vec<DecisionContextFact>,
    pub memories: Vec<DecisionMemory>,
}

impl DecisionContext {
    /// Copies only the requested facts and memories from a compiled context.
    /// No reference or complete `RuntimeContext` crosses the decision boundary.
    pub fn from_selected(
        context: &RuntimeContext,
        fact_keys: &[&str],
        memory_ids: &[MemoryId],
    ) -> Self {
        let facts = context
            .inherited_context
            .iter()
            .chain(&context.local_facts)
            .chain(&context.metadata)
            .filter(|item| fact_keys.contains(&item.key.as_str()))
            .map(DecisionContextFact::from)
            .collect();
        let memories = context
            .selected_memories
            .iter()
            .filter(|memory| memory_ids.contains(&memory.memory_id))
            .map(|memory| DecisionMemory {
                memory_id: memory.memory_id,
                scope: memory.scope,
                branch_id: memory.branch_id,
                memory_type: memory.memory_type,
                content: memory.content.clone(),
                provenance: memory.provenance.clone(),
            })
            .collect();
        Self { facts, memories }
    }
}

impl From<&ContextItem> for DecisionContextFact {
    fn from(item: &ContextItem) -> Self {
        Self {
            key: item.key.clone(),
            value: item.value.clone(),
            provenance: item.provenance.clone(),
            priority: item.priority,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionConstraintKind {
    Hard,
    Guidance,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionConstraint {
    pub kind: DecisionConstraintKind,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DecisionRequest {
    pub request_id: DecisionRequestId,
    pub mission_id: MissionId,
    pub runtime_id: RuntimeId,
    /// The top-level child runtime that owns this runtime's branch (or the root
    /// runtime itself for the root branch).
    pub branch_id: RuntimeId,
    pub decision_type: DecisionType,
    pub options: Vec<DecisionOption>,
    pub runtime_state: DecisionRuntimeState,
    pub context: DecisionContext,
    pub constraints: Vec<DecisionConstraint>,
    pub created_at: Timestamp,
}

impl DecisionRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mission_id: MissionId,
        runtime_id: RuntimeId,
        branch_id: RuntimeId,
        parent_runtime_id: Option<RuntimeId>,
        lifecycle_state: RuntimeState,
        objective: impl Into<String>,
        decision_type: DecisionType,
        options: Vec<DecisionOption>,
    ) -> Self {
        Self {
            request_id: DecisionRequestId::new(),
            mission_id,
            runtime_id,
            branch_id,
            decision_type,
            options,
            runtime_state: DecisionRuntimeState {
                parent_runtime_id,
                lifecycle_state,
                objective: objective.into(),
            },
            context: DecisionContext::default(),
            constraints: Vec::new(),
            created_at: now_millis(),
        }
    }

    pub fn with_context(mut self, context: DecisionContext) -> Self {
        self.context = context;
        self
    }

    pub fn with_constraints(mut self, constraints: Vec<DecisionConstraint>) -> Self {
        self.constraints = constraints;
        self
    }

    pub fn validate(&self) -> Result<(), DecisionError> {
        if matches!(&self.decision_type, DecisionType::Custom(name) if name.trim().is_empty()) {
            return Err(DecisionError::InvalidRequest(
                "custom decision types must have a name".into(),
            ));
        }
        if self.options.is_empty() {
            return Err(DecisionError::NoValidOptions);
        }
        let mut ids = HashSet::with_capacity(self.options.len());
        for option in &self.options {
            if option.id.0.trim().is_empty() || option.description.trim().is_empty() {
                return Err(DecisionError::InvalidRequest(
                    "each option needs a non-empty ID and description".into(),
                ));
            }
            if !ids.insert(&option.id) {
                return Err(DecisionError::InvalidRequest(format!(
                    "duplicate option ID: {}",
                    option.id.as_str()
                )));
            }
        }
        if self
            .constraints
            .iter()
            .any(|constraint| constraint.description.trim().is_empty())
        {
            return Err(DecisionError::InvalidRequest(
                "decision constraints must have a description".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecisionEngineProvenance {
    pub engine_id: String,
    pub version: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DecisionResult {
    pub request_id: DecisionRequestId,
    pub selected_option: DecisionOptionId,
    pub confidence: Option<f64>,
    pub engine: DecisionEngineProvenance,
    pub reasoning: Option<String>,
    pub latency_ms: Option<u64>,
}

impl DecisionResult {
    pub fn validate_for(&self, request: &DecisionRequest) -> Result<(), DecisionError> {
        if self.request_id != request.request_id {
            return Err(DecisionError::MalformedResult(
                "request ID does not match".into(),
            ));
        }
        if !request
            .options
            .iter()
            .any(|option| option.id == self.selected_option)
        {
            return Err(DecisionError::MalformedResult(
                "selected option was not supplied in the request".into(),
            ));
        }
        if self.engine.engine_id.trim().is_empty() {
            return Err(DecisionError::MalformedResult(
                "engine provenance needs a non-empty engine ID".into(),
            ));
        }
        if self
            .confidence
            .is_some_and(|confidence| !confidence.is_finite() || !(0.0..=1.0).contains(&confidence))
        {
            return Err(DecisionError::MalformedResult(
                "confidence must be between 0 and 1".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait DecisionEngine: Send + Sync {
    async fn decide(&self, request: DecisionRequest) -> Result<DecisionResult, DecisionError>;
}

/// Deterministic test implementation: select the first explicitly offered option.
#[derive(Clone, Copy, Debug, Default)]
pub struct MockDecisionEngine;

#[async_trait]
impl DecisionEngine for MockDecisionEngine {
    async fn decide(&self, request: DecisionRequest) -> Result<DecisionResult, DecisionError> {
        request.validate()?;
        Ok(DecisionResult {
            request_id: request.request_id,
            selected_option: request.options[0].id.clone(),
            confidence: None,
            engine: DecisionEngineProvenance {
                engine_id: "wyvern.mock".into(),
                version: Some("1".into()),
            },
            reasoning: Some("Selected the first option in request order.".into()),
            latency_ms: Some(0),
        })
    }
}
