//! In-process bindings between Agent identities and replaceable runtime behavior.

pub mod demo;

use crate::agent::{AgentDefinition, AgentId, AgentKind};
use crate::context::RuntimeContext;
use crate::decision::{
    DecisionApplication, DecisionConstraint, DecisionContext, DecisionOption, DecisionResult,
    DecisionType,
};
use crate::domain::{RuntimeId, RuntimeState};
use crate::error::Result;
use crate::kernel::{Runtime, RuntimeHandle};
use crate::result::RuntimeResult;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{watch, RwLock};

#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum BehaviorError {
    #[error("no behavior is registered for agent {0}")]
    NotFound(AgentId),
    #[error("a behavior is already registered for agent {0}")]
    DuplicateAgent(AgentId),
    #[error("a default behavior is already registered for agent kind {0}")]
    DuplicateKind(AgentKind),
    #[error("agent is already registered: {0}")]
    AgentAlreadyRegistered(AgentId),
}

#[async_trait]
pub trait RuntimeBehavior: Send + Sync {
    async fn execute(&self, execution: RuntimeExecutionContext) -> Result<RuntimeResult>;
}

/// Resolves concrete behavior by AgentId. Optional kind defaults are copied to
/// an AgentId binding when that agent is registered; execution never falls
/// back to a kind or an implicit default.
#[derive(Default)]
pub struct RuntimeBehaviorRegistry {
    by_agent: RwLock<HashMap<AgentId, Arc<dyn RuntimeBehavior>>>,
    kind_defaults: RwLock<HashMap<AgentKind, Arc<dyn RuntimeBehavior>>>,
}

impl RuntimeBehaviorRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn register_for_agent(
        &self,
        agent_id: AgentId,
        behavior: Arc<dyn RuntimeBehavior>,
    ) -> std::result::Result<(), BehaviorError> {
        let mut bindings = self.by_agent.write().await;
        if bindings.contains_key(&agent_id) {
            return Err(BehaviorError::DuplicateAgent(agent_id));
        }
        bindings.insert(agent_id, behavior);
        Ok(())
    }

    pub async fn register_kind_default(
        &self,
        kind: AgentKind,
        behavior: Arc<dyn RuntimeBehavior>,
    ) -> std::result::Result<(), BehaviorError> {
        let mut defaults = self.kind_defaults.write().await;
        if defaults.contains_key(&kind) {
            return Err(BehaviorError::DuplicateKind(kind));
        }
        defaults.insert(kind, behavior);
        Ok(())
    }

    pub async fn bind_agent_default(&self, agent_id: AgentId, kind: AgentKind) {
        let Some(behavior) = self.kind_defaults.read().await.get(&kind).cloned() else {
            return;
        };
        self.by_agent
            .write()
            .await
            .entry(agent_id)
            .or_insert(behavior);
    }

    pub async fn resolve(
        &self,
        agent: &AgentDefinition,
    ) -> std::result::Result<Arc<dyn RuntimeBehavior>, BehaviorError> {
        self.by_agent
            .read()
            .await
            .get(&agent.id)
            .cloned()
            .ok_or(BehaviorError::NotFound(agent.id))
    }

    pub async fn is_bound(&self, agent_id: AgentId) -> bool {
        self.by_agent.read().await.contains_key(&agent_id)
    }
}

#[derive(Clone)]
pub struct BehaviorRuntimeHandle {
    runtime: RuntimeHandle,
}

impl BehaviorRuntimeHandle {
    pub(crate) fn new(runtime: RuntimeHandle) -> Self {
        Self { runtime }
    }

    pub fn id(&self) -> RuntimeId {
        self.runtime.id()
    }

    pub async fn inspect(&self) -> Result<Runtime> {
        self.runtime.inspect().await
    }

    pub async fn agent_by_kind(&self, kind: AgentKind) -> Result<AgentDefinition> {
        self.runtime.agent_by_kind(kind).await
    }

    pub async fn spawn_child(
        &self,
        agent: &AgentDefinition,
        objective: impl Into<String>,
    ) -> Result<Self> {
        self.runtime
            .spawn_child(agent, objective)
            .await
            .map(Self::new)
    }

    /// Requests the only non-terminal lifecycle operation demo behaviors need.
    pub async fn wait(&self) -> Result<Runtime> {
        self.runtime.transition_to(RuntimeState::Waiting).await
    }

    pub async fn decide(
        &self,
        decision_type: DecisionType,
        options: Vec<DecisionOption>,
        context: DecisionContext,
        constraints: Vec<DecisionConstraint>,
    ) -> Result<DecisionResult> {
        self.runtime
            .decide(decision_type, options, context, constraints)
            .await
    }

    pub async fn decide_and_apply(
        &self,
        context: DecisionContext,
        constraints: Vec<DecisionConstraint>,
    ) -> Result<DecisionApplication> {
        self.runtime.decide_and_apply(context, constraints).await
    }
}

#[async_trait]
pub(crate) trait ChildRuntimeRunner: Send + Sync {
    async fn run_child(
        &self,
        parent_id: RuntimeId,
        child_id: RuntimeId,
        hooks: demo::RuntimeHooks,
    ) -> Result<RuntimeResult>;
}

#[derive(Clone)]
pub struct RuntimeExecutionContext {
    pub runtime: BehaviorRuntimeHandle,
    pub agent: AgentDefinition,
    /// This is the context owned by this runtime. No parent/sibling lookup is
    /// done by behavior resolution or execution.
    pub context: RuntimeContext,
    pub cancellation: watch::Receiver<bool>,
    child_runner: Arc<dyn ChildRuntimeRunner>,
    hooks: demo::RuntimeHooks,
}

impl RuntimeExecutionContext {
    pub(crate) fn new(
        runtime: BehaviorRuntimeHandle,
        agent: AgentDefinition,
        context: RuntimeContext,
        cancellation: watch::Receiver<bool>,
        child_runner: Arc<dyn ChildRuntimeRunner>,
        hooks: demo::RuntimeHooks,
    ) -> Self {
        Self {
            runtime,
            agent,
            context,
            cancellation,
            child_runner,
            hooks,
        }
    }

    pub async fn run_child(&self, child: &BehaviorRuntimeHandle) -> Result<RuntimeResult> {
        self.child_runner
            .run_child(self.runtime.id(), child.id(), self.hooks.clone())
            .await
    }

    pub(crate) fn demo_hooks(&self) -> &demo::RuntimeHooks {
        &self.hooks
    }
}
