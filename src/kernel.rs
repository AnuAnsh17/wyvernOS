use crate::agent::{AgentDefinition, AgentKind};
use crate::capability::{
    CapabilityCancellation, CapabilityPermission, CapabilityRegistry, CapabilityRequest,
    CapabilityResult,
};
use crate::context::{
    ContextItem, ContextProvenance, ContextSourceType, ContextStore, RuntimeContext,
    RuntimeStateView, StructuredChildOutput,
};
use crate::decision::{
    AppliedDecision, DecisionApplication, DecisionConstraint, DecisionContext, DecisionEngine,
    DecisionError, DecisionOption, DecisionRequest, DecisionResult, DecisionType,
};
use crate::domain::{now_millis, Mission, MissionId, MissionState, RuntimeId, RuntimeState};
use crate::error::{Result, WyvernError};
use crate::events::{EventBus, RuntimeEvent};
use crate::memory::{MemoryDraft, MemoryItem, MemoryStore};
use crate::persistence::{RuntimeStore, SqliteRuntimeStore};
use crate::result::RuntimeResult;
use async_recursion::async_recursion;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{watch, Barrier, Mutex, Notify, RwLock};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Runtime {
    pub id: RuntimeId,
    pub mission_id: MissionId,
    pub agent_id: crate::agent::AgentId,
    pub parent_id: Option<RuntimeId>,
    pub objective: String,
    pub state: RuntimeState,
    pub created_at: i64,
    pub updated_at: i64,
    pub children: Vec<RuntimeId>,
    pub result: Option<RuntimeResult>,
}

impl Runtime {
    pub fn new(
        mission_id: MissionId,
        agent_id: crate::agent::AgentId,
        parent_id: Option<RuntimeId>,
        objective: impl Into<String>,
    ) -> Self {
        let now = now_millis();
        Self {
            id: RuntimeId::new(),
            mission_id,
            agent_id,
            parent_id,
            objective: objective.into(),
            state: RuntimeState::Created,
            created_at: now,
            updated_at: now,
            children: Vec::new(),
            result: None,
        }
    }

    pub fn transition_to(&mut self, next: RuntimeState) -> Result<()> {
        if !self.state.can_transition_to(next) {
            return Err(WyvernError::InvalidStateTransition {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        self.updated_at = now_millis().max(self.updated_at.saturating_add(1));
        Ok(())
    }

    pub fn set_result(&mut self, result: RuntimeResult) {
        self.result = Some(result);
        self.updated_at = now_millis().max(self.updated_at.saturating_add(1));
    }
}

#[derive(Default)]
pub struct RuntimeRegistry {
    runtimes: RwLock<HashMap<RuntimeId, Runtime>>,
}

impl RuntimeRegistry {
    pub async fn insert(&self, runtime: Runtime) {
        self.runtimes.write().await.insert(runtime.id, runtime);
    }

    pub async fn get(&self, runtime_id: RuntimeId) -> Option<Runtime> {
        self.runtimes.read().await.get(&runtime_id).cloned()
    }

    pub async fn update(&self, runtime: Runtime) {
        self.runtimes.write().await.insert(runtime.id, runtime);
    }

    pub async fn list(&self) -> Vec<Runtime> {
        self.runtimes.read().await.values().cloned().collect()
    }

    pub async fn by_mission(&self, mission_id: MissionId) -> Vec<Runtime> {
        self.runtimes
            .read()
            .await
            .values()
            .filter(|runtime| runtime.mission_id == mission_id)
            .cloned()
            .collect()
    }

    pub async fn by_state(&self, state: RuntimeState) -> Vec<Runtime> {
        self.runtimes
            .read()
            .await
            .values()
            .filter(|runtime| runtime.state == state)
            .cloned()
            .collect()
    }

    pub async fn children_of(&self, runtime_id: RuntimeId) -> Vec<Runtime> {
        let runtimes = self.runtimes.read().await;
        let Some(parent) = runtimes.get(&runtime_id) else {
            return Vec::new();
        };
        parent
            .children
            .iter()
            .filter_map(|child_id| runtimes.get(child_id).cloned())
            .collect()
    }

    pub async fn parent_of(&self, runtime_id: RuntimeId) -> Option<Runtime> {
        let runtimes = self.runtimes.read().await;
        let parent_id = runtimes.get(&runtime_id)?.parent_id?;
        runtimes.get(&parent_id).cloned()
    }

    pub async fn add_child(&self, parent_id: RuntimeId, child_id: RuntimeId) -> Result<()> {
        let mut runtimes = self.runtimes.write().await;
        let Some(parent) = runtimes.get_mut(&parent_id) else {
            return Err(WyvernError::RuntimeNotFound(parent_id));
        };
        if !parent.children.contains(&child_id) {
            parent.children.push(child_id);
            parent.updated_at = now_millis().max(parent.updated_at.saturating_add(1));
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct MissionRegistry {
    missions: RwLock<HashMap<MissionId, Mission>>,
}

impl MissionRegistry {
    pub async fn insert(&self, mission: Mission) {
        self.missions.write().await.insert(mission.id, mission);
    }

    pub async fn get(&self, mission_id: MissionId) -> Option<Mission> {
        self.missions.read().await.get(&mission_id).cloned()
    }

    pub async fn update(&self, mission: Mission) {
        self.missions.write().await.insert(mission.id, mission);
    }

    pub async fn list(&self) -> Vec<Mission> {
        self.missions.read().await.values().cloned().collect()
    }
}

#[derive(Default)]
pub struct AgentRegistry {
    by_id: RwLock<HashMap<crate::agent::AgentId, AgentDefinition>>,
    by_kind: RwLock<HashMap<AgentKind, crate::agent::AgentId>>,
}

impl AgentRegistry {
    pub async fn register(&self, agent: AgentDefinition) -> AgentDefinition {
        self.by_kind.write().await.insert(agent.kind, agent.id);
        self.by_id.write().await.insert(agent.id, agent.clone());
        agent
    }

    pub async fn get(&self, agent_id: crate::agent::AgentId) -> Option<AgentDefinition> {
        self.by_id.read().await.get(&agent_id).cloned()
    }

    pub async fn get_by_kind(&self, kind: AgentKind) -> Option<AgentDefinition> {
        let agent_id = self.by_kind.read().await.get(&kind).copied()?;
        self.get(agent_id).await
    }

    pub async fn list(&self) -> Vec<AgentDefinition> {
        self.by_id.read().await.values().cloned().collect()
    }
}

#[derive(Clone)]
pub struct Kernel {
    inner: Arc<KernelInner>,
}

struct KernelInner {
    pub agents: AgentRegistry,
    pub missions: MissionRegistry,
    pub runtimes: RuntimeRegistry,
    pub events: EventBus,
    pub capabilities: CapabilityRegistry,
    pub store: Arc<dyn RuntimeStore>,
    pub context_store: ContextStore,
    pub memory_store: MemoryStore,
    pub decision_engine: Option<Arc<dyn DecisionEngine>>,
    pub controls: RwLock<HashMap<RuntimeId, watch::Sender<bool>>>,
    lifecycle_locks: RwLock<HashMap<RuntimeId, Arc<Mutex<()>>>>,
}

impl Kernel {
    pub async fn new(store: Arc<dyn RuntimeStore>) -> Result<Self> {
        let context_store = ContextStore::in_memory().await?;
        let events = EventBus::new(1024);
        let memory_store = MemoryStore::in_memory(events.clone()).await?;
        Self::new_with_stores(store, context_store, memory_store, events, None).await
    }

    pub async fn new_with_decision_engine(
        store: Arc<dyn RuntimeStore>,
        decision_engine: Arc<dyn DecisionEngine>,
    ) -> Result<Self> {
        let context_store = ContextStore::in_memory().await?;
        let events = EventBus::new(1024);
        let memory_store = MemoryStore::in_memory(events.clone()).await?;
        Self::new_with_stores(
            store,
            context_store,
            memory_store,
            events,
            Some(decision_engine),
        )
        .await
    }

    async fn new_with_stores(
        store: Arc<dyn RuntimeStore>,
        context_store: ContextStore,
        memory_store: MemoryStore,
        events: EventBus,
        decision_engine: Option<Arc<dyn DecisionEngine>>,
    ) -> Result<Self> {
        let capabilities = CapabilityRegistry::new(events.clone());
        let kernel = Self {
            inner: Arc::new(KernelInner {
                agents: AgentRegistry::default(),
                missions: MissionRegistry::default(),
                runtimes: RuntimeRegistry::default(),
                events,
                capabilities,
                store,
                context_store,
                memory_store,
                decision_engine,
                controls: RwLock::new(HashMap::new()),
                lifecycle_locks: RwLock::new(HashMap::new()),
            }),
        };
        kernel.hydrate().await?;
        Ok(kernel)
    }

    pub async fn new_sqlite(path: impl AsRef<Path>) -> Result<Self> {
        let store = SqliteRuntimeStore::open(&path).await?;
        let context_store = ContextStore::open(&path).await?;
        let events = EventBus::new(1024);
        let memory_store = MemoryStore::open(&path, events.clone()).await?;
        Self::new_with_stores(Arc::new(store), context_store, memory_store, events, None).await
    }

    pub async fn new_sqlite_with_decision_engine(
        path: impl AsRef<Path>,
        decision_engine: Arc<dyn DecisionEngine>,
    ) -> Result<Self> {
        let store = SqliteRuntimeStore::open(&path).await?;
        let context_store = ContextStore::open(&path).await?;
        let events = EventBus::new(1024);
        let memory_store = MemoryStore::open(&path, events.clone()).await?;
        Self::new_with_stores(
            Arc::new(store),
            context_store,
            memory_store,
            events,
            Some(decision_engine),
        )
        .await
    }

    pub fn contexts(&self) -> ContextStore {
        self.inner.context_store.clone()
    }

    pub fn memories(&self) -> MemoryStore {
        self.inner.memory_store.clone()
    }

    pub fn capabilities(&self) -> CapabilityRegistry {
        self.inner.capabilities.clone()
    }

    /// Builds a bounded decision request from runtime state and caller-selected
    /// context. The entire runtime context is never passed implicitly.
    pub async fn build_decision_request(
        &self,
        runtime_id: RuntimeId,
        decision_type: DecisionType,
        options: Vec<crate::decision::DecisionOption>,
        context: DecisionContext,
        constraints: Vec<DecisionConstraint>,
    ) -> Result<DecisionRequest> {
        let runtime = self.get_runtime(runtime_id).await?;
        let branch_id = self.branch_id(runtime_id).await?;
        Ok(DecisionRequest::new(
            runtime.mission_id,
            runtime.id,
            branch_id,
            runtime.parent_id,
            runtime.state,
            runtime.objective,
            decision_type,
            options,
        )
        .with_runtime_updated_at(runtime.updated_at)
        .with_context(context)
        .with_constraints(constraints))
    }

    /// Dispatches a bounded decision through the injected model-agnostic
    /// boundary. The selected option is validated against the supplied options.
    pub async fn decide(&self, request: DecisionRequest) -> Result<DecisionResult> {
        request.validate()?;
        let runtime = self.get_runtime(request.runtime_id).await?;
        let expected_branch_id = self.branch_id(runtime.id).await?;
        if runtime.mission_id != request.mission_id
            || runtime.parent_id != request.runtime_state.parent_runtime_id
            || runtime.objective != request.runtime_state.objective
            || expected_branch_id != request.branch_id
        {
            return Err(crate::decision::DecisionError::InvalidRequest(
                "runtime, mission, branch, or state does not match the current runtime".into(),
            )
            .into());
        }

        if runtime.state != request.runtime_state.lifecycle_state
            || runtime.updated_at != request.runtime_updated_at
        {
            self.publish(RuntimeEvent::DecisionRequested {
                mission_id: runtime.mission_id,
                runtime_id: runtime.id,
                parent_id: runtime.parent_id,
                request_id: request.request_id,
                decision_type: request.decision_type.clone(),
                timestamp: now_millis(),
            })?;
            let error = DecisionError::Stale(format!(
                "runtime {} changed before decision request {} was dispatched",
                runtime.id, request.request_id
            ));
            self.publish(RuntimeEvent::DecisionStale {
                mission_id: runtime.mission_id,
                runtime_id: runtime.id,
                parent_id: runtime.parent_id,
                request_id: request.request_id,
                error: error.clone(),
                timestamp: now_millis(),
            })?;
            return Err(error.into());
        }

        self.publish(RuntimeEvent::DecisionRequested {
            mission_id: runtime.mission_id,
            runtime_id: runtime.id,
            parent_id: runtime.parent_id,
            request_id: request.request_id,
            decision_type: request.decision_type.clone(),
            timestamp: now_millis(),
        })?;

        let Some(engine) = self.inner.decision_engine.as_ref() else {
            let error = crate::decision::DecisionError::EngineUnavailable;
            self.publish(RuntimeEvent::DecisionFailed {
                mission_id: runtime.mission_id,
                runtime_id: runtime.id,
                parent_id: runtime.parent_id,
                request_id: request.request_id,
                error: error.clone(),
                timestamp: now_millis(),
            })?;
            return Err(error.into());
        };

        let result = match engine.decide(request.clone()).await {
            Ok(result) => result,
            Err(error) => {
                self.publish(RuntimeEvent::DecisionFailed {
                    mission_id: runtime.mission_id,
                    runtime_id: runtime.id,
                    parent_id: runtime.parent_id,
                    request_id: request.request_id,
                    error: error.clone(),
                    timestamp: now_millis(),
                })?;
                return Err(error.into());
            }
        };

        if let Err(error) = result.validate_for(&request) {
            self.publish(RuntimeEvent::DecisionFailed {
                mission_id: runtime.mission_id,
                runtime_id: runtime.id,
                parent_id: runtime.parent_id,
                request_id: request.request_id,
                error: error.clone(),
                timestamp: now_millis(),
            })?;
            return Err(error.into());
        }

        self.publish(RuntimeEvent::DecisionCompleted {
            mission_id: runtime.mission_id,
            runtime_id: runtime.id,
            parent_id: runtime.parent_id,
            request_id: result.request_id,
            selected_option: result.selected_option.clone(),
            engine: result.engine.clone(),
            timestamp: now_millis(),
        })?;
        Ok(result)
    }

    /// Applies an already validated lifecycle proposal through the kernel's
    /// versioned runtime transition path.
    pub async fn apply_lifecycle_decision(
        &self,
        request: &DecisionRequest,
        result: &DecisionResult,
    ) -> Result<AppliedDecision> {
        request.validate()?;
        if let Err(error) = result.validate_for(request) {
            self.publish(RuntimeEvent::DecisionRejected {
                mission_id: request.mission_id,
                runtime_id: request.runtime_id,
                parent_id: request.runtime_state.parent_runtime_id,
                request_id: request.request_id,
                selected_option: result.selected_option.clone(),
                error: error.clone(),
                timestamp: now_millis(),
            })?;
            return Err(error.into());
        }
        if request.decision_type != DecisionType::RuntimeLifecycle {
            return self
                .reject_lifecycle(
                    request,
                    result,
                    "only RuntimeLifecycle decisions can be applied".into(),
                )
                .await;
        }
        let (next, no_op) = match result.selected_option.as_str() {
            "continue" => match request.runtime_state.lifecycle_state {
                RuntimeState::Ready | RuntimeState::Waiting => (RuntimeState::Running, false),
                RuntimeState::Running => (RuntimeState::Running, true),
                state => {
                    return self
                        .reject_lifecycle(
                            request,
                            result,
                            format!("continue is not valid from {state}"),
                        )
                        .await
                }
            },
            "wait" => (RuntimeState::Waiting, false),
            "complete" => (RuntimeState::Completed, false),
            "fail" => (RuntimeState::Failed, false),
            option => {
                return self
                    .reject_lifecycle(
                        request,
                        result,
                        format!("unsupported lifecycle option: {option}"),
                    )
                    .await
            }
        };
        if !no_op
            && !request
                .runtime_state
                .lifecycle_state
                .can_transition_to(next)
        {
            return self
                .reject_lifecycle(
                    request,
                    result,
                    format!(
                        "transition from {} to {next} is not allowed",
                        request.runtime_state.lifecycle_state
                    ),
                )
                .await;
        }

        let _guard = self
            .lifecycle_lock(request.runtime_id)
            .await
            .lock_owned()
            .await;
        let runtime = self.get_runtime(request.runtime_id).await?;
        if runtime.mission_id != request.mission_id
            || runtime.parent_id != request.runtime_state.parent_runtime_id
            || runtime.objective != request.runtime_state.objective
            || runtime.state != request.runtime_state.lifecycle_state
            || runtime.updated_at != request.runtime_updated_at
            || self.branch_id(runtime.id).await? != request.branch_id
        {
            let error = DecisionError::Stale(format!(
                "runtime {} changed after request {} was created",
                request.runtime_id, request.request_id
            ));
            self.publish(RuntimeEvent::DecisionStale {
                mission_id: request.mission_id,
                runtime_id: request.runtime_id,
                parent_id: request.runtime_state.parent_runtime_id,
                request_id: request.request_id,
                error: error.clone(),
                timestamp: now_millis(),
            })?;
            return Err(error.into());
        }
        let timestamp = now_millis();
        let resulting_state = if no_op {
            runtime.state
        } else {
            let updated = match self
                .transition_runtime_locked(request.runtime_id, next)
                .await
            {
                Ok(updated) => updated,
                Err(WyvernError::Decision(error @ DecisionError::Stale(_))) => {
                    self.publish(RuntimeEvent::DecisionStale {
                        mission_id: request.mission_id,
                        runtime_id: request.runtime_id,
                        parent_id: request.runtime_state.parent_runtime_id,
                        request_id: request.request_id,
                        error: error.clone(),
                        timestamp: now_millis(),
                    })?;
                    return Err(error.into());
                }
                Err(error) => return Err(error),
            };
            self.publish(RuntimeEvent::DecisionAccepted {
                mission_id: request.mission_id,
                runtime_id: request.runtime_id,
                parent_id: request.runtime_state.parent_runtime_id,
                request_id: request.request_id,
                selected_option: result.selected_option.clone(),
                from: runtime.state,
                to: next,
                changed_state: true,
                timestamp,
            })?;
            self.publish(RuntimeEvent::RuntimeStateChanged {
                mission_id: updated.mission_id,
                runtime_id: updated.id,
                parent_id: updated.parent_id,
                from: runtime.state,
                to: next,
                timestamp,
            })?;
            next
        };
        let applied = AppliedDecision {
            request_id: request.request_id,
            runtime_id: request.runtime_id,
            selected_option: result.selected_option.clone(),
            previous_state: runtime.state,
            resulting_state,
            applied_at: timestamp,
            changed_state: !no_op,
        };
        if no_op {
            self.publish(RuntimeEvent::DecisionAccepted {
                mission_id: request.mission_id,
                runtime_id: request.runtime_id,
                parent_id: request.runtime_state.parent_runtime_id,
                request_id: request.request_id,
                selected_option: result.selected_option.clone(),
                from: runtime.state,
                to: resulting_state,
                changed_state: !no_op,
                timestamp,
            })?;
        }
        Ok(applied)
    }

    async fn reject_lifecycle<T>(
        &self,
        request: &DecisionRequest,
        result: &DecisionResult,
        reason: String,
    ) -> Result<T> {
        let error = DecisionError::Rejected(reason);
        self.publish(RuntimeEvent::DecisionRejected {
            mission_id: request.mission_id,
            runtime_id: request.runtime_id,
            parent_id: request.runtime_state.parent_runtime_id,
            request_id: request.request_id,
            selected_option: result.selected_option.clone(),
            error: error.clone(),
            timestamp: now_millis(),
        })?;
        Err(error.into())
    }

    async fn lifecycle_lock(&self, runtime_id: RuntimeId) -> Arc<Mutex<()>> {
        if let Some(lock) = self
            .inner
            .lifecycle_locks
            .read()
            .await
            .get(&runtime_id)
            .cloned()
        {
            return lock;
        }
        let mut locks = self.inner.lifecycle_locks.write().await;
        locks
            .entry(runtime_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn branch_id(&self, runtime_id: RuntimeId) -> Result<RuntimeId> {
        let mut current = self.get_runtime(runtime_id).await?;
        loop {
            let Some(parent_id) = current.parent_id else {
                return Ok(current.id);
            };
            let parent = self.get_runtime(parent_id).await?;
            if parent.parent_id.is_none() {
                return Ok(current.id);
            }
            current = parent;
        }
    }

    /// Dispatches a request only after confirming its runtime/mission association.
    pub async fn execute_capability(
        &self,
        mut request: CapabilityRequest,
        permission: &CapabilityPermission,
        cancellation: &CapabilityCancellation,
    ) -> Result<CapabilityResult> {
        let runtime = self.get_runtime(request.runtime_id).await?;
        if runtime.mission_id != request.mission_id {
            return Err(WyvernError::ExecutionError(
                "capability request mission does not match its runtime".into(),
            ));
        }
        request.parent_runtime_id = runtime.parent_id;
        Ok(self
            .inner
            .capabilities
            .execute(request, permission, cancellation)
            .await)
    }

    pub async fn create_memory(&self, draft: MemoryDraft) -> Result<MemoryItem> {
        self.validate_memory_draft(&draft).await?;
        self.inner.memory_store.create(draft).await
    }

    pub async fn supersede_memory(
        &self,
        memory_id: crate::domain::MemoryId,
        draft: MemoryDraft,
    ) -> Result<MemoryItem> {
        self.validate_memory_draft(&draft).await?;
        self.inner.memory_store.supersede(memory_id, draft).await
    }

    pub async fn promote_memory(
        &self,
        memory_id: crate::domain::MemoryId,
        target_runtime: RuntimeId,
    ) -> Result<MemoryItem> {
        let source = self
            .inner
            .memory_store
            .get(memory_id)
            .await?
            .ok_or(WyvernError::MemoryNotFound(memory_id))?;
        let target = self.get_runtime(target_runtime).await?;
        if source.mission_id != target.mission_id {
            return Err(WyvernError::ExecutionError(
                "promoted memory target belongs to another mission".into(),
            ));
        }
        self.inner
            .memory_store
            .promote(memory_id, target_runtime)
            .await
    }

    async fn validate_memory_draft(&self, draft: &MemoryDraft) -> Result<()> {
        self.get_mission(draft.mission_id).await?;
        let source_runtime_id = draft.provenance.source_runtime_id.ok_or_else(|| {
            WyvernError::ExecutionError("memory provenance requires a source runtime".into())
        })?;
        let source = self.get_runtime(source_runtime_id).await?;
        if source.mission_id != draft.mission_id {
            return Err(WyvernError::ExecutionError(
                "memory provenance runtime belongs to another mission".into(),
            ));
        }
        if draft
            .provenance
            .source_agent_id
            .is_some_and(|agent_id| agent_id != source.agent_id)
        {
            return Err(WyvernError::ExecutionError(
                "memory provenance agent does not match its source runtime".into(),
            ));
        }
        if let Some(runtime_id) = draft.runtime_id {
            if self.get_runtime(runtime_id).await?.mission_id != draft.mission_id {
                return Err(WyvernError::ExecutionError(
                    "runtime-scoped memory owner belongs to another mission".into(),
                ));
            }
        }
        if let Some(branch_id) = draft.branch_id {
            if self.get_runtime(branch_id).await?.mission_id != draft.mission_id {
                return Err(WyvernError::ExecutionError(
                    "branch-scoped memory owner belongs to another mission".into(),
                ));
            }
        }
        if let Some(agent_id) = draft.agent_id {
            if self.inner.agents.get(agent_id).await.is_none() {
                return Err(WyvernError::ExecutionError(format!(
                    "agent-scoped memory owner is not registered: {agent_id}"
                )));
            }
        }
        Ok(())
    }

    pub async fn visible_memories(&self, runtime_id: RuntimeId) -> Result<Vec<MemoryItem>> {
        let runtime = self.get_runtime(runtime_id).await?;
        let mut ancestors = vec![runtime_id];
        let mut parent_id = runtime.parent_id;
        while let Some(id) = parent_id {
            ancestors.push(id);
            parent_id = self.get_runtime(id).await?.parent_id;
        }
        self.inner
            .memory_store
            .visible_to_runtime(runtime.mission_id, runtime_id, runtime.agent_id, &ancestors)
            .await
    }

    pub async fn add_context_fact(
        &self,
        runtime_id: RuntimeId,
        key: impl Into<String>,
        value: crate::context::ContextValue,
    ) -> Result<RuntimeContext> {
        let runtime = self.get_runtime(runtime_id).await?;
        let key = key.into();
        let item = ContextItem {
            key: key.clone(),
            value,
            provenance: ContextProvenance {
                source_type: ContextSourceType::LocalContext,
                source_runtime_id: Some(runtime_id),
                source_event_id: None,
                created_at: crate::domain::now_millis(),
            },
            priority: None,
        };
        let context = self
            .inner
            .context_store
            .update(runtime_id, |context| context.local_facts.push(item.clone()))
            .await?;
        self.publish(RuntimeEvent::ContextUpdated {
            mission_id: runtime.mission_id,
            runtime_id,
            parent_id: runtime.parent_id,
            keys: vec![key],
            timestamp: crate::domain::now_millis(),
        })?;
        Ok(context)
    }

    pub async fn context(&self, runtime_id: RuntimeId) -> Result<RuntimeContext> {
        self.inner
            .context_store
            .get(runtime_id)
            .await?
            .ok_or(WyvernError::RuntimeNotFound(runtime_id))
    }

    /// Replaces only this runtime's context with a deterministic projection.
    pub async fn compile_context(
        &self,
        runtime_id: RuntimeId,
        inherited_context: Vec<ContextItem>,
        relevant_events: Vec<crate::events::EventEnvelope>,
        child_outputs: Vec<StructuredChildOutput>,
        constraints: Vec<String>,
    ) -> Result<RuntimeContext> {
        self.compile_context_with_memory(
            runtime_id,
            inherited_context,
            relevant_events,
            child_outputs,
            constraints,
            Vec::new(),
        )
        .await
    }

    /// Compiles an explicit memory selection. The compiler never queries storage.
    pub async fn compile_context_with_memory(
        &self,
        runtime_id: RuntimeId,
        inherited_context: Vec<ContextItem>,
        relevant_events: Vec<crate::events::EventEnvelope>,
        child_outputs: Vec<StructuredChildOutput>,
        constraints: Vec<String>,
        selected_memories: Vec<MemoryItem>,
    ) -> Result<RuntimeContext> {
        self.compile_context_with_capability_results(
            runtime_id,
            inherited_context,
            relevant_events,
            child_outputs,
            constraints,
            selected_memories,
            Vec::new(),
        )
        .await
    }

    /// Compiles only the capability results explicitly selected by the caller.
    pub async fn compile_context_with_capability_results(
        &self,
        runtime_id: RuntimeId,
        inherited_context: Vec<ContextItem>,
        relevant_events: Vec<crate::events::EventEnvelope>,
        child_outputs: Vec<StructuredChildOutput>,
        constraints: Vec<String>,
        selected_memories: Vec<MemoryItem>,
        capability_results: Vec<CapabilityResult>,
    ) -> Result<RuntimeContext> {
        let runtime = self.get_runtime(runtime_id).await?;
        let context =
            crate::context::ContextCompiler::compile(crate::context::ContextCompileInput {
                runtime: Some(RuntimeStateView {
                    runtime_id,
                    mission_id: runtime.mission_id,
                    agent_id: runtime.agent_id,
                    parent_id: runtime.parent_id,
                    lifecycle_state: runtime.state,
                    objective: runtime.objective.clone(),
                    created_at: runtime.created_at,
                    updated_at: runtime.updated_at,
                    result: runtime.result.clone(),
                }),
                objective: runtime.objective,
                inherited_context,
                local_facts: self.context(runtime_id).await?.local_facts,
                relevant_events,
                child_outputs,
                selected_memories,
                capability_results,
                constraints,
                metadata: Vec::new(),
            });
        self.inner
            .context_store
            .update(runtime_id, |current| *current = context.clone())
            .await?;
        Ok(context)
    }

    /// Explicit upward propagation. Child output is stored in its immediate parent's context only.
    pub async fn propagate_child_result(
        &self,
        parent_id: RuntimeId,
        result: &RuntimeResult,
        result_type: impl Into<String>,
    ) -> Result<RuntimeContext> {
        let parent = self.get_runtime(parent_id).await?;
        let child = self.get_runtime(result.runtime_id).await?;
        if child.parent_id != Some(parent_id) || child.mission_id != parent.mission_id {
            return Err(WyvernError::ExecutionError(
                "child result does not belong to this parent and mission".into(),
            ));
        }
        let output = StructuredChildOutput::from_result(parent.mission_id, result, result_type);
        let context = self
            .inner
            .context_store
            .update(parent_id, |context| {
                context.child_outputs.push(output.clone())
            })
            .await?;
        self.publish(RuntimeEvent::ContextUpdated {
            mission_id: parent.mission_id,
            runtime_id: parent_id,
            parent_id: parent.parent_id,
            keys: vec![format!("child:{}", result.runtime_id)],
            timestamp: crate::domain::now_millis(),
        })?;
        Ok(context)
    }

    /// Explicit downward propagation using a copied selection of parent facts.
    pub async fn inherit_context(
        &self,
        parent_id: RuntimeId,
        child_id: RuntimeId,
        keys: &[String],
    ) -> Result<RuntimeContext> {
        let parent = self.get_runtime(parent_id).await?;
        let child = self.get_runtime(child_id).await?;
        if child.parent_id != Some(parent_id) || child.mission_id != parent.mission_id {
            return Err(WyvernError::ExecutionError(
                "context inheritance requires a direct child in the same mission".into(),
            ));
        }
        let selected = self
            .context(parent_id)
            .await?
            .local_facts
            .into_iter()
            .filter(|item| keys.contains(&item.key))
            .map(|mut item| {
                item.provenance = ContextProvenance {
                    source_type: ContextSourceType::ParentContext,
                    source_runtime_id: Some(parent_id),
                    source_event_id: None,
                    created_at: crate::domain::now_millis(),
                };
                item
            })
            .collect();
        self.compile_context(child_id, selected, Vec::new(), Vec::new(), Vec::new())
            .await
    }

    pub fn events(&self) -> EventBus {
        self.inner.events.clone()
    }

    async fn hydrate(&self) -> Result<()> {
        for mission in self.inner.store.list_missions().await? {
            self.inner.missions.insert(mission.into_mission()?).await;
        }

        let mut loaded_runtimes = Vec::new();
        for persisted in self.inner.store.list_runtimes().await? {
            loaded_runtimes.push(persisted.into_runtime()?);
        }

        for runtime in loaded_runtimes {
            self.inner
                .context_store
                .create(
                    runtime.id,
                    runtime.mission_id,
                    RuntimeContext {
                        objective: runtime.objective.clone(),
                        runtime: Some(RuntimeStateView {
                            runtime_id: runtime.id,
                            mission_id: runtime.mission_id,
                            agent_id: runtime.agent_id,
                            parent_id: runtime.parent_id,
                            lifecycle_state: runtime.state,
                            objective: runtime.objective.clone(),
                            created_at: runtime.created_at,
                            updated_at: runtime.updated_at,
                            result: runtime.result.clone(),
                        }),
                        ..RuntimeContext::default()
                    },
                )
                .await?;
            self.inner.runtimes.insert(runtime).await;
        }

        let runtimes = self.inner.runtimes.list().await;
        for runtime in runtimes {
            if let Some(parent_id) = runtime.parent_id {
                let _ = self.inner.runtimes.add_child(parent_id, runtime.id).await;
            }
        }

        Ok(())
    }

    pub async fn register_agent(&self, agent: AgentDefinition) -> AgentDefinition {
        self.inner.agents.register(agent).await
    }

    pub async fn agent_by_kind(&self, kind: AgentKind) -> Result<AgentDefinition> {
        self.inner.agents.get_by_kind(kind).await.ok_or_else(|| {
            WyvernError::ExecutionError(format!("agent kind not registered: {kind}"))
        })
    }

    pub async fn agent(&self, agent_id: crate::agent::AgentId) -> Result<AgentDefinition> {
        self.inner
            .agents
            .get(agent_id)
            .await
            .ok_or_else(|| WyvernError::ExecutionError(format!("agent not found: {agent_id}")))
    }

    pub async fn create_mission(&self, objective: impl Into<String>) -> Result<Mission> {
        let mission = Mission::new(objective);
        self.inner.store.upsert_mission(&mission).await?;
        self.inner.missions.insert(mission.clone()).await;
        Ok(mission)
    }

    pub async fn get_mission(&self, mission_id: MissionId) -> Result<Mission> {
        self.inner
            .missions
            .get(mission_id)
            .await
            .ok_or(WyvernError::MissionNotFound(mission_id))
    }

    pub async fn list_missions(&self) -> Vec<Mission> {
        self.inner.missions.list().await
    }

    pub async fn persisted_missions(&self) -> Result<Vec<crate::persistence::PersistedMission>> {
        self.inner.store.list_missions().await
    }

    pub async fn list_runtimes(&self) -> Vec<Runtime> {
        self.inner.runtimes.list().await
    }

    pub async fn persisted_runtimes(&self) -> Result<Vec<crate::persistence::PersistedRuntime>> {
        self.inner.store.list_runtimes().await
    }

    pub async fn get_runtime(&self, runtime_id: RuntimeId) -> Result<Runtime> {
        self.inner
            .runtimes
            .get(runtime_id)
            .await
            .ok_or(WyvernError::RuntimeNotFound(runtime_id))
    }

    pub async fn persisted_runtime(
        &self,
        runtime_id: RuntimeId,
    ) -> Result<Option<crate::persistence::PersistedRuntime>> {
        self.inner.store.get_runtime(runtime_id).await
    }

    pub async fn persisted_mission(
        &self,
        mission_id: MissionId,
    ) -> Result<Option<crate::persistence::PersistedMission>> {
        self.inner.store.get_mission(mission_id).await
    }

    pub(crate) async fn cancellation_receiver(
        &self,
        runtime_id: RuntimeId,
    ) -> Result<watch::Receiver<bool>> {
        let sender = self
            .inner
            .controls
            .read()
            .await
            .get(&runtime_id)
            .cloned()
            .ok_or_else(|| {
                WyvernError::CancellationError(format!(
                    "runtime {runtime_id} has no active cancellation handle"
                ))
            })?;
        Ok(sender.subscribe())
    }

    pub async fn create_runtime(
        &self,
        mission_id: MissionId,
        parent_id: Option<RuntimeId>,
        agent: &AgentDefinition,
        objective: impl Into<String>,
    ) -> Result<RuntimeHandle> {
        if self.get_mission(mission_id).await.is_err() {
            return Err(WyvernError::MissionNotFound(mission_id));
        }

        if let Some(parent_id) = parent_id {
            let parent = self
                .inner
                .runtimes
                .get(parent_id)
                .await
                .ok_or(WyvernError::ParentNotFound(parent_id))?;
            if parent.mission_id != mission_id {
                return Err(WyvernError::CrossMissionHierarchyError {
                    parent_id,
                    parent_mission_id: parent.mission_id,
                    child_mission_id: mission_id,
                });
            }
        }

        let runtime = Runtime::new(mission_id, agent.id, parent_id, objective);
        self.inner.store.upsert_runtime(&runtime, agent).await?;
        self.inner
            .context_store
            .create(
                runtime.id,
                mission_id,
                RuntimeContext {
                    objective: runtime.objective.clone(),
                    runtime: Some(RuntimeStateView {
                        runtime_id: runtime.id,
                        mission_id,
                        agent_id: runtime.agent_id,
                        parent_id,
                        lifecycle_state: runtime.state,
                        objective: runtime.objective.clone(),
                        created_at: runtime.created_at,
                        updated_at: runtime.updated_at,
                        result: None,
                    }),
                    ..RuntimeContext::default()
                },
            )
            .await?;
        self.inner.runtimes.insert(runtime.clone()).await;
        self.inner
            .controls
            .write()
            .await
            .insert(runtime.id, watch::channel(false).0);
        if parent_id.is_none() {
            if let Some(mut mission) = self.inner.missions.get(mission_id).await {
                mission.assign_root_runtime(runtime.id);
                self.inner.store.upsert_mission(&mission).await?;
                self.inner.missions.update(mission.clone()).await;
            }
        }
        self.publish(RuntimeEvent::RuntimeCreated {
            mission_id,
            runtime_id: runtime.id,
            parent_id,
            agent_id: agent.id,
            timestamp: now_millis(),
        })?;

        let handle = RuntimeHandle::new(self.clone(), runtime.id);
        handle.transition_to(RuntimeState::Ready).await?;
        Ok(handle)
    }

    pub async fn spawn_child(
        &self,
        parent_runtime_id: RuntimeId,
        agent: &AgentDefinition,
        objective: impl Into<String>,
    ) -> Result<RuntimeHandle> {
        let _guard = self
            .lifecycle_lock(parent_runtime_id)
            .await
            .lock_owned()
            .await;
        let parent = self.get_runtime(parent_runtime_id).await?;
        let handle = self
            .create_runtime(parent.mission_id, Some(parent_runtime_id), agent, objective)
            .await?;
        if let Some(mut parent_runtime) = self.inner.runtimes.get(parent_runtime_id).await {
            if !parent_runtime.children.contains(&handle.id()) {
                parent_runtime.children.push(handle.id());
                parent_runtime.updated_at =
                    now_millis().max(parent_runtime.updated_at.saturating_add(1));
                self.inner
                    .store
                    .upsert_runtime(&parent_runtime, &self.agent(parent_runtime.agent_id).await?)
                    .await?;
                self.inner.runtimes.update(parent_runtime.clone()).await;
            }
        }
        self.publish(RuntimeEvent::RuntimeChildSpawned {
            mission_id: parent.mission_id,
            runtime_id: parent_runtime_id,
            parent_id: parent.parent_id,
            child_runtime_id: handle.id(),
            child_agent_id: agent.id,
            timestamp: now_millis(),
        })?;
        Ok(handle)
    }

    pub async fn transition_runtime(
        &self,
        runtime_id: RuntimeId,
        next: RuntimeState,
    ) -> Result<Runtime> {
        let _guard = self.lifecycle_lock(runtime_id).await.lock_owned().await;
        let from = self.get_runtime(runtime_id).await?.state;
        let runtime = self.transition_runtime_locked(runtime_id, next).await?;
        self.publish(RuntimeEvent::RuntimeStateChanged {
            mission_id: runtime.mission_id,
            runtime_id,
            parent_id: runtime.parent_id,
            from,
            to: next,
            timestamp: now_millis(),
        })?;
        Ok(runtime)
    }

    async fn transition_runtime_locked(
        &self,
        runtime_id: RuntimeId,
        next: RuntimeState,
    ) -> Result<Runtime> {
        let mut runtime = self.get_runtime(runtime_id).await?;
        let from = runtime.state;
        let expected_updated_at = runtime.updated_at;
        runtime.transition_to(next)?;
        if !self
            .inner
            .store
            .compare_and_transition_runtime_state(
                runtime_id,
                from,
                expected_updated_at,
                next,
                runtime.updated_at,
            )
            .await?
        {
            let persisted = self.inner.store.get_runtime(runtime_id).await?;
            return Err(DecisionError::Stale(format!(
                "runtime {runtime_id} changed during transition (expected {from} at {expected_updated_at}, persisted {:?})",
                persisted.map(|runtime| (runtime.state, runtime.updated_at))
            ))
            .into());
        }
        self.inner.runtimes.update(runtime.clone()).await;
        if runtime.parent_id.is_none() && next == RuntimeState::Running {
            if let Some(mut mission) = self.inner.missions.get(runtime.mission_id).await {
                if mission.state == MissionState::Created {
                    mission.transition_to(MissionState::Running)?;
                    self.inner.store.upsert_mission(&mission).await?;
                    self.inner.missions.update(mission.clone()).await;
                }
            }
        }
        Ok(runtime)
    }

    pub async fn complete_runtime(
        &self,
        runtime_id: RuntimeId,
        result: RuntimeResult,
    ) -> Result<Runtime> {
        let _guard = self.lifecycle_lock(runtime_id).await.lock_owned().await;
        let mut runtime = self.get_runtime(runtime_id).await?;
        runtime.set_result(result.clone());
        runtime.transition_to(RuntimeState::Completed)?;
        let agent = self.agent(runtime.agent_id).await?;
        self.inner.store.upsert_runtime(&runtime, &agent).await?;
        self.inner.runtimes.update(runtime.clone()).await;
        self.inner.controls.write().await.remove(&runtime_id);
        self.publish(RuntimeEvent::RuntimeCompleted {
            mission_id: runtime.mission_id,
            runtime_id,
            parent_id: runtime.parent_id,
            status: result.status,
            timestamp: now_millis(),
        })?;
        if runtime.parent_id.is_none() {
            if let Some(mut mission) = self.inner.missions.get(runtime.mission_id).await {
                mission.assign_root_runtime(runtime.id);
                mission.transition_to(MissionState::Completed)?;
                self.inner.store.upsert_mission(&mission).await?;
                self.inner.missions.update(mission.clone()).await;
            }
        }
        Ok(runtime)
    }

    pub async fn fail_runtime(
        &self,
        runtime_id: RuntimeId,
        message: impl Into<String>,
    ) -> Result<Runtime> {
        let _guard = self.lifecycle_lock(runtime_id).await.lock_owned().await;
        let message = message.into();
        let mut runtime = self.get_runtime(runtime_id).await?;
        runtime.transition_to(RuntimeState::Failed)?;
        let agent = self.agent(runtime.agent_id).await?;
        self.inner.store.upsert_runtime(&runtime, &agent).await?;
        self.inner.runtimes.update(runtime.clone()).await;
        self.inner.controls.write().await.remove(&runtime_id);
        self.publish(RuntimeEvent::RuntimeFailed {
            mission_id: runtime.mission_id,
            runtime_id,
            parent_id: runtime.parent_id,
            error: message,
            timestamp: now_millis(),
        })?;
        if runtime.parent_id.is_none() {
            if let Some(mut mission) = self.inner.missions.get(runtime.mission_id).await {
                mission.transition_to(MissionState::Failed)?;
                self.inner.store.upsert_mission(&mission).await?;
                self.inner.missions.update(mission.clone()).await;
            }
        }
        Ok(runtime)
    }

    pub async fn cancel_runtime(&self, runtime_id: RuntimeId) -> Result<Runtime> {
        let _guard = self.lifecycle_lock(runtime_id).await.lock_owned().await;
        let mut runtime = self.get_runtime(runtime_id).await?;
        if !matches!(
            runtime.state,
            RuntimeState::Ready | RuntimeState::Running | RuntimeState::Waiting
        ) {
            return Err(WyvernError::CancellationError(format!(
                "runtime {runtime_id} is not cancellable from state {}",
                runtime.state
            )));
        }
        if let Some(sender) = self.inner.controls.read().await.get(&runtime_id).cloned() {
            let _ = sender.send(true);
        }
        runtime.transition_to(RuntimeState::Cancelled)?;
        let agent = self.agent(runtime.agent_id).await?;
        self.inner.store.upsert_runtime(&runtime, &agent).await?;
        self.inner.runtimes.update(runtime.clone()).await;
        self.inner.controls.write().await.remove(&runtime_id);
        self.publish(RuntimeEvent::RuntimeCancelled {
            mission_id: runtime.mission_id,
            runtime_id,
            parent_id: runtime.parent_id,
            timestamp: now_millis(),
        })?;
        if runtime.parent_id.is_none() {
            if let Some(mut mission) = self.inner.missions.get(runtime.mission_id).await {
                mission.transition_to(MissionState::Cancelled)?;
                self.inner.store.upsert_mission(&mission).await?;
                self.inner.missions.update(mission.clone()).await;
            }
        }
        Ok(runtime)
    }

    pub async fn timeout_runtime(&self, runtime_id: RuntimeId) -> Result<Runtime> {
        let runtime = self.get_runtime(runtime_id).await?;
        let updated = self
            .transition_runtime(runtime_id, RuntimeState::TimedOut)
            .await?;
        self.inner.controls.write().await.remove(&runtime_id);
        if runtime.parent_id.is_none() {
            if let Some(mut mission) = self.inner.missions.get(runtime.mission_id).await {
                if mission.state == MissionState::Running {
                    mission.transition_to(MissionState::Failed)?;
                    self.inner.store.upsert_mission(&mission).await?;
                    self.inner.missions.update(mission).await;
                }
            }
        }
        Ok(updated)
    }

    /// Marks a runtime interrupted during scheduler recovery without requiring its agent definition.
    pub async fn mark_runtime_interrupted(&self, runtime_id: RuntimeId) -> Result<Runtime> {
        let _guard = self.lifecycle_lock(runtime_id).await.lock_owned().await;
        let mut runtime = self.get_runtime(runtime_id).await?;
        let from = runtime.state;
        if from != RuntimeState::Running {
            return Ok(runtime);
        }
        runtime.transition_to(RuntimeState::Interrupted)?;
        if !self
            .inner
            .store
            .update_runtime_state(
                runtime_id,
                from,
                RuntimeState::Interrupted,
                runtime.updated_at,
            )
            .await?
        {
            return Err(WyvernError::Scheduler(format!(
                "runtime changed during recovery: {runtime_id}"
            )));
        }
        self.inner.runtimes.update(runtime.clone()).await;
        self.inner.controls.write().await.remove(&runtime_id);
        self.publish(RuntimeEvent::RuntimeStateChanged {
            mission_id: runtime.mission_id,
            runtime_id,
            parent_id: runtime.parent_id,
            from,
            to: RuntimeState::Interrupted,
            timestamp: now_millis(),
        })?;
        Ok(runtime)
    }

    pub fn executor(&self) -> RuntimeExecutor {
        RuntimeExecutor {
            kernel: self.clone(),
        }
    }

    pub async fn demo_recursive(&self) -> Result<RuntimeResult> {
        let mission = self
            .create_mission("Perform recursive runtime orchestration demo")
            .await?;

        let commander = self
            .register_agent(AgentDefinition::new("L1 Commander", AgentKind::Commander))
            .await;
        let _worker_a = self
            .register_agent(AgentDefinition::new("L2 Worker A", AgentKind::WorkerA))
            .await;
        let _worker_b = self
            .register_agent(AgentDefinition::new("L2 Worker B", AgentKind::WorkerB))
            .await;
        let worker_a1 = self
            .register_agent(AgentDefinition::new("L3 Worker A1", AgentKind::WorkerA1))
            .await;
        let worker_a2 = self
            .register_agent(AgentDefinition::new("L3 Worker A2", AgentKind::WorkerA2))
            .await;

        let root = self
            .create_runtime(
                mission.id,
                None,
                &commander,
                "Coordinate recursive runtime demo",
            )
            .await?;

        let _ = (worker_a1, worker_a2);
        let barrier = Arc::new(Barrier::new(2));
        let hooks = RuntimeHooks::recursive_demo(barrier);
        self.executor().run(root.id(), hooks).await
    }

    fn publish(&self, event: RuntimeEvent) -> Result<()> {
        self.inner.events.publish(event)
    }
}

#[derive(Clone, Default)]
pub struct RuntimeHooks {
    pub root_barrier: Option<Arc<Barrier>>,
    pub level_two_barrier: Option<Arc<Barrier>>,
    pub failure_kinds: HashSet<AgentKind>,
    pub pause_gate: Option<Arc<PauseGate>>,
}

impl RuntimeHooks {
    pub fn recursive_demo(barrier: Arc<Barrier>) -> Self {
        Self {
            level_two_barrier: Some(barrier),
            ..Self::default()
        }
    }

    pub fn with_root_barrier(mut self, barrier: Arc<Barrier>) -> Self {
        self.root_barrier = Some(barrier);
        self
    }

    pub fn with_failure_kind(mut self, kind: AgentKind) -> Self {
        self.failure_kinds.insert(kind);
        self
    }

    pub fn with_pause_gate(mut self, gate: Arc<PauseGate>) -> Self {
        self.pause_gate = Some(gate);
        self
    }
}

#[derive(Default)]
pub struct PauseGate {
    pub started: Notify,
    pub release: Notify,
}

impl PauseGate {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[derive(Clone)]
pub struct RuntimeExecutor {
    kernel: Kernel,
}

impl RuntimeExecutor {
    #[async_recursion]
    pub async fn run(&self, runtime_id: RuntimeId, hooks: RuntimeHooks) -> Result<RuntimeResult> {
        let mut cancel_rx = self.kernel.cancellation_receiver(runtime_id).await?;
        if *cancel_rx.borrow() {
            return Ok(RuntimeResult::cancelled(
                runtime_id,
                "runtime cancelled before start",
                json!({"runtime_id": runtime_id.to_string()}),
                Vec::new(),
            ));
        }

        let runtime = self.kernel.get_runtime(runtime_id).await?;
        self.kernel.publish(RuntimeEvent::RuntimeStarted {
            mission_id: runtime.mission_id,
            runtime_id,
            parent_id: runtime.parent_id,
            agent_id: runtime.agent_id,
            timestamp: now_millis(),
        })?;
        self.kernel
            .transition_runtime(runtime_id, RuntimeState::Running)
            .await?;

        let agent = self.kernel.agent(runtime.agent_id).await?;
        match agent.kind {
            AgentKind::Commander => self.run_commander(runtime_id, hooks, &mut cancel_rx).await,
            AgentKind::WorkerA => self.run_worker_a(runtime_id, hooks, &mut cancel_rx).await,
            AgentKind::WorkerB => self.run_worker_b(runtime_id, hooks, &mut cancel_rx).await,
            AgentKind::WorkerA1 | AgentKind::WorkerA2 => {
                self.run_leaf(runtime_id, hooks, &mut cancel_rx).await
            }
        }
    }

    #[async_recursion]
    async fn run_commander(
        &self,
        runtime_id: RuntimeId,
        hooks: RuntimeHooks,
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> Result<RuntimeResult> {
        if let Some(barrier) = hooks.root_barrier.as_ref() {
            tokio::select! {
                _ = barrier.wait() => {},
                _ = cancel_rx.changed() => {
                    return Ok(RuntimeResult::cancelled(runtime_id, "runtime cancelled", json!({"runtime_id": runtime_id.to_string()}), Vec::new()));
                }
            }
        }

        let worker_a = self.kernel.agent_by_kind(AgentKind::WorkerA).await?;
        let worker_b = self.kernel.agent_by_kind(AgentKind::WorkerB).await?;

        let child_a = self
            .kernel
            .spawn_child(runtime_id, &worker_a, "Investigate branch A")
            .await?;
        let child_b = self
            .kernel
            .spawn_child(runtime_id, &worker_b, "Investigate branch B")
            .await?;

        self.kernel
            .transition_runtime(runtime_id, RuntimeState::Waiting)
            .await?;

        let kernel_a = self.kernel.clone();
        let kernel_b = self.kernel.clone();
        let hooks_a = hooks.clone();
        let hooks_b = hooks;
        let task_a =
            tokio::spawn(async move { kernel_a.executor().run(child_a.id(), hooks_a).await });
        let task_b =
            tokio::spawn(async move { kernel_b.executor().run(child_b.id(), hooks_b).await });

        let child_a_result = task_a
            .await
            .map_err(|err| WyvernError::ExecutionError(err.to_string()))??;
        let child_b_result = task_b
            .await
            .map_err(|err| WyvernError::ExecutionError(err.to_string()))??;

        if *cancel_rx.borrow() {
            return Ok(RuntimeResult::cancelled(
                runtime_id,
                "runtime cancelled",
                json!({"runtime_id": runtime_id.to_string()}),
                vec![child_a_result, child_b_result],
            ));
        }

        let runtime = self.kernel.get_runtime(runtime_id).await?;
        let result = RuntimeResult::completed(
            runtime_id,
            "Commander synthesized child results",
            json!({
                "role": "commander",
                "mission_id": runtime.mission_id,
                "children": [child_a_result.runtime_id, child_b_result.runtime_id],
            }),
            vec![child_a_result, child_b_result],
        );
        self.kernel
            .complete_runtime(runtime_id, result.clone())
            .await?;
        Ok(result)
    }

    #[async_recursion]
    async fn run_worker_a(
        &self,
        runtime_id: RuntimeId,
        hooks: RuntimeHooks,
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> Result<RuntimeResult> {
        self.kernel
            .transition_runtime(runtime_id, RuntimeState::Waiting)
            .await?;
        if let Some(barrier) = hooks.level_two_barrier.as_ref() {
            tokio::select! {
                _ = barrier.wait() => {},
                _ = cancel_rx.changed() => {
                    return Ok(RuntimeResult::cancelled(runtime_id, "runtime cancelled", json!({"runtime_id": runtime_id.to_string()}), Vec::new()));
                }
            }
        }

        let worker_a1 = self.kernel.agent_by_kind(AgentKind::WorkerA1).await?;
        let worker_a2 = self.kernel.agent_by_kind(AgentKind::WorkerA2).await?;

        let child_a1 = self
            .kernel
            .spawn_child(runtime_id, &worker_a1, "Analyze endpoint A1")
            .await?;
        let child_a2 = self
            .kernel
            .spawn_child(runtime_id, &worker_a2, "Analyze endpoint A2")
            .await?;

        let kernel_a1 = self.kernel.clone();
        let kernel_a2 = self.kernel.clone();
        let hooks_a1 = hooks.clone();
        let hooks_a2 = hooks;
        let task_a1 =
            tokio::spawn(async move { kernel_a1.executor().run(child_a1.id(), hooks_a1).await });
        let task_a2 =
            tokio::spawn(async move { kernel_a2.executor().run(child_a2.id(), hooks_a2).await });

        let child_a1_result = task_a1
            .await
            .map_err(|err| WyvernError::ExecutionError(err.to_string()))??;
        let child_a2_result = task_a2
            .await
            .map_err(|err| WyvernError::ExecutionError(err.to_string()))??;

        if *cancel_rx.borrow() {
            return Ok(RuntimeResult::cancelled(
                runtime_id,
                "runtime cancelled",
                json!({"runtime_id": runtime_id.to_string()}),
                vec![child_a1_result, child_a2_result],
            ));
        }

        let result = RuntimeResult::completed(
            runtime_id,
            "Worker A synthesized its children",
            json!({
                "role": "worker_a",
                "children": [child_a1_result.runtime_id, child_a2_result.runtime_id],
            }),
            vec![child_a1_result, child_a2_result],
        );
        self.kernel
            .complete_runtime(runtime_id, result.clone())
            .await?;
        Ok(result)
    }

    #[async_recursion]
    async fn run_worker_b(
        &self,
        runtime_id: RuntimeId,
        hooks: RuntimeHooks,
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> Result<RuntimeResult> {
        self.kernel
            .transition_runtime(runtime_id, RuntimeState::Waiting)
            .await?;
        if let Some(barrier) = hooks.level_two_barrier.as_ref() {
            tokio::select! {
                _ = barrier.wait() => {},
                _ = cancel_rx.changed() => {
                    return Ok(RuntimeResult::cancelled(runtime_id, "runtime cancelled", json!({"runtime_id": runtime_id.to_string()}), Vec::new()));
                }
            }
        }

        if hooks.failure_kinds.contains(&AgentKind::WorkerB) {
            let failure = self
                .kernel
                .fail_runtime(runtime_id, "demo worker B failure")
                .await?;
            return Ok(RuntimeResult::failed(
                runtime_id,
                "Worker B failed",
                json!({"runtime_id": runtime_id.to_string(), "state": failure.state.to_string()}),
                Vec::new(),
            ));
        }

        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {},
            _ = cancel_rx.changed() => {
                return Ok(RuntimeResult::cancelled(
                    runtime_id,
                    "runtime cancelled",
                    json!({"runtime_id": runtime_id.to_string()}),
                    Vec::new(),
                ));
            }
        }

        if *cancel_rx.borrow() {
            return Ok(RuntimeResult::cancelled(
                runtime_id,
                "runtime cancelled",
                json!({"runtime_id": runtime_id.to_string()}),
                Vec::new(),
            ));
        }

        let result = RuntimeResult::completed(
            runtime_id,
            "Worker B completed deterministic work",
            json!({
                "role": "worker_b",
                "kind": "leaf",
            }),
            Vec::new(),
        );
        self.kernel
            .complete_runtime(runtime_id, result.clone())
            .await?;
        Ok(result)
    }

    #[async_recursion]
    async fn run_leaf(
        &self,
        runtime_id: RuntimeId,
        hooks: RuntimeHooks,
        cancel_rx: &mut watch::Receiver<bool>,
    ) -> Result<RuntimeResult> {
        let runtime = self.kernel.get_runtime(runtime_id).await?;
        let agent = self.kernel.agent(runtime.agent_id).await?;

        if let Some(gate) = hooks.pause_gate.as_ref() {
            gate.started.notify_waiters();
            tokio::select! {
                _ = gate.release.notified() => {},
                _ = cancel_rx.changed() => {
                    return Ok(RuntimeResult::cancelled(
                        runtime_id,
                        "runtime cancelled",
                        json!({"runtime_id": runtime_id.to_string()}),
                        Vec::new(),
                    ));
                }
            }
        }

        if hooks.failure_kinds.contains(&agent.kind) {
            let failure = self
                .kernel
                .fail_runtime(runtime_id, format!("demo leaf failure for {}", agent.kind))
                .await?;
            return Ok(RuntimeResult::failed(
                runtime_id,
                format!("{} failed", agent.name),
                json!({"runtime_id": runtime_id.to_string(), "state": failure.state.to_string()}),
                Vec::new(),
            ));
        }

        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(15)) => {},
            _ = cancel_rx.changed() => {
                return Ok(RuntimeResult::cancelled(
                    runtime_id,
                    "runtime cancelled",
                    json!({"runtime_id": runtime_id.to_string()}),
                    Vec::new(),
                ));
            }
        }

        if *cancel_rx.borrow() {
            return Ok(RuntimeResult::cancelled(
                runtime_id,
                "runtime cancelled",
                json!({"runtime_id": runtime_id.to_string()}),
                Vec::new(),
            ));
        }

        let result = RuntimeResult::completed(
            runtime_id,
            format!("{} completed leaf work", agent.name),
            json!({
                "role": agent.kind.to_string(),
                "kind": "leaf",
            }),
            Vec::new(),
        );
        self.kernel
            .complete_runtime(runtime_id, result.clone())
            .await?;
        Ok(result)
    }
}

impl RuntimeHandle {
    pub fn new(kernel: Kernel, runtime_id: RuntimeId) -> Self {
        Self { kernel, runtime_id }
    }

    pub fn id(&self) -> RuntimeId {
        self.runtime_id
    }

    pub async fn spawn_child(
        &self,
        agent: &AgentDefinition,
        objective: impl Into<String>,
    ) -> Result<RuntimeHandle> {
        self.kernel
            .spawn_child(self.runtime_id, agent, objective)
            .await
    }

    pub async fn transition_to(&self, next: RuntimeState) -> Result<Runtime> {
        self.kernel.transition_runtime(self.runtime_id, next).await
    }

    /// Requests a bounded decision for this runtime using the engine injected
    /// into the Kernel. Context must be selected explicitly by the caller.
    pub async fn decide(
        &self,
        decision_type: DecisionType,
        options: Vec<crate::decision::DecisionOption>,
        context: DecisionContext,
        constraints: Vec<DecisionConstraint>,
    ) -> Result<DecisionResult> {
        let request = self
            .kernel
            .build_decision_request(
                self.runtime_id,
                decision_type,
                options,
                context,
                constraints,
            )
            .await?;
        self.kernel.decide(request).await
    }

    /// Requests and applies the bounded RuntimeLifecycle decision. The engine
    /// receives only the explicit context and the four fixed lifecycle choices.
    pub async fn decide_and_apply(
        &self,
        context: DecisionContext,
        constraints: Vec<DecisionConstraint>,
    ) -> Result<DecisionApplication> {
        let options = [
            ("continue", "Continue execution"),
            ("wait", "Wait for an external condition"),
            ("complete", "Mark this runtime complete"),
            ("fail", "Mark this runtime failed"),
        ]
        .into_iter()
        .map(|(id, description)| DecisionOption::new(id, description))
        .collect::<std::result::Result<Vec<_>, _>>()?;
        let request = self
            .kernel
            .build_decision_request(
                self.runtime_id,
                DecisionType::RuntimeLifecycle,
                options,
                context,
                constraints,
            )
            .await?;
        let proposal = self.kernel.decide(request.clone()).await?;
        let outcome = self
            .kernel
            .apply_lifecycle_decision(&request, &proposal)
            .await?;
        Ok(DecisionApplication { proposal, outcome })
    }

    pub async fn apply_lifecycle_decision(
        &self,
        request: &DecisionRequest,
        proposal: &DecisionResult,
    ) -> Result<AppliedDecision> {
        if request.runtime_id != self.runtime_id {
            return Err(DecisionError::InvalidRequest(
                "request belongs to a different runtime handle".into(),
            )
            .into());
        }
        self.kernel
            .apply_lifecycle_decision(request, proposal)
            .await
    }
}

#[derive(Clone)]
pub struct RuntimeHandle {
    kernel: Kernel,
    runtime_id: RuntimeId,
}
