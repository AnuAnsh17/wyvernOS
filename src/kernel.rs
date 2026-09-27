use crate::agent::{AgentDefinition, AgentKind};
use crate::domain::{now_millis, Mission, MissionId, MissionState, RuntimeId, RuntimeState};
use crate::error::{Result, WyvernError};
use crate::events::{EventBus, RuntimeEvent};
use crate::persistence::{RuntimeStore, SqliteRuntimeStore};
use crate::result::RuntimeResult;
use async_recursion::async_recursion;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{watch, Barrier, Notify, RwLock};

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
        self.updated_at = now_millis();
        Ok(())
    }

    pub fn set_result(&mut self, result: RuntimeResult) {
        self.result = Some(result);
        self.updated_at = now_millis();
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
            parent.updated_at = now_millis();
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
    pub store: Arc<dyn RuntimeStore>,
    pub controls: RwLock<HashMap<RuntimeId, watch::Sender<bool>>>,
}

impl Kernel {
    pub async fn new(store: Arc<dyn RuntimeStore>) -> Result<Self> {
        let kernel = Self {
            inner: Arc::new(KernelInner {
                agents: AgentRegistry::default(),
                missions: MissionRegistry::default(),
                runtimes: RuntimeRegistry::default(),
                events: EventBus::new(1024),
                store,
                controls: RwLock::new(HashMap::new()),
            }),
        };
        kernel.hydrate().await?;
        Ok(kernel)
    }

    pub async fn new_sqlite(path: impl AsRef<Path>) -> Result<Self> {
        let store = SqliteRuntimeStore::open(path).await?;
        Self::new(Arc::new(store)).await
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

    async fn cancellation_receiver(&self, runtime_id: RuntimeId) -> Result<watch::Receiver<bool>> {
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
        let parent = self.get_runtime(parent_runtime_id).await?;
        let handle = self
            .create_runtime(parent.mission_id, Some(parent_runtime_id), agent, objective)
            .await?;
        self.inner
            .runtimes
            .add_child(parent_runtime_id, handle.id())
            .await?;
        if let Some(mut parent_runtime) = self.inner.runtimes.get(parent_runtime_id).await {
            if !parent_runtime.children.contains(&handle.id()) {
                parent_runtime.children.push(handle.id());
                parent_runtime.updated_at = now_millis();
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
        let mut runtime = self.get_runtime(runtime_id).await?;
        let from = runtime.state;
        runtime.transition_to(next)?;
        let agent = self.agent(runtime.agent_id).await?;
        self.inner.store.upsert_runtime(&runtime, &agent).await?;
        self.inner.runtimes.update(runtime.clone()).await;
        if runtime.parent_id.is_none() && next == RuntimeState::Running {
            if let Some(mut mission) = self.inner.missions.get(runtime.mission_id).await {
                mission.transition_to(MissionState::Running)?;
                self.inner.store.upsert_mission(&mission).await?;
                self.inner.missions.update(mission.clone()).await;
            }
        }
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

    pub async fn complete_runtime(
        &self,
        runtime_id: RuntimeId,
        result: RuntimeResult,
    ) -> Result<Runtime> {
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
        let mut runtime = self.get_runtime(runtime_id).await?;
        if !matches!(runtime.state, RuntimeState::Running | RuntimeState::Waiting) {
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
}

#[derive(Clone)]
pub struct RuntimeHandle {
    kernel: Kernel,
    runtime_id: RuntimeId,
}
