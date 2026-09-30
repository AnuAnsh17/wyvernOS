//! Deterministic recursive demo behaviors and their test controls.

use super::{BehaviorError, RuntimeBehavior, RuntimeBehaviorRegistry, RuntimeExecutionContext};
use crate::agent::{AgentDefinition, AgentKind};
use crate::domain::RuntimeId;
use crate::error::{Result, WyvernError};
use crate::kernel::Kernel;
use crate::result::RuntimeResult;
use async_trait::async_trait;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::{watch, Barrier, Notify};

pub async fn registry_with_demo_behaviors(
) -> std::result::Result<RuntimeBehaviorRegistry, BehaviorError> {
    let registry = RuntimeBehaviorRegistry::new();
    registry
        .register_kind_default(AgentKind::Commander, Arc::new(CommanderBehavior))
        .await?;
    registry
        .register_kind_default(AgentKind::WorkerA, Arc::new(WorkerABehavior))
        .await?;
    registry
        .register_kind_default(AgentKind::WorkerB, Arc::new(WorkerBBehavior))
        .await?;
    registry
        .register_kind_default(
            AgentKind::WorkerA1,
            Arc::new(LeafBehavior {
                kind: AgentKind::WorkerA1,
            }),
        )
        .await?;
    registry
        .register_kind_default(
            AgentKind::WorkerA2,
            Arc::new(LeafBehavior {
                kind: AgentKind::WorkerA2,
            }),
        )
        .await?;
    Ok(registry)
}

pub async fn run_recursive_demo(kernel: &Kernel) -> Result<RuntimeResult> {
    let mission = kernel
        .create_mission("Perform recursive runtime orchestration demo")
        .await?;
    let commander = kernel
        .register_agent(AgentDefinition::new("L1 Commander", AgentKind::Commander))
        .await?;
    let _worker_a = kernel
        .register_agent(AgentDefinition::new("L2 Worker A", AgentKind::WorkerA))
        .await?;
    let _worker_b = kernel
        .register_agent(AgentDefinition::new("L2 Worker B", AgentKind::WorkerB))
        .await?;
    let _worker_a1 = kernel
        .register_agent(AgentDefinition::new("L3 Worker A1", AgentKind::WorkerA1))
        .await?;
    let _worker_a2 = kernel
        .register_agent(AgentDefinition::new("L3 Worker A2", AgentKind::WorkerA2))
        .await?;
    let root = kernel
        .create_runtime(
            mission.id,
            None,
            &commander,
            "Coordinate recursive runtime demo",
        )
        .await?;
    let hooks = RuntimeHooks::recursive_demo(Arc::new(Barrier::new(2)));
    kernel.executor().run_with_hooks(root.id(), hooks).await
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

struct CommanderBehavior;
struct WorkerABehavior;
struct WorkerBBehavior;
struct LeafBehavior {
    kind: AgentKind,
}

#[async_trait]
impl RuntimeBehavior for CommanderBehavior {
    async fn execute(&self, execution: RuntimeExecutionContext) -> Result<RuntimeResult> {
        if let Some(barrier) = execution.demo_hooks().root_barrier.as_ref() {
            tokio::select! {
                _ = barrier.wait() => {},
                _ = wait_for_cancellation(execution.cancellation.clone()) => {
                    return Ok(cancelled(execution.runtime.id()));
                }
            }
        }
        let worker_a = execution.runtime.agent_by_kind(AgentKind::WorkerA).await?;
        let worker_b = execution.runtime.agent_by_kind(AgentKind::WorkerB).await?;
        let child_a = execution
            .runtime
            .spawn_child(&worker_a, "Investigate branch A")
            .await?;
        let child_b = execution
            .runtime
            .spawn_child(&worker_b, "Investigate branch B")
            .await?;
        execution.runtime.wait().await?;

        let exec_a = execution.clone();
        let exec_b = execution.clone();
        let task_a = tokio::spawn(async move { exec_a.run_child(&child_a).await });
        let task_b = tokio::spawn(async move { exec_b.run_child(&child_b).await });
        let child_a_result = task_a
            .await
            .map_err(|error| WyvernError::ExecutionError(error.to_string()))??;
        let child_b_result = task_b
            .await
            .map_err(|error| WyvernError::ExecutionError(error.to_string()))??;
        if *execution.cancellation.borrow() {
            return Ok(cancelled_with_children(
                execution.runtime.id(),
                vec![child_a_result, child_b_result],
            ));
        }
        let runtime = execution.runtime.inspect().await?;
        Ok(RuntimeResult::completed(
            runtime.id,
            "Commander synthesized child results",
            json!({
                "role": "commander",
                "mission_id": runtime.mission_id,
                "children": [child_a_result.runtime_id, child_b_result.runtime_id],
            }),
            vec![child_a_result, child_b_result],
        ))
    }
}

#[async_trait]
impl RuntimeBehavior for WorkerABehavior {
    async fn execute(&self, execution: RuntimeExecutionContext) -> Result<RuntimeResult> {
        execution.runtime.wait().await?;
        if let Some(barrier) = execution.demo_hooks().level_two_barrier.as_ref() {
            tokio::select! {
                _ = barrier.wait() => {},
                _ = wait_for_cancellation(execution.cancellation.clone()) => {
                    return Ok(cancelled(execution.runtime.id()));
                }
            }
        }
        let worker_a1 = execution.runtime.agent_by_kind(AgentKind::WorkerA1).await?;
        let worker_a2 = execution.runtime.agent_by_kind(AgentKind::WorkerA2).await?;
        let child_a1 = execution
            .runtime
            .spawn_child(&worker_a1, "Analyze endpoint A1")
            .await?;
        let child_a2 = execution
            .runtime
            .spawn_child(&worker_a2, "Analyze endpoint A2")
            .await?;
        let exec_a1 = execution.clone();
        let exec_a2 = execution.clone();
        let task_a1 = tokio::spawn(async move { exec_a1.run_child(&child_a1).await });
        let task_a2 = tokio::spawn(async move { exec_a2.run_child(&child_a2).await });
        let child_a1_result = task_a1
            .await
            .map_err(|error| WyvernError::ExecutionError(error.to_string()))??;
        let child_a2_result = task_a2
            .await
            .map_err(|error| WyvernError::ExecutionError(error.to_string()))??;
        if *execution.cancellation.borrow() {
            return Ok(cancelled_with_children(
                execution.runtime.id(),
                vec![child_a1_result, child_a2_result],
            ));
        }
        Ok(RuntimeResult::completed(
            execution.runtime.id(),
            "Worker A synthesized its children",
            json!({
                "role": "worker_a",
                "children": [child_a1_result.runtime_id, child_a2_result.runtime_id],
            }),
            vec![child_a1_result, child_a2_result],
        ))
    }
}

#[async_trait]
impl RuntimeBehavior for WorkerBBehavior {
    async fn execute(&self, execution: RuntimeExecutionContext) -> Result<RuntimeResult> {
        execution.runtime.wait().await?;
        if let Some(barrier) = execution.demo_hooks().level_two_barrier.as_ref() {
            tokio::select! {
                _ = barrier.wait() => {},
                _ = wait_for_cancellation(execution.cancellation.clone()) => {
                    return Ok(cancelled(execution.runtime.id()));
                }
            }
        }
        if execution
            .demo_hooks()
            .failure_kinds
            .contains(&AgentKind::WorkerB)
        {
            return Ok(RuntimeResult::failed(
                execution.runtime.id(),
                "Worker B failed",
                json!({
                    "runtime_id": execution.runtime.id().to_string(),
                    "state": "Failed"
                }),
                Vec::new(),
            ));
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {},
            _ = wait_for_cancellation(execution.cancellation.clone()) => {
                return Ok(cancelled(execution.runtime.id()));
            }
        }
        if *execution.cancellation.borrow() {
            return Ok(cancelled(execution.runtime.id()));
        }
        Ok(RuntimeResult::completed(
            execution.runtime.id(),
            "Worker B completed deterministic work",
            json!({"role": "worker_b", "kind": "leaf"}),
            Vec::new(),
        ))
    }
}

#[async_trait]
impl RuntimeBehavior for LeafBehavior {
    async fn execute(&self, execution: RuntimeExecutionContext) -> Result<RuntimeResult> {
        if let Some(gate) = execution.demo_hooks().pause_gate.as_ref() {
            gate.started.notify_waiters();
            tokio::select! {
                _ = gate.release.notified() => {},
                _ = wait_for_cancellation(execution.cancellation.clone()) => {
                    return Ok(cancelled(execution.runtime.id()));
                }
            }
        }
        if execution.demo_hooks().failure_kinds.contains(&self.kind) {
            return Ok(RuntimeResult::failed(
                execution.runtime.id(),
                format!("{} failed", execution.agent.name),
                json!({
                    "runtime_id": execution.runtime.id().to_string(),
                    "state": "Failed"
                }),
                Vec::new(),
            ));
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(15)) => {},
            _ = wait_for_cancellation(execution.cancellation.clone()) => {
                return Ok(cancelled(execution.runtime.id()));
            }
        }
        if *execution.cancellation.borrow() {
            return Ok(cancelled(execution.runtime.id()));
        }
        Ok(RuntimeResult::completed(
            execution.runtime.id(),
            format!("{} completed leaf work", execution.agent.name),
            json!({
                "role": self.kind.to_string(),
                "kind": "leaf",
            }),
            Vec::new(),
        ))
    }
}

async fn wait_for_cancellation(mut cancellation: watch::Receiver<bool>) {
    if !*cancellation.borrow() {
        let _ = cancellation.changed().await;
    }
}

fn cancelled(runtime_id: RuntimeId) -> RuntimeResult {
    RuntimeResult::cancelled(
        runtime_id,
        "runtime cancelled",
        json!({"runtime_id": runtime_id.to_string()}),
        Vec::new(),
    )
}

fn cancelled_with_children(runtime_id: RuntimeId, children: Vec<RuntimeResult>) -> RuntimeResult {
    RuntimeResult::cancelled(
        runtime_id,
        "runtime cancelled",
        json!({"runtime_id": runtime_id.to_string()}),
        children,
    )
}
