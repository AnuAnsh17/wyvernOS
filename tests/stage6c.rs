use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::behavior::demo::RuntimeHooks;
use wyvern::behavior::{
    BehaviorError, RuntimeBehavior, RuntimeBehaviorRegistry, RuntimeExecutionContext,
};
use wyvern::error::Result;
use wyvern::kernel::Kernel;
use wyvern::result::RuntimeResult;
use wyvern::scheduler::{Scheduler, SchedulerConfig, SchedulerState};

struct Marker(&'static str);

#[async_trait]
impl RuntimeBehavior for Marker {
    async fn execute(&self, execution: RuntimeExecutionContext) -> Result<RuntimeResult> {
        let keys = execution
            .context
            .local_facts
            .iter()
            .map(|fact| fact.key.clone())
            .collect::<Vec<_>>();
        Ok(RuntimeResult::completed(
            execution.runtime.id(),
            "custom behavior completed",
            json!({"marker": self.0, "keys": keys}),
            Vec::new(),
        ))
    }
}

async fn wait_for(scheduler: &Scheduler, id: wyvern::RuntimeId, expected: SchedulerState) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if scheduler.get(id).await.unwrap().state == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("scheduler state timeout");
}

#[tokio::test]
async fn registry_resolves_agent_bindings_and_rejects_duplicates() {
    let registry = RuntimeBehaviorRegistry::new();
    let agent = AgentDefinition::new("custom", AgentKind::WorkerA);
    registry
        .register_for_agent(agent.id, Arc::new(Marker("one")))
        .await
        .unwrap();
    assert!(registry.is_bound(agent.id).await);
    assert!(registry.resolve(&agent).await.is_ok());
    assert!(
        matches!(registry.register_for_agent(agent.id, Arc::new(Marker("two"))).await,
        Err(BehaviorError::DuplicateAgent(id)) if id == agent.id)
    );
    assert!(matches!(
        registry
            .resolve(&AgentDefinition::new("missing", AgentKind::WorkerA))
            .await,
        Err(BehaviorError::NotFound(_))
    ));
    let defaults = RuntimeBehaviorRegistry::new();
    defaults
        .register_kind_default(AgentKind::WorkerA, Arc::new(Marker("default")))
        .await
        .unwrap();
    assert!(matches!(
        defaults
            .register_kind_default(AgentKind::WorkerA, Arc::new(Marker("duplicate")))
            .await,
        Err(BehaviorError::DuplicateKind(AgentKind::WorkerA))
    ));
}

#[tokio::test]
async fn distinct_agent_identities_select_distinct_behaviors_and_local_context() {
    let dir = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite_with_behavior_registry(
        dir.path().join("stage6c-identities.sqlite"),
        RuntimeBehaviorRegistry::new(),
    )
    .await
    .unwrap();
    let mission = kernel
        .create_mission("identity behavior mapping")
        .await
        .unwrap();
    let first_agent = kernel
        .register_agent_with_behavior(
            AgentDefinition::new("first", AgentKind::WorkerA),
            Arc::new(Marker("first")),
        )
        .await
        .unwrap();
    let second_agent = kernel
        .register_agent_with_behavior(
            AgentDefinition::new("second", AgentKind::WorkerA),
            Arc::new(Marker("second")),
        )
        .await
        .unwrap();
    let first = kernel
        .create_runtime(mission.id, None, &first_agent, "first")
        .await
        .unwrap();
    let second = kernel
        .create_runtime(mission.id, None, &second_agent, "second")
        .await
        .unwrap();
    kernel
        .add_context_fact(
            first.id(),
            "first_private",
            wyvern::ContextValue::Text("1".into()),
        )
        .await
        .unwrap();
    kernel
        .add_context_fact(
            second.id(),
            "second_private",
            wyvern::ContextValue::Text("2".into()),
        )
        .await
        .unwrap();
    let first_result = kernel
        .executor()
        .run_with_hooks(first.id(), RuntimeHooks::default())
        .await
        .unwrap();
    let second_result = kernel
        .executor()
        .run_with_hooks(second.id(), RuntimeHooks::default())
        .await
        .unwrap();
    assert_eq!(first_result.data["marker"], "first");
    assert_eq!(second_result.data["marker"], "second");
    assert_eq!(first_result.data["keys"], json!(["first_private"]));
    assert_eq!(second_result.data["keys"], json!(["second_private"]));
    assert_eq!(
        kernel.get_runtime(first.id()).await.unwrap().state,
        wyvern::RuntimeState::Completed
    );
    assert_eq!(
        kernel.get_runtime(second.id()).await.unwrap().state,
        wyvern::RuntimeState::Completed
    );
}

#[tokio::test]
async fn missing_behavior_is_typed_and_leaves_direct_runtime_ready() {
    let dir = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite_with_behavior_registry(
        dir.path().join("stage6c-missing.sqlite"),
        RuntimeBehaviorRegistry::new(),
    )
    .await
    .unwrap();
    let mission = kernel.create_mission("missing behavior").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("unbound", AgentKind::WorkerA))
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "unbound")
        .await
        .unwrap();
    assert!(
        matches!(kernel.executor().run_with_hooks(runtime.id(), RuntimeHooks::default()).await,
        Err(wyvern::WyvernError::Behavior(BehaviorError::NotFound(id))) if id == agent.id)
    );
    assert_eq!(
        kernel.get_runtime(runtime.id()).await.unwrap().state,
        wyvern::RuntimeState::Ready
    );
}

#[tokio::test]
async fn scheduler_uses_the_same_behavior_executor_and_records_missing_binding() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stage6c-scheduler.sqlite");
    let kernel = Kernel::new_sqlite_with_behavior_registry(&db, RuntimeBehaviorRegistry::new())
        .await
        .unwrap();
    let mission = kernel.create_mission("scheduled behavior").await.unwrap();
    let agent = kernel
        .register_agent_with_behavior(
            AgentDefinition::new("scheduled", AgentKind::WorkerA),
            Arc::new(Marker("scheduled")),
        )
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "scheduled")
        .await
        .unwrap();
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        Arc::new(kernel.executor()),
        SchedulerConfig::default(),
    )
    .await
    .unwrap();
    scheduler
        .submit(runtime.id(), Default::default())
        .await
        .unwrap();
    wait_for(&scheduler, runtime.id(), SchedulerState::Completed).await;
    assert_eq!(
        kernel.get_runtime(runtime.id()).await.unwrap().state,
        wyvern::RuntimeState::Completed
    );

    let missing_agent = kernel
        .register_agent(AgentDefinition::new("unbound", AgentKind::WorkerB))
        .await
        .unwrap();
    let missing = kernel
        .create_runtime(mission.id, None, &missing_agent, "missing")
        .await
        .unwrap();
    scheduler
        .submit(missing.id(), Default::default())
        .await
        .unwrap();
    wait_for(&scheduler, missing.id(), SchedulerState::Failed).await;
    assert_eq!(
        kernel.get_runtime(missing.id()).await.unwrap().state,
        wyvern::RuntimeState::Failed
    );
    scheduler.shutdown().await;
}

#[tokio::test]
async fn behavior_can_request_wait_but_kernel_finalizes_terminal_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite_with_behavior_registry(
        dir.path().join("stage6c-state.sqlite"),
        RuntimeBehaviorRegistry::new(),
    )
    .await
    .unwrap();
    let mission = kernel
        .create_mission("kernel owns lifecycle")
        .await
        .unwrap();
    let agent = kernel
        .register_agent_with_behavior(
            AgentDefinition::new("wait", AgentKind::WorkerA),
            Arc::new(WaitBehavior),
        )
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "wait then complete")
        .await
        .unwrap();
    let result = kernel
        .executor()
        .run_with_hooks(runtime.id(), RuntimeHooks::default())
        .await
        .unwrap();
    assert_eq!(result.status, wyvern::RuntimeOutcome::Completed);
    let stored = kernel.get_runtime(runtime.id()).await.unwrap();
    assert_eq!(stored.state, wyvern::RuntimeState::Completed);
    assert_eq!(stored.result.unwrap().summary, "behavior returned outcome");
}

struct WaitBehavior;

#[async_trait]
impl RuntimeBehavior for WaitBehavior {
    async fn execute(&self, execution: RuntimeExecutionContext) -> Result<RuntimeResult> {
        execution.runtime.wait().await?;
        Ok(RuntimeResult::completed(
            execution.runtime.id(),
            "behavior returned outcome",
            json!({"ok": true}),
            Vec::new(),
        ))
    }
}
