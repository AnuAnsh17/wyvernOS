use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::watch;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::decision::{
    DecisionContext, DecisionEngine, DecisionEngineProvenance, DecisionError, DecisionOption,
    DecisionRequest, DecisionResult, DecisionType,
};
use wyvern::domain::RuntimeState;
use wyvern::kernel::Kernel;
use wyvern::memory::{MemoryDraft, MemoryProvenance, MemoryScope, MemoryType};
use wyvern::result::RuntimeResult;
use wyvern::scheduler::{
    RuntimeExecutionError, ScheduledRuntimeExecutor, Scheduler, SchedulerConfig,
};

struct SelectEngine(&'static str);

struct ImmediateExecutor;

#[async_trait]
impl ScheduledRuntimeExecutor for ImmediateExecutor {
    async fn execute(
        &self,
        runtime: wyvern::Runtime,
        _cancellation: watch::Receiver<bool>,
    ) -> Result<RuntimeResult, RuntimeExecutionError> {
        Ok(RuntimeResult::completed(
            runtime.id,
            "completed",
            json!({"ok": true}),
            Vec::new(),
        ))
    }
}

#[async_trait]
impl DecisionEngine for SelectEngine {
    async fn decide(&self, request: DecisionRequest) -> Result<DecisionResult, DecisionError> {
        Ok(DecisionResult {
            request_id: request.request_id,
            selected_option: DecisionOption::new(self.0, self.0)?.id,
            confidence: Some(0.9),
            engine: DecisionEngineProvenance {
                engine_id: "test.lifecycle".into(),
                version: Some("1".into()),
            },
            reasoning: None,
            latency_ms: Some(0),
        })
    }
}

async fn setup(
    engine: Arc<dyn DecisionEngine>,
) -> (tempfile::TempDir, Kernel, wyvern::RuntimeHandle) {
    let dir = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite_with_decision_engine(dir.path().join("stage6b.sqlite"), engine)
        .await
        .unwrap();
    let mission = kernel
        .create_mission("bounded lifecycle decision")
        .await
        .unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "perform bounded work")
        .await
        .unwrap();
    runtime.transition_to(RuntimeState::Running).await.unwrap();
    (dir, kernel, runtime)
}

async fn lifecycle_request(kernel: &Kernel, runtime_id: wyvern::RuntimeId) -> DecisionRequest {
    kernel
        .build_decision_request(
            runtime_id,
            DecisionType::RuntimeLifecycle,
            ["continue", "wait", "complete", "fail"]
                .into_iter()
                .map(|option| DecisionOption::new(option, option).unwrap())
                .collect(),
            DecisionContext::default(),
            Vec::new(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn accepted_complete_is_persisted_and_events_describe_proposal_then_application() {
    let (dir, kernel, runtime) = setup(Arc::new(SelectEngine("complete"))).await;
    let mut envelopes = kernel.events().subscribe_envelopes();
    let application = runtime
        .decide_and_apply(DecisionContext::default(), Vec::new())
        .await
        .unwrap();
    assert_eq!(application.proposal.selected_option.as_str(), "complete");
    assert_eq!(application.outcome.previous_state, RuntimeState::Running);
    assert_eq!(application.outcome.resulting_state, RuntimeState::Completed);
    assert!(application.outcome.changed_state);
    assert_eq!(
        kernel.get_runtime(runtime.id()).await.unwrap().state,
        RuntimeState::Completed
    );
    let envelopes = (0..4)
        .map(|_| envelopes.try_recv().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        envelopes
            .iter()
            .map(|envelope| envelope.event_type.as_str())
            .collect::<Vec<_>>(),
        [
            "decision.requested",
            "decision.completed",
            "decision.accepted",
            "runtime.state_changed"
        ]
    );

    let db = dir.path().join("stage6b.sqlite");
    drop(runtime);
    drop(kernel);
    let recovered = Kernel::new_sqlite(&db).await.unwrap();
    assert_eq!(
        recovered
            .get_runtime(application.outcome.runtime_id)
            .await
            .unwrap()
            .state,
        RuntimeState::Completed
    );
}

#[tokio::test]
async fn illegal_transition_is_rejected_without_runtime_change() {
    let (_dir, kernel, _runtime) = setup(Arc::new(SelectEngine("complete"))).await;
    let created = kernel.create_mission("invalid transition").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker2", AgentKind::WorkerB))
        .await
        .unwrap();
    let fresh = kernel
        .create_runtime(created.id, None, &agent, "not started")
        .await
        .unwrap();
    let request = lifecycle_request(&kernel, fresh.id()).await;
    let result = SelectEngine("complete")
        .decide(request.clone())
        .await
        .unwrap();
    let before = kernel.get_runtime(fresh.id()).await.unwrap();
    let mut events = kernel.events().subscribe_envelopes();
    assert!(matches!(
        fresh.apply_lifecycle_decision(&request, &result).await,
        Err(wyvern::WyvernError::Decision(DecisionError::Rejected(_)))
    ));
    assert_eq!(
        kernel.get_runtime(fresh.id()).await.unwrap().state,
        before.state
    );
    assert_eq!(events.try_recv().unwrap().event_type, "decision.rejected");
}

#[tokio::test]
async fn stale_proposal_is_rejected_after_runtime_version_changes() {
    let (dir, kernel, runtime) = setup(Arc::new(SelectEngine("complete"))).await;
    let request = lifecycle_request(&kernel, runtime.id()).await;
    kernel
        .add_context_fact(
            runtime.id(),
            "private",
            wyvern::ContextValue::Text("unchanged".into()),
        )
        .await
        .unwrap();
    let context_before = kernel.context(runtime.id()).await.unwrap();
    let mission_id = kernel.get_runtime(runtime.id()).await.unwrap().mission_id;
    let memory = kernel
        .create_memory({
            let mut draft = MemoryDraft::new(
                MemoryScope::Runtime,
                mission_id,
                MemoryType::Finding,
                "unchanged after stale decision",
                MemoryProvenance {
                    source_runtime_id: Some(runtime.id()),
                    source_agent_id: None,
                    source_event_id: None,
                    created_at: 2,
                },
            );
            draft.runtime_id = Some(runtime.id());
            draft
        })
        .await
        .unwrap();
    let memory_before = kernel.memories().get(memory.memory_id).await.unwrap();
    let scheduler = Scheduler::open(
        kernel.clone(),
        dir.path().join("stage6b.sqlite"),
        Arc::new(ImmediateExecutor),
        SchedulerConfig::default(),
    )
    .await
    .unwrap();
    let scheduler_before = scheduler.inspect_mission(mission_id).await.unwrap();
    let mut events = kernel.events().subscribe_envelopes();
    let proposal = kernel.decide(request.clone()).await.unwrap();
    runtime.transition_to(RuntimeState::Waiting).await.unwrap();
    let before = kernel.get_runtime(runtime.id()).await.unwrap();
    assert!(matches!(
        runtime.apply_lifecycle_decision(&request, &proposal).await,
        Err(wyvern::WyvernError::Decision(DecisionError::Stale(_)))
    ));
    assert_eq!(
        kernel.get_runtime(runtime.id()).await.unwrap().state,
        before.state
    );
    let envelopes = (0..4)
        .map(|_| events.try_recv().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        envelopes
            .iter()
            .map(|envelope| envelope.event_type.as_str())
            .collect::<Vec<_>>(),
        [
            "decision.requested",
            "decision.completed",
            "runtime.state_changed",
            "decision.stale"
        ]
    );
    assert_eq!(kernel.context(runtime.id()).await.unwrap(), context_before);
    assert_eq!(
        kernel.memories().get(memory.memory_id).await.unwrap(),
        memory_before
    );
    let scheduler_after = scheduler.inspect_mission(mission_id).await.unwrap();
    assert_eq!(scheduler_after.total, scheduler_before.total);
    assert_eq!(
        scheduler_after.runtimes.len(),
        scheduler_before.runtimes.len()
    );
    assert_eq!(scheduler_after.completed, scheduler_before.completed);
    assert_eq!(scheduler_after.failed, scheduler_before.failed);
    let db = dir.path().join("stage6b.sqlite");
    drop(scheduler);
    drop(runtime);
    drop(kernel);
    let recovered = Kernel::new_sqlite(db).await.unwrap();
    assert_eq!(
        recovered.get_runtime(before.id).await.unwrap().state,
        RuntimeState::Waiting
    );
    assert_eq!(recovered.context(before.id).await.unwrap(), context_before);
    assert_eq!(
        recovered.memories().get(memory.memory_id).await.unwrap(),
        memory_before
    );
}

#[tokio::test]
async fn concurrent_applications_of_one_proposal_have_one_winner() {
    let (_dir, kernel, runtime) = setup(Arc::new(SelectEngine("complete"))).await;
    let request = lifecycle_request(&kernel, runtime.id()).await;
    let proposal = kernel.decide(request.clone()).await.unwrap();
    let (first, second) = tokio::join!(
        runtime.apply_lifecycle_decision(&request, &proposal),
        runtime.apply_lifecycle_decision(&request, &proposal),
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let loser = match (first, second) {
        (Err(error), Ok(_)) | (Ok(_), Err(error)) => error,
        _ => panic!("exactly one application must lose the race"),
    };
    assert!(matches!(
        loser,
        wyvern::WyvernError::Decision(DecisionError::Stale(_))
    ));
    assert_eq!(
        kernel.get_runtime(runtime.id()).await.unwrap().state,
        RuntimeState::Completed
    );
}

#[tokio::test]
async fn engine_cannot_apply_an_unoffered_option() {
    let (_dir, kernel, runtime) = setup(Arc::new(SelectEngine("complete"))).await;
    let request = lifecycle_request(&kernel, runtime.id()).await;
    let invalid = DecisionResult {
        request_id: request.request_id,
        selected_option: DecisionOption::new("invented", "invented").unwrap().id,
        confidence: None,
        engine: DecisionEngineProvenance {
            engine_id: "test".into(),
            version: None,
        },
        reasoning: None,
        latency_ms: None,
    };
    let before = kernel.get_runtime(runtime.id()).await.unwrap();
    let mut events = kernel.events().subscribe_envelopes();
    assert!(matches!(
        runtime.apply_lifecycle_decision(&request, &invalid).await,
        Err(wyvern::WyvernError::Decision(
            DecisionError::MalformedResult(_)
        ))
    ));
    assert_eq!(
        kernel.get_runtime(runtime.id()).await.unwrap().state,
        before.state
    );
    assert_eq!(events.try_recv().unwrap().event_type, "decision.rejected");
}

#[tokio::test]
async fn context_and_memory_stores_are_not_modified_by_application() {
    let (_dir, kernel, runtime) = setup(Arc::new(SelectEngine("wait"))).await;
    let mission_id = kernel.get_runtime(runtime.id()).await.unwrap().mission_id;
    kernel
        .add_context_fact(
            runtime.id(),
            "local",
            wyvern::ContextValue::Text("kept".into()),
        )
        .await
        .unwrap();
    let context_before = kernel.context(runtime.id()).await.unwrap();
    let memory = kernel
        .create_memory({
            let mut draft = MemoryDraft::new(
                MemoryScope::Runtime,
                mission_id,
                MemoryType::Finding,
                "preserve during lifecycle decision",
                MemoryProvenance {
                    source_runtime_id: Some(runtime.id()),
                    source_agent_id: None,
                    source_event_id: None,
                    created_at: 1,
                },
            );
            draft.runtime_id = Some(runtime.id());
            draft
        })
        .await
        .unwrap();
    let memory_before = kernel.memories().get(memory.memory_id).await.unwrap();
    let application = runtime
        .decide_and_apply(DecisionContext::default(), Vec::new())
        .await
        .unwrap();
    assert_eq!(application.outcome.resulting_state, RuntimeState::Waiting);
    assert_eq!(kernel.context(runtime.id()).await.unwrap(), context_before);
    assert_eq!(
        kernel.memories().get(memory.memory_id).await.unwrap(),
        memory_before
    );
}
