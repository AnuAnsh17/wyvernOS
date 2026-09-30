use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use tokio::sync::watch;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::context::{ContextProvenance, ContextSourceType, ContextValue};
use wyvern::decision::{
    DecisionConstraint, DecisionConstraintKind, DecisionContext, DecisionEngine,
    DecisionEngineProvenance, DecisionError, DecisionOption, DecisionRequest, DecisionResult,
    DecisionType, MockDecisionEngine,
};
use wyvern::domain::{DecisionRequestId, MissionId, RuntimeId, RuntimeState};
use wyvern::events::RuntimeEvent;
use wyvern::kernel::{Kernel, Runtime};
use wyvern::memory::{MemoryDraft, MemoryProvenance, MemoryScope, MemoryType};
use wyvern::result::RuntimeResult;
use wyvern::scheduler::{
    RuntimeExecutionError, ScheduleOptions, ScheduledRuntimeExecutor, Scheduler, SchedulerConfig,
    SchedulerState,
};

fn options() -> Vec<DecisionOption> {
    vec![
        DecisionOption::new("continue", "Continue authorized work").unwrap(),
        DecisionOption::new("ask_user", "Ask the user for clarification").unwrap(),
    ]
}

fn request(mission_id: MissionId, runtime_id: RuntimeId) -> DecisionRequest {
    DecisionRequest::new(
        mission_id,
        runtime_id,
        runtime_id,
        None,
        RuntimeState::Ready,
        "choose a bounded next step",
        DecisionType::ExecutionStrategy,
        options(),
    )
}

struct InvalidOptionEngine;

#[async_trait]
impl DecisionEngine for InvalidOptionEngine {
    async fn decide(&self, request: DecisionRequest) -> Result<DecisionResult, DecisionError> {
        Ok(DecisionResult {
            request_id: request.request_id,
            selected_option: DecisionOption::new("invented", "not offered")?.id,
            confidence: Some(0.8),
            engine: DecisionEngineProvenance {
                engine_id: "test.invalid-option".into(),
                version: None,
            },
            reasoning: None,
            latency_ms: Some(1),
        })
    }
}

struct FailingEngine;

#[async_trait]
impl DecisionEngine for FailingEngine {
    async fn decide(&self, _request: DecisionRequest) -> Result<DecisionResult, DecisionError> {
        Err(DecisionError::InternalFailure("test failure".into()))
    }
}

struct ImmediateExecutor;

#[async_trait]
impl ScheduledRuntimeExecutor for ImmediateExecutor {
    async fn execute(
        &self,
        runtime: Runtime,
        _cancellation: watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        Ok(RuntimeResult::completed(
            runtime.id,
            "completed",
            json!({"ok": true}),
            Vec::new(),
        ))
    }
}

async fn create_runtime(kernel: &Kernel) -> (MissionId, wyvern::RuntimeHandle) {
    let mission = kernel
        .create_mission("decision boundary test")
        .await
        .unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "choose a safe next step")
        .await
        .unwrap();
    (mission.id, runtime)
}

async fn wait_for_scheduler_state(
    scheduler: &Scheduler,
    runtime_id: RuntimeId,
    expected: SchedulerState,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if scheduler.get(runtime_id).await.unwrap().state == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("scheduler runtime did not reach the expected state");
}

#[test]
fn decision_contract_types_round_trip_through_serde() {
    let request_id = DecisionRequestId::new();
    assert!(request_id.to_string().starts_with("dreq_"));
    let mut request = request(MissionId::new(), RuntimeId::new());
    request.request_id = request_id;
    request
        .context
        .facts
        .push(wyvern::decision::DecisionContextFact {
            key: "finding".into(),
            value: ContextValue::Text("selected".into()),
            provenance: ContextProvenance {
                source_type: ContextSourceType::ParentContext,
                source_runtime_id: Some(RuntimeId::new()),
                source_event_id: Some("event-1".into()),
                created_at: 17,
            },
            priority: Some(4),
        });
    request.constraints.push(DecisionConstraint {
        kind: DecisionConstraintKind::Hard,
        description: "Do not exceed mission permissions".into(),
    });
    request.options[0].metadata.insert("rank".into(), json!(1));

    let encoded = serde_json::to_string(&request).unwrap();
    let decoded: DecisionRequest = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, request);
    assert_eq!(
        serde_json::from_str::<DecisionType>(
            &serde_json::to_string(&DecisionType::Custom("custom_review".into())).unwrap()
        )
        .unwrap(),
        DecisionType::Custom("custom_review".into())
    );
    assert_eq!(
        serde_json::from_str::<DecisionOption>(
            &serde_json::to_string(&request.options[0]).unwrap()
        )
        .unwrap(),
        request.options[0]
    );

    let result = DecisionResult {
        request_id,
        selected_option: request.options[0].id.clone(),
        confidence: Some(0.75),
        engine: DecisionEngineProvenance {
            engine_id: "test.engine".into(),
            version: Some("v1".into()),
        },
        reasoning: Some("This option is within the supplied bounds.".into()),
        latency_ms: Some(3),
    };
    let encoded = serde_json::to_string(&result).unwrap();
    let decoded: DecisionResult = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, result);
}

#[tokio::test]
async fn mock_engine_is_deterministic_and_rejects_empty_or_invalid_options() {
    let mission_id = MissionId::new();
    let runtime_id = RuntimeId::new();
    let request = request(mission_id, runtime_id);
    let first = MockDecisionEngine.decide(request.clone()).await.unwrap();
    let second = MockDecisionEngine.decide(request.clone()).await.unwrap();
    assert_eq!(first.selected_option, request.options[0].id);
    assert_eq!(first, second);
    first.validate_for(&request).unwrap();

    let empty = DecisionRequest::new(
        mission_id,
        runtime_id,
        runtime_id,
        None,
        RuntimeState::Ready,
        "objective",
        DecisionType::TaskRouting,
        Vec::new(),
    );
    assert_eq!(
        MockDecisionEngine.decide(empty).await,
        Err(DecisionError::NoValidOptions)
    );

    let duplicate = DecisionRequest::new(
        mission_id,
        runtime_id,
        runtime_id,
        None,
        RuntimeState::Ready,
        "objective",
        DecisionType::TaskRouting,
        vec![
            DecisionOption::new("same", "first").unwrap(),
            DecisionOption::new("same", "second").unwrap(),
        ],
    );
    assert!(matches!(
        MockDecisionEngine.decide(duplicate).await,
        Err(DecisionError::InvalidRequest(_))
    ));
}

#[test]
fn selected_context_projection_keeps_provenance_and_excludes_unselected_items() {
    let mut context = wyvern::RuntimeContext::default();
    let fact = |key: &str| wyvern::ContextItem {
        key: key.into(),
        value: ContextValue::Text(key.into()),
        provenance: ContextProvenance {
            source_type: ContextSourceType::LocalContext,
            source_runtime_id: Some(RuntimeId::new()),
            source_event_id: None,
            created_at: 10,
        },
        priority: None,
    };
    context.local_facts = vec![fact("selected"), fact("private")];
    let selected = DecisionContext::from_selected(&context, &["selected"], &[]);
    assert_eq!(selected.facts.len(), 1);
    assert_eq!(selected.facts[0].key, "selected");
    assert_eq!(
        selected.facts[0].provenance.source_type,
        ContextSourceType::LocalContext
    );
    assert!(selected.memories.is_empty());
}

#[tokio::test]
async fn runtime_handle_injects_engine_builds_branch_identity_and_emits_enveloped_events() {
    let dir = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite_with_decision_engine(
        dir.path().join("stage6a-success.sqlite"),
        Arc::new(MockDecisionEngine),
    )
    .await
    .unwrap();
    let mission = kernel.create_mission("branch identities").await.unwrap();
    let root_agent = kernel
        .register_agent(AgentDefinition::new("root", AgentKind::Commander))
        .await
        .unwrap();
    let child_agent = kernel
        .register_agent(AgentDefinition::new("child", AgentKind::WorkerA))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &root_agent, "root")
        .await
        .unwrap();
    let branch_a = kernel
        .spawn_child(root.id(), &child_agent, "branch A")
        .await
        .unwrap();
    let branch_b = kernel
        .spawn_child(root.id(), &child_agent, "branch B")
        .await
        .unwrap();
    let leaf_a = kernel
        .spawn_child(branch_a.id(), &child_agent, "leaf A")
        .await
        .unwrap();
    let runtime_before = kernel.get_runtime(leaf_a.id()).await.unwrap();
    let options = options();
    let mut envelopes = kernel.events().subscribe_envelopes();
    let request = kernel
        .build_decision_request(
            leaf_a.id(),
            DecisionType::ExecutionStrategy,
            options.clone(),
            DecisionContext::default(),
            vec![DecisionConstraint {
                kind: DecisionConstraintKind::Hard,
                description: "Keep within the supplied task permissions".into(),
            }],
        )
        .await
        .unwrap();
    assert_eq!(request.mission_id, mission.id);
    assert_eq!(request.runtime_id, leaf_a.id());
    assert_eq!(request.branch_id, branch_a.id());
    assert_ne!(request.branch_id, branch_b.id());
    assert_eq!(request.runtime_state.lifecycle_state, RuntimeState::Ready);

    let result = leaf_a
        .decide(
            DecisionType::ExecutionStrategy,
            options,
            DecisionContext::default(),
            Vec::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.selected_option.as_str(), "continue");
    assert_eq!(
        kernel.get_runtime(leaf_a.id()).await.unwrap().state,
        runtime_before.state
    );

    let requested = envelopes.try_recv().unwrap();
    let completed = envelopes.try_recv().unwrap();
    assert_eq!(requested.event_type, "decision.requested");
    assert_eq!(completed.event_type, "decision.completed");
    assert_eq!(requested.mission_id, mission.id);
    assert_eq!(requested.runtime_id, leaf_a.id());
    assert_eq!(requested.parent_runtime_id, Some(branch_a.id()));
    assert_eq!(completed.runtime_id, leaf_a.id());
    assert_ne!(requested.event_id, completed.event_id);
    let requested_id = match &requested.payload {
        RuntimeEvent::DecisionRequested { request_id, .. } => *request_id,
        event => panic!("unexpected requested event: {event:?}"),
    };
    let completed_id = match &completed.payload {
        RuntimeEvent::DecisionCompleted { request_id, .. } => *request_id,
        event => panic!("unexpected completed event: {event:?}"),
    };
    assert_eq!(result.request_id, requested_id);
    assert_eq!(requested_id, completed_id);
    assert!(matches!(
        requested.payload,
        RuntimeEvent::DecisionRequested { .. }
    ));
    assert!(matches!(
        completed.payload,
        RuntimeEvent::DecisionCompleted { .. }
    ));
}

#[tokio::test]
async fn engine_failure_preserves_runtime_scheduler_context_memory_and_persisted_state() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stage6a-failure.sqlite");
    let kernel = Kernel::new_sqlite_with_decision_engine(&db, Arc::new(FailingEngine))
        .await
        .unwrap();
    let (mission_id, runtime) = create_runtime(&kernel).await;
    kernel
        .add_context_fact(
            runtime.id(),
            "local",
            ContextValue::Structured(json!({"keep": true})),
        )
        .await
        .unwrap();
    let memory = kernel
        .create_memory({
            let mut draft = MemoryDraft::new(
                MemoryScope::Runtime,
                mission_id,
                MemoryType::Finding,
                "persist this finding",
                MemoryProvenance {
                    source_runtime_id: Some(runtime.id()),
                    source_agent_id: None,
                    source_event_id: None,
                    created_at: 11,
                },
            );
            draft.runtime_id = Some(runtime.id());
            draft
        })
        .await
        .unwrap();
    let context_before = kernel.context(runtime.id()).await.unwrap();
    let memory_before = kernel.memories().get(memory.memory_id).await.unwrap();
    let mut compiled_context = context_before.clone();
    compiled_context
        .selected_memories
        .push(memory_before.clone().unwrap());
    let selected_context =
        DecisionContext::from_selected(&compiled_context, &["local"], &[memory.memory_id]);
    assert_eq!(selected_context.facts.len(), 1);
    assert_eq!(selected_context.memories.len(), 1);
    assert_eq!(selected_context.memories[0].content, "persist this finding");
    let runtime_before = kernel.get_runtime(runtime.id()).await.unwrap();
    let scheduler_agent = kernel
        .register_agent(AgentDefinition::new("scheduled worker", AgentKind::WorkerB))
        .await
        .unwrap();
    let scheduled_runtime = kernel
        .create_runtime(
            mission_id,
            None,
            &scheduler_agent,
            "unrelated scheduled work",
        )
        .await
        .unwrap();
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        Arc::new(ImmediateExecutor),
        SchedulerConfig::default(),
    )
    .await
    .unwrap();
    scheduler
        .submit(scheduled_runtime.id(), ScheduleOptions::default())
        .await
        .unwrap();
    wait_for_scheduler_state(
        &scheduler,
        scheduled_runtime.id(),
        SchedulerState::Completed,
    )
    .await;
    let scheduler_before = scheduler.inspect_mission(mission_id).await.unwrap();
    assert_eq!(scheduler_before.completed, 1);
    let mut events = kernel.events().subscribe_envelopes();

    let error = runtime
        .decide(
            DecisionType::Verification,
            options(),
            selected_context,
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        wyvern::WyvernError::Decision(DecisionError::InternalFailure(_))
    ));
    assert_eq!(
        kernel.get_runtime(runtime.id()).await.unwrap().state,
        runtime_before.state
    );
    assert_eq!(kernel.context(runtime.id()).await.unwrap(), context_before);
    assert_eq!(
        kernel.memories().get(memory.memory_id).await.unwrap(),
        memory_before
    );
    let scheduler_after = scheduler.inspect_mission(mission_id).await.unwrap();
    assert_eq!(scheduler_before.total, scheduler_after.total);
    assert_eq!(
        scheduler_before.runtimes.len(),
        scheduler_after.runtimes.len()
    );
    assert_eq!(scheduler_after.completed, 1);
    assert_eq!(
        scheduler_before.runtimes[0].runtime_id,
        scheduler_after.runtimes[0].runtime_id
    );
    assert_eq!(events.try_recv().unwrap().event_type, "decision.requested");
    assert_eq!(events.try_recv().unwrap().event_type, "decision.failed");

    drop(runtime);
    drop(scheduler);
    drop(kernel);
    let reopened = Kernel::new_sqlite(&db).await.unwrap();
    assert_eq!(
        reopened.get_runtime(runtime_before.id).await.unwrap().state,
        runtime_before.state
    );
    assert_eq!(
        reopened.context(runtime_before.id).await.unwrap(),
        context_before
    );
    assert_eq!(
        reopened.memories().get(memory.memory_id).await.unwrap(),
        memory_before
    );
    let reopened_scheduler = Scheduler::open(
        reopened.clone(),
        &db,
        Arc::new(ImmediateExecutor),
        SchedulerConfig::default(),
    )
    .await
    .unwrap();
    let recovered_schedule = reopened_scheduler
        .inspect_mission(mission_id)
        .await
        .unwrap();
    assert_eq!(recovered_schedule.completed, 1);
    assert_eq!(recovered_schedule.runtimes.len(), 1);
}

#[tokio::test]
async fn engine_result_cannot_select_an_option_that_was_not_offered() {
    let dir = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite_with_decision_engine(
        dir.path().join("stage6a-invalid-result.sqlite"),
        Arc::new(InvalidOptionEngine),
    )
    .await
    .unwrap();
    let (_mission_id, runtime) = create_runtime(&kernel).await;
    let before = kernel.get_runtime(runtime.id()).await.unwrap();
    let error = runtime
        .decide(
            DecisionType::TaskRouting,
            options(),
            DecisionContext::default(),
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        wyvern::WyvernError::Decision(DecisionError::MalformedResult(_))
    ));
    assert_eq!(
        kernel.get_runtime(runtime.id()).await.unwrap().state,
        before.state
    );
}

#[tokio::test]
async fn default_kernel_reports_missing_engine_without_using_a_concrete_implementation() {
    let dir = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite(dir.path().join("stage6a-no-engine.sqlite"))
        .await
        .unwrap();
    let (_mission_id, runtime) = create_runtime(&kernel).await;
    let mut events = kernel.events().subscribe_envelopes();

    let error = runtime
        .decide(
            DecisionType::TaskRouting,
            options(),
            DecisionContext::default(),
            Vec::new(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        wyvern::WyvernError::Decision(DecisionError::EngineUnavailable)
    ));
    assert_eq!(events.try_recv().unwrap().event_type, "decision.requested");
    assert_eq!(events.try_recv().unwrap().event_type, "decision.failed");
}
