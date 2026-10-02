use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

use serde_json::json;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::authorization::{AuthorizationGrantDraft, AuthorizationIssuer, AuthorizationScope};
use wyvern::capability::{
    mock_echo_definition, CapabilityCancellation, CapabilityDescriptor, CapabilityError,
    CapabilityExecutor, CapabilityKind, CapabilityRequest, CapabilityRequestStatus, RiskLevel,
    ToolDescriptor,
};
use wyvern::domain::{CapabilityId, MissionId, RuntimeId};
use wyvern::events::{EventBus, RuntimeEvent};
use wyvern::kernel::Kernel;
use wyvern::memory::{MemoryProvenance, MemoryScope, MemoryType};
use wyvern::CapabilityRegistry;
use wyvern::RuntimeContext;

async fn runtime() -> (Kernel, MissionId, RuntimeId, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite(dir.path().join("stage4.sqlite"))
        .await
        .unwrap();
    let mission = kernel.create_mission("capability boundary").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "exercise capability")
        .await
        .unwrap();
    (kernel, mission.id, runtime.id(), dir)
}

async fn install_mock(kernel: &Kernel) -> (CapabilityId, wyvern::ToolId) {
    let (capability, tool, executor) = mock_echo_definition();
    let ids = (capability.id, tool.id);
    kernel
        .capabilities()
        .register(capability, vec![tool], executor)
        .await
        .unwrap();
    ids
}

async fn grant(
    kernel: &Kernel,
    mission_id: MissionId,
    runtime_id: RuntimeId,
    capability_id: CapabilityId,
    tool_id: wyvern::ToolId,
) -> wyvern::GrantId {
    let runtime = kernel.get_runtime(runtime_id).await.unwrap();
    kernel
        .issue_authorization_grant(
            runtime_id,
            AuthorizationGrantDraft {
                issuer: AuthorizationIssuer::Runtime(runtime_id),
                subject_agent_id: runtime.agent_id,
                capability_id,
                allowed_tools: vec![tool_id],
                scope: AuthorizationScope::Mission(mission_id),
                maximum_risk: RiskLevel::Critical,
                expires_at: None,
            },
        )
        .await
        .unwrap()
        .id
}

fn request(
    mission_id: MissionId,
    runtime_id: RuntimeId,
    capability_id: CapabilityId,
    tool_id: wyvern::ToolId,
    input: serde_json::Value,
) -> CapabilityRequest {
    CapabilityRequest::new(runtime_id, mission_id, capability_id, tool_id, input)
}

#[tokio::test]
async fn registry_register_lookup_list_resolve_unregister_and_duplicate_work() {
    let registry = CapabilityRegistry::new(EventBus::new(64));
    let (capability, tool, executor) = mock_echo_definition();
    registry
        .register(capability.clone(), vec![tool.clone()], executor.clone())
        .await
        .unwrap();
    assert_eq!(registry.get(capability.id).await, Some(capability.clone()));
    assert_eq!(registry.list().await, vec![capability.clone()]);
    assert_eq!(registry.list_tools().await, vec![tool.clone()]);
    assert_eq!(registry.resolve_tool(tool.id).await.unwrap().1, tool);
    assert!(matches!(
        registry.resolve_tool(wyvern::ToolId::new()).await,
        Err(CapabilityError::ToolNotFound(_))
    ));
    assert!(matches!(
        registry
            .register(capability.clone(), Vec::new(), executor)
            .await,
        Err(CapabilityError::RegistryError(_))
    ));
    assert_eq!(
        registry.unregister(capability.id).await.unwrap(),
        capability
    );
    assert!(registry.list().await.is_empty());
}

#[tokio::test]
async fn mock_echo_succeeds_and_emits_routed_lifecycle_events() {
    let (kernel, mission_id, runtime_id, _dir) = runtime().await;
    let (capability_id, tool_id) = install_mock(&kernel).await;
    let request = request(
        mission_id,
        runtime_id,
        capability_id,
        tool_id,
        json!({"message":"hello"}),
    );
    let request_id = request.request_id;
    grant(&kernel, mission_id, runtime_id, capability_id, tool_id).await;
    let mut events = kernel.events().subscribe_envelopes();
    let result = kernel
        .execute_capability(request, &CapabilityCancellation::new())
        .await
        .unwrap();
    assert_eq!(result.status, CapabilityRequestStatus::Succeeded);
    assert_eq!(result.output, Some(json!({"message":"hello"})));
    let envelopes: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
    assert_eq!(envelopes.len(), 4);
    assert_eq!(
        envelopes
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            "capability.requested",
            "authorization.checked",
            "capability.started",
            "capability.succeeded"
        ]
    );
    let mut event_ids: Vec<_> = envelopes
        .iter()
        .map(|event| event.event_id.to_string())
        .collect();
    event_ids.sort_unstable();
    event_ids.dedup();
    assert_eq!(event_ids.len(), envelopes.len());
    assert!(envelopes.iter().all(|event| {
        event.mission_id == mission_id
            && event.runtime_id == runtime_id
            && event.parent_runtime_id.is_none()
    }));
    assert!(envelopes.iter().all(|event| match &event.payload {
        RuntimeEvent::CapabilityRequested { request_id: id, .. }
        | RuntimeEvent::AuthorizationChecked { request_id: id, .. }
        | RuntimeEvent::CapabilityStarted { request_id: id, .. }
        | RuntimeEvent::CapabilitySucceeded { request_id: id, .. } => *id == request_id,
        _ => false,
    }));
}

#[tokio::test]
async fn permission_denial_and_invalid_input_are_typed_and_audited() {
    let (kernel, mission_id, runtime_id, _dir) = runtime().await;
    let (capability_id, tool_id) = install_mock(&kernel).await;
    let mut events = kernel.events().subscribe_envelopes();
    let denied = kernel
        .execute_capability(
            request(
                mission_id,
                runtime_id,
                capability_id,
                tool_id,
                json!({"message":"x"}),
            ),
            &CapabilityCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status, CapabilityRequestStatus::Rejected);
    assert!(matches!(
        denied.error,
        Some(CapabilityError::PermissionDenied(_))
    ));
    grant(&kernel, mission_id, runtime_id, capability_id, tool_id).await;
    let failed = kernel
        .execute_capability(
            request(
                mission_id,
                runtime_id,
                capability_id,
                tool_id,
                json!({"message":4}),
            ),
            &CapabilityCancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(failed.status, CapabilityRequestStatus::Failed);
    assert!(matches!(
        failed.error,
        Some(CapabilityError::InvalidInput(_))
    ));
    let events: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
    assert!(events
        .iter()
        .any(|event| event.event_type == "capability.rejected"));
    assert!(events
        .iter()
        .any(|event| event.event_type == "capability.failed"));
}

struct SlowExecutor;

#[async_trait::async_trait]
impl CapabilityExecutor for SlowExecutor {
    async fn execute(
        &self,
        _request: CapabilityRequest,
    ) -> Result<serde_json::Value, CapabilityError> {
        tokio::time::sleep(Duration::from_millis(150)).await;
        Ok(json!({"done":true}))
    }
}

async fn install_slow(kernel: &Kernel) -> (CapabilityId, wyvern::ToolId) {
    let capability = CapabilityDescriptor::new("mock.slow", "1", "tests", CapabilityKind::Custom);
    let tool = ToolDescriptor::new(
        "wait",
        "test timeout",
        capability.id,
        json!({}),
        json!({}),
        RiskLevel::Low,
    );
    let ids = (capability.id, tool.id);
    kernel
        .capabilities()
        .register(capability, vec![tool], Arc::new(SlowExecutor))
        .await
        .unwrap();
    ids
}

#[tokio::test]
async fn timeout_and_cancellation_stop_waiting_and_have_distinct_states() {
    let (kernel, mission_id, runtime_id, _dir) = runtime().await;
    let (capability_id, tool_id) = install_slow(&kernel).await;
    grant(&kernel, mission_id, runtime_id, capability_id, tool_id).await;
    let mut events = kernel.events().subscribe_envelopes();
    let mut timeout_request = request(mission_id, runtime_id, capability_id, tool_id, json!({}));
    timeout_request.timeout_ms = Some(5);
    let timeout = kernel
        .execute_capability(timeout_request, &CapabilityCancellation::new())
        .await
        .unwrap();
    assert_eq!(timeout.status, CapabilityRequestStatus::TimedOut);
    assert_eq!(timeout.error, Some(CapabilityError::ExecutionTimeout));

    let cancel = CapabilityCancellation::new();
    let cancel_request = request(mission_id, runtime_id, capability_id, tool_id, json!({}));
    let kernel_copy = kernel.clone();
    let cancellation_copy = cancel.clone();
    let task = tokio::spawn(async move {
        kernel_copy
            .execute_capability(cancel_request, &cancellation_copy)
            .await
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(5)).await;
    cancel.cancel();
    let cancelled = task.await.unwrap();
    assert_eq!(cancelled.status, CapabilityRequestStatus::Cancelled);
    assert_eq!(cancelled.error, Some(CapabilityError::Cancelled));
    let envelopes: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
    assert_eq!(
        envelopes
            .iter()
            .filter(|event| event.event_type == "capability.cancelled")
            .count(),
        2
    );
    assert!(envelopes
        .iter()
        .all(|event| { event.mission_id == mission_id && event.runtime_id == runtime_id }));
    assert!(envelopes.iter().any(|event| matches!(
        event.payload,
        RuntimeEvent::CapabilityCancelled {
            timed_out: true,
            ..
        }
    )));
}

#[tokio::test]
async fn result_enters_context_or_memory_only_after_explicit_selection() {
    let (kernel, mission_id, runtime_id, _dir) = runtime().await;
    let (capability_id, tool_id) = install_mock(&kernel).await;
    grant(&kernel, mission_id, runtime_id, capability_id, tool_id).await;
    let request = request(
        mission_id,
        runtime_id,
        capability_id,
        tool_id,
        json!({"message":"remember"}),
    );
    let result = kernel
        .execute_capability(request.clone(), &CapabilityCancellation::new())
        .await
        .unwrap();
    assert!(kernel
        .context(runtime_id)
        .await
        .unwrap()
        .capability_results
        .is_empty());
    let context = kernel
        .compile_context_with_capability_results(
            runtime_id,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![result.clone()],
        )
        .await
        .unwrap();
    assert_eq!(context.capability_results, vec![result.clone()]);
    let draft = result
        .to_memory_draft(
            &request,
            MemoryScope::Runtime,
            MemoryType::Finding,
            MemoryProvenance {
                source_runtime_id: Some(runtime_id),
                source_agent_id: None,
                source_event_id: None,
                created_at: result.completed_at,
            },
        )
        .unwrap();
    assert_eq!(draft.runtime_id, Some(runtime_id));
    assert!(draft.content.contains("remember"));
    let mut local_context = RuntimeContext::default();
    result.include_in_context(&mut local_context);
    assert_eq!(local_context.capability_results, vec![result]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_requests_execute_concurrently_without_global_execution_lock() {
    let (kernel, mission_id, runtime_id, _dir) = runtime().await;
    let (capability_id, tool_id) = install_slow(&kernel).await;
    grant(&kernel, mission_id, runtime_id, capability_id, tool_id).await;
    let k1 = kernel.clone();
    let k2 = kernel.clone();
    let c1 = CapabilityCancellation::new();
    let c2 = CapabilityCancellation::new();
    let r1 = request(mission_id, runtime_id, capability_id, tool_id, json!({}));
    let r2 = request(mission_id, runtime_id, capability_id, tool_id, json!({}));
    let start = tokio::time::Instant::now();
    let (one, two) = tokio::join!(
        k1.execute_capability(r1, &c1),
        k2.execute_capability(r2, &c2),
    );
    assert_eq!(one.unwrap().status, CapabilityRequestStatus::Succeeded);
    assert_eq!(two.unwrap().status, CapabilityRequestStatus::Succeeded);
    assert!(start.elapsed() < Duration::from_millis(260));
}

#[test]
fn lifecycle_has_unambiguous_pending_running_and_terminal_states() {
    use CapabilityRequestStatus::*;
    let states = [
        Pending, Running, Succeeded, Failed, Cancelled, TimedOut, Rejected,
    ];
    for state in states {
        assert_eq!(
            serde_json::from_str::<CapabilityRequestStatus>(
                &serde_json::to_string(&state).unwrap()
            )
            .unwrap(),
            state
        );
    }
}

#[tokio::test]
async fn cli_lists_and_inspects_the_inert_mock_capability_and_tool() {
    let dir = tempfile::tempdir().unwrap();
    let exe = std::env::var("CARGO_BIN_EXE_wyvern").unwrap();
    let database = dir.path().join("cli.sqlite");
    let capability_list = Command::new(&exe)
        .arg("--database")
        .arg(&database)
        .args(["capability", "list"])
        .output()
        .unwrap();
    assert!(capability_list.status.success());
    assert!(String::from_utf8_lossy(&capability_list.stdout).contains("mock.echo"));
    let capability = Command::new(&exe)
        .arg("--database")
        .arg(&database)
        .args(["capability", "inspect", "mock.echo"])
        .output()
        .unwrap();
    assert!(capability.status.success());
    assert!(String::from_utf8_lossy(&capability.stdout).contains("echo"));
    let tools = Command::new(&exe)
        .arg("--database")
        .arg(&database)
        .args(["tool", "list"])
        .output()
        .unwrap();
    assert!(tools.status.success());
    assert!(String::from_utf8_lossy(&tools.stdout).contains("echo"));
    let tool = Command::new(&exe)
        .arg("--database")
        .arg(&database)
        .args(["tool", "inspect", "echo"])
        .output()
        .unwrap();
    assert!(tool.status.success());
    assert!(String::from_utf8_lossy(&tool.stdout).contains("mock.echo"));
}
