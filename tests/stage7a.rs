use std::time::Duration;

use serde_json::json;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::authorization::{AuthorizationGrantDraft, AuthorizationIssuer, AuthorizationScope};
use wyvern::capability::{
    mock_echo_definition, CapabilityCancellation, CapabilityDescriptor, CapabilityRequest,
    CapabilityRequestStatus, RiskLevel, ToolDescriptor,
};
use wyvern::domain::{now_millis, AgentId, CapabilityId, MissionId, RuntimeId};
use wyvern::events::RuntimeEvent;
use wyvern::kernel::Kernel;
use wyvern::{GrantId, ToolId, WyvernError};

struct Rig {
    _directory: tempfile::TempDir,
    kernel: Kernel,
    mission_a: MissionId,
    mission_b: MissionId,
    agent_a: AgentId,
    agent_b: AgentId,
    runtime_a: RuntimeId,
    runtime_a2: RuntimeId,
    runtime_b: RuntimeId,
    runtime_other_mission: RuntimeId,
    capability_a: CapabilityId,
    tool_low: ToolId,
    tool_critical: ToolId,
    capability_b: CapabilityId,
    tool_b: ToolId,
}

async fn rig() -> Rig {
    let directory = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite(directory.path().join("stage7a.sqlite"))
        .await
        .unwrap();
    let mission_a = kernel
        .create_mission("authorization mission A")
        .await
        .unwrap();
    let mission_b = kernel
        .create_mission("authorization mission B")
        .await
        .unwrap();
    let agent_a = kernel
        .register_agent(AgentDefinition::new("subject A", AgentKind::WorkerA))
        .await
        .unwrap();
    let agent_b = kernel
        .register_agent(AgentDefinition::new("subject B", AgentKind::WorkerB))
        .await
        .unwrap();
    let runtime_a = kernel
        .create_runtime(mission_a.id, None, &agent_a, "runtime A")
        .await
        .unwrap();
    let runtime_a2 = kernel
        .create_runtime(mission_a.id, None, &agent_a, "runtime A2")
        .await
        .unwrap();
    let runtime_b = kernel
        .create_runtime(mission_a.id, None, &agent_b, "runtime B")
        .await
        .unwrap();
    let runtime_other_mission = kernel
        .create_runtime(mission_b.id, None, &agent_a, "runtime other mission")
        .await
        .unwrap();

    let (capability, low_tool, executor) = mock_echo_definition();
    let critical_tool = ToolDescriptor::new(
        "critical-echo",
        "test tool with critical risk metadata",
        capability.id,
        json!({}),
        json!({}),
        RiskLevel::Critical,
    );
    kernel
        .capabilities()
        .register(
            capability.clone(),
            vec![low_tool.clone(), critical_tool.clone()],
            executor,
        )
        .await
        .unwrap();
    let (capability_b, tool_b, executor_b) = mock_echo_definition();
    kernel
        .capabilities()
        .register(capability_b.clone(), vec![tool_b.clone()], executor_b)
        .await
        .unwrap();

    Rig {
        _directory: directory,
        kernel,
        mission_a: mission_a.id,
        mission_b: mission_b.id,
        agent_a: agent_a.id,
        agent_b: agent_b.id,
        runtime_a: runtime_a.id(),
        runtime_a2: runtime_a2.id(),
        runtime_b: runtime_b.id(),
        runtime_other_mission: runtime_other_mission.id(),
        capability_a: capability.id,
        tool_low: low_tool.id,
        tool_critical: critical_tool.id,
        capability_b: capability_b.id,
        tool_b: tool_b.id,
    }
}

fn draft(
    issuer_runtime_id: RuntimeId,
    subject_agent_id: AgentId,
    capability_id: CapabilityId,
    tool_id: ToolId,
    scope: AuthorizationScope,
    maximum_risk: RiskLevel,
    expires_at: Option<i64>,
) -> AuthorizationGrantDraft {
    AuthorizationGrantDraft {
        issuer: AuthorizationIssuer::Runtime(issuer_runtime_id),
        subject_agent_id,
        capability_id,
        allowed_tools: vec![tool_id],
        scope,
        maximum_risk,
        expires_at,
    }
}

async fn issue(
    kernel: &Kernel,
    issuer_runtime_id: RuntimeId,
    agent_id: AgentId,
    capability_id: CapabilityId,
    tool_id: ToolId,
    scope: AuthorizationScope,
    maximum_risk: RiskLevel,
    expires_at: Option<i64>,
) -> GrantId {
    kernel
        .issue_authorization_grant(
            issuer_runtime_id,
            draft(
                issuer_runtime_id,
                agent_id,
                capability_id,
                tool_id,
                scope,
                maximum_risk,
                expires_at,
            ),
        )
        .await
        .unwrap()
        .id
}

fn request(
    mission_id: MissionId,
    runtime_id: RuntimeId,
    capability_id: CapabilityId,
    tool_id: ToolId,
    grant_id: Option<GrantId>,
) -> CapabilityRequest {
    let mut request = CapabilityRequest::new(
        runtime_id,
        mission_id,
        capability_id,
        tool_id,
        json!({"message":"authorized"}),
    );
    request.authorization_grant_id = grant_id;
    request
}

async fn invoke(kernel: &Kernel, request: CapabilityRequest) -> wyvern::CapabilityResult {
    kernel
        .execute_capability(request, &CapabilityCancellation::new())
        .await
        .unwrap()
}

#[tokio::test]
async fn grants_bind_agent_scope_capability_tool_and_risk() {
    let rig = rig().await;
    let grant_id = issue(
        &rig.kernel,
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Runtime(rig.runtime_a),
        RiskLevel::Moderate,
        None,
    )
    .await;

    let allowed = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_a,
            rig.capability_a,
            rig.tool_low,
            Some(grant_id),
        ),
    )
    .await;
    assert_eq!(allowed.status, CapabilityRequestStatus::Succeeded);

    let wrong_runtime = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_a2,
            rig.capability_a,
            rig.tool_low,
            Some(grant_id),
        ),
    )
    .await;
    assert_eq!(wrong_runtime.status, CapabilityRequestStatus::Rejected);

    let wrong_agent = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_b,
            rig.capability_a,
            rig.tool_low,
            Some(grant_id),
        ),
    )
    .await;
    assert_eq!(wrong_agent.status, CapabilityRequestStatus::Rejected);

    let wrong_tool = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_a,
            rig.capability_a,
            rig.tool_critical,
            Some(grant_id),
        ),
    )
    .await;
    assert_eq!(wrong_tool.status, CapabilityRequestStatus::Rejected);

    let wrong_capability = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_a,
            rig.capability_b,
            rig.tool_b,
            Some(grant_id),
        ),
    )
    .await;
    assert_eq!(wrong_capability.status, CapabilityRequestStatus::Rejected);

    let high_risk_id = issue(
        &rig.kernel,
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_critical,
        AuthorizationScope::Runtime(rig.runtime_a),
        RiskLevel::Moderate,
        None,
    )
    .await;
    let excessive_risk = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_a,
            rig.capability_a,
            rig.tool_critical,
            Some(high_risk_id),
        ),
    )
    .await;
    assert_eq!(excessive_risk.status, CapabilityRequestStatus::Rejected);

    let wrong_scope_id = issue(
        &rig.kernel,
        rig.runtime_other_mission,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Mission(rig.mission_b),
        RiskLevel::Critical,
        None,
    )
    .await;
    let wrong_scope = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_a,
            rig.capability_a,
            rig.tool_low,
            Some(wrong_scope_id),
        ),
    )
    .await;
    assert_eq!(wrong_scope.status, CapabilityRequestStatus::Rejected);
}

#[tokio::test]
async fn no_grant_unknown_grant_and_wrong_scope_are_denied_and_audited() {
    let rig = rig().await;
    let mission_scope_grant = issue(
        &rig.kernel,
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Mission(rig.mission_a),
        RiskLevel::Low,
        None,
    )
    .await;
    let no_grant = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_b,
            rig.capability_a,
            rig.tool_low,
            None,
        ),
    )
    .await;
    assert_eq!(no_grant.status, CapabilityRequestStatus::Rejected);

    let unknown = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_a,
            rig.capability_a,
            rig.tool_low,
            Some(GrantId::new()),
        ),
    )
    .await;
    assert_eq!(unknown.status, CapabilityRequestStatus::Rejected);

    let wrong_scope = invoke(
        &rig.kernel,
        request(
            rig.mission_b,
            rig.runtime_other_mission,
            rig.capability_a,
            rig.tool_low,
            Some(mission_scope_grant),
        ),
    )
    .await;
    assert_eq!(wrong_scope.status, CapabilityRequestStatus::Rejected);

    let history = rig.kernel.persisted_events().await.unwrap();
    assert!(history.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::AuthorizationChecked { allowed: false, .. }
    )));
    assert!(history.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::AuthorizationDenied { .. }
    )));
    assert!(history.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::AuthorizationGranted { .. }
    )));
}

#[tokio::test]
async fn invalid_and_duplicate_grants_are_rejected() {
    let rig = rig().await;
    let empty = draft(
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Mission(rig.mission_a),
        RiskLevel::Low,
        None,
    );
    let mut empty = empty;
    empty.allowed_tools.clear();
    assert!(matches!(
        rig.kernel
            .issue_authorization_grant(rig.runtime_a, empty)
            .await,
        Err(WyvernError::InvalidAuthorizationGrant(_))
    ));

    let wrong_runtime_subject = draft(
        rig.runtime_a,
        rig.agent_b,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Runtime(rig.runtime_a),
        RiskLevel::Low,
        None,
    );
    assert!(matches!(
        rig.kernel
            .issue_authorization_grant(rig.runtime_a, wrong_runtime_subject)
            .await,
        Err(WyvernError::InvalidAuthorizationGrant(_))
    ));

    let first = draft(
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Mission(rig.mission_a),
        RiskLevel::Low,
        None,
    );
    rig.kernel
        .issue_authorization_grant(rig.runtime_a, first.clone())
        .await
        .unwrap();
    assert!(matches!(
        rig.kernel
            .issue_authorization_grant(rig.runtime_a, first)
            .await,
        Err(WyvernError::DuplicateAuthorizationGrant(_))
    ));
}

#[tokio::test]
async fn expiry_and_revocation_deny_invocation() {
    let rig = rig().await;
    let expiring_id = issue(
        &rig.kernel,
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Runtime(rig.runtime_a),
        RiskLevel::Low,
        Some(now_millis() + 30),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(40)).await;
    let expired = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_a,
            rig.capability_a,
            rig.tool_low,
            Some(expiring_id),
        ),
    )
    .await;
    assert_eq!(expired.status, CapabilityRequestStatus::Rejected);

    let revocable_id = issue(
        &rig.kernel,
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Mission(rig.mission_a),
        RiskLevel::Low,
        None,
    )
    .await;
    let revoked = rig
        .kernel
        .revoke_authorization_grant(rig.runtime_a, revocable_id)
        .await
        .unwrap();
    assert!(revoked.revoked_at.is_some());
    let result = invoke(
        &rig.kernel,
        request(
            rig.mission_a,
            rig.runtime_a,
            rig.capability_a,
            rig.tool_low,
            Some(revocable_id),
        ),
    )
    .await;
    assert_eq!(result.status, CapabilityRequestStatus::Rejected);
    let history = rig.kernel.persisted_events().await.unwrap();
    assert!(history.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::AuthorizationRevoked { grant_id, .. } if grant_id == revocable_id
    )));
}

#[tokio::test]
async fn cancellation_requested_before_executor_subscription_is_retained() {
    let rig = rig().await;
    let grant_id = issue(
        &rig.kernel,
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Runtime(rig.runtime_a),
        RiskLevel::Low,
        None,
    )
    .await;
    let request = request(
        rig.mission_a,
        rig.runtime_a,
        rig.capability_a,
        rig.tool_low,
        Some(grant_id),
    );
    let request_id = request.request_id;
    let cancellation = CapabilityCancellation::new();
    cancellation.cancel();
    let result = rig
        .kernel
        .execute_capability(request, &cancellation)
        .await
        .unwrap();
    assert_eq!(result.status, CapabilityRequestStatus::Cancelled);
    assert_eq!(result.error, Some(wyvern::CapabilityError::Cancelled));
    let history = rig.kernel.events_for_runtime(rig.runtime_a).await.unwrap();
    assert!(history.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::CapabilityCancelled { request_id: id, .. } if id == request_id
    )));
}

#[tokio::test]
async fn grants_and_authorization_history_survive_kernel_restart() {
    let rig = rig().await;
    let database = rig._directory.path().join("stage7a.sqlite");
    let grant_id = issue(
        &rig.kernel,
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Runtime(rig.runtime_a),
        RiskLevel::Low,
        None,
    )
    .await;
    let old_runtime_id = rig.runtime_a;
    let mission_id = rig.mission_a;
    let agent_id = rig.agent_a;
    let capability_id = rig.capability_a;
    let tool_id = rig.tool_low;
    drop(rig);

    let reopened = Kernel::new_sqlite(&database).await.unwrap();
    let grant = reopened.authorization_grant(grant_id).await.unwrap();
    assert_eq!(grant.subject_agent_id, agent_id);
    assert_eq!(grant.capability_id, capability_id);
    assert_eq!(grant.allowed_tools, vec![tool_id]);
    let (capability, tool, executor) = mock_echo_definition();
    // Preserve the persisted IDs when rebuilding the process-local executor registry.
    let capability = CapabilityDescriptor {
        id: capability_id,
        ..capability
    };
    let tool = ToolDescriptor {
        id: tool_id,
        capability_id,
        ..tool
    };
    reopened
        .capabilities()
        .register(capability, vec![tool], executor)
        .await
        .unwrap();
    let result = invoke(
        &reopened,
        request(
            mission_id,
            old_runtime_id,
            capability_id,
            tool_id,
            Some(grant_id),
        ),
    )
    .await;
    assert_eq!(result.status, CapabilityRequestStatus::Succeeded);
    let history = reopened.persisted_events().await.unwrap();
    assert!(history.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::AuthorizationGranted { grant_id: id, .. } if id == grant_id
    )));
    assert!(history.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::AuthorizationChecked { allowed: true, .. }
    )));
}

#[tokio::test]
async fn grant_revocation_survives_restart() {
    let rig = rig().await;
    let database = rig._directory.path().join("stage7a.sqlite");
    let grant_id = issue(
        &rig.kernel,
        rig.runtime_a,
        rig.agent_a,
        rig.capability_a,
        rig.tool_low,
        AuthorizationScope::Runtime(rig.runtime_a),
        RiskLevel::Low,
        None,
    )
    .await;
    rig.kernel
        .revoke_authorization_grant(rig.runtime_a, grant_id)
        .await
        .unwrap();
    let runtime_id = rig.runtime_a;
    let mission_id = rig.mission_a;
    let capability_id = rig.capability_a;
    let tool_id = rig.tool_low;
    drop(rig);

    let reopened = Kernel::new_sqlite(&database).await.unwrap();
    let (capability, tool, executor) = mock_echo_definition();
    reopened
        .capabilities()
        .register(
            CapabilityDescriptor {
                id: capability_id,
                ..capability
            },
            vec![ToolDescriptor {
                id: tool_id,
                capability_id,
                ..tool
            }],
            executor,
        )
        .await
        .unwrap();
    let result = invoke(
        &reopened,
        request(
            mission_id,
            runtime_id,
            capability_id,
            tool_id,
            Some(grant_id),
        ),
    )
    .await;
    assert_eq!(result.status, CapabilityRequestStatus::Rejected);
    assert!(reopened
        .authorization_grant(grant_id)
        .await
        .unwrap()
        .revoked_at
        .is_some());
}
