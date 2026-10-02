use serde_json::json;
use tempfile::TempDir;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::authorization::{AuthorizationGrantDraft, AuthorizationIssuer, AuthorizationScope};
use wyvern::capability::{
    CapabilityCancellation, CapabilityRequest, CapabilityRequestStatus, RiskLevel,
};
use wyvern::domain::{AgentId, CapabilityId, MissionId, RuntimeId, ToolId};
use wyvern::kernel::Kernel;
use wyvern::{filesystem_read_definition, GrantId};

struct Rig {
    _directory: TempDir,
    workspace: TempDir,
    kernel: Kernel,
    mission_id: MissionId,
    runtime_id: RuntimeId,
    agent_id: AgentId,
    capability_id: CapabilityId,
    tool_id: ToolId,
}

async fn rig(max_bytes: u64) -> Rig {
    let directory = tempfile::tempdir().unwrap();
    let workspace = tempfile::tempdir().unwrap();
    std::fs::write(workspace.path().join("hello.txt"), "hello from wyvern").unwrap();
    std::fs::write(workspace.path().join("data.bin"), [0, 1, 2, 255]).unwrap();
    let kernel = Kernel::new_sqlite(directory.path().join("stage7d.sqlite"))
        .await
        .unwrap();
    let mission = kernel.create_mission("filesystem read").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new(
            "filesystem reader",
            AgentKind::WorkerA,
        ))
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "read workspace files")
        .await
        .unwrap();
    let (capability, tool, executor) = filesystem_read_definition(workspace.path(), max_bytes);
    kernel
        .capabilities()
        .register(capability.clone(), vec![tool.clone()], executor)
        .await
        .unwrap();
    Rig {
        _directory: directory,
        workspace,
        kernel,
        mission_id: mission.id,
        runtime_id: runtime.id(),
        agent_id: agent.id,
        capability_id: capability.id,
        tool_id: tool.id,
    }
}

async fn grant(rig: &Rig) -> GrantId {
    rig.kernel
        .issue_authorization_grant(
            rig.runtime_id,
            AuthorizationGrantDraft {
                issuer: AuthorizationIssuer::Runtime(rig.runtime_id),
                subject_agent_id: rig.agent_id,
                capability_id: rig.capability_id,
                allowed_tools: vec![rig.tool_id],
                scope: AuthorizationScope::Runtime(rig.runtime_id),
                maximum_risk: RiskLevel::Low,
                expires_at: None,
            },
        )
        .await
        .unwrap()
        .id
}

async fn read(rig: &Rig, path: &str, grant_id: Option<GrantId>) -> wyvern::CapabilityResult {
    let mut request = CapabilityRequest::new(
        rig.runtime_id,
        rig.mission_id,
        rig.capability_id,
        rig.tool_id,
        json!({"path": path}),
    );
    request.authorization_grant_id = grant_id;
    rig.kernel
        .execute_capability(request, &CapabilityCancellation::new())
        .await
        .unwrap()
}

#[tokio::test]
async fn authorized_filesystem_read_returns_text_and_binary_results() {
    let rig = rig(1024).await;
    let grant_id = grant(&rig).await;

    let text = read(&rig, "hello.txt", Some(grant_id)).await;
    assert_eq!(text.status, CapabilityRequestStatus::Succeeded);
    assert_eq!(text.output.unwrap()["content"], "hello from wyvern");

    let binary = read(&rig, "data.bin", Some(grant_id)).await;
    assert_eq!(binary.status, CapabilityRequestStatus::Succeeded);
    assert_eq!(binary.output.unwrap()["encoding"], "binary");
}

#[tokio::test]
async fn filesystem_read_rejects_unsafe_and_oversized_paths() {
    let rig = rig(4).await;
    let grant_id = grant(&rig).await;
    let absolute = rig
        .workspace
        .path()
        .join("hello.txt")
        .to_string_lossy()
        .into_owned();

    for path in [
        "../hello.txt",
        absolute.as_str(),
        "missing.txt",
        "hello.txt",
    ] {
        let result = read(&rig, path, Some(grant_id)).await;
        assert_eq!(
            result.status,
            CapabilityRequestStatus::Failed,
            "unexpected status for {path}"
        );
    }
}

#[tokio::test]
async fn filesystem_read_requires_a_kernel_grant() {
    let rig = rig(1024).await;
    let result = read(&rig, "hello.txt", None).await;
    assert_eq!(result.status, CapabilityRequestStatus::Rejected);
}
