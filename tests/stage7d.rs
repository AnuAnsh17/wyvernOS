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

#[tokio::test]
async fn authorized_filesystem_reads_are_independent_and_concurrent() {
    let rig = rig(1024).await;
    let grant_id = grant(&rig).await;
    let (first, second, third) = tokio::join!(
        read(&rig, "hello.txt", Some(grant_id)),
        read(&rig, "data.bin", Some(grant_id)),
        read(&rig, "hello.txt", Some(grant_id)),
    );
    assert_eq!(first.status, CapabilityRequestStatus::Succeeded);
    assert_eq!(second.status, CapabilityRequestStatus::Succeeded);
    assert_eq!(third.status, CapabilityRequestStatus::Succeeded);
}

#[tokio::test]
async fn filesystem_read_can_be_registered_again_after_kernel_restart() {
    let rig = rig(1024).await;
    let grant_id = grant(&rig).await;
    let database = rig._directory.path().join("stage7d.sqlite");
    let workspace = rig.workspace.path().to_path_buf();
    let mission_id = rig.mission_id;
    let runtime_id = rig.runtime_id;
    let agent_id = rig.agent_id;
    let capability_id = rig.capability_id;
    let tool_id = rig.tool_id;
    drop(rig.kernel);

    let reopened = Kernel::new_sqlite(&database).await.unwrap();
    let (capability, tool, executor) = filesystem_read_definition(workspace, 1024);
    reopened
        .capabilities()
        .register(
            wyvern::CapabilityDescriptor {
                id: capability_id,
                ..capability
            },
            vec![wyvern::ToolDescriptor {
                id: tool_id,
                capability_id,
                ..tool
            }],
            executor,
        )
        .await
        .unwrap();
    let mut request = CapabilityRequest::new(
        runtime_id,
        mission_id,
        capability_id,
        tool_id,
        json!({"path": "hello.txt"}),
    );
    request.authorization_grant_id = Some(grant_id);
    let result = reopened
        .execute_capability(request, &CapabilityCancellation::new())
        .await
        .unwrap();
    assert_eq!(result.status, CapabilityRequestStatus::Succeeded);
    assert_eq!(reopened.agent(agent_id).await.unwrap().id, agent_id);
}

#[cfg(unix)]
fn create_file_symlink(source: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(source, link)
}

#[cfg(windows)]
fn create_file_symlink(source: &std::path::Path, link: &std::path::Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(source, link)
}

#[tokio::test]
async fn filesystem_read_rejects_symlink_escape() {
    let rig = rig(1024).await;
    let outside = tempfile::tempdir().unwrap();
    let outside_file = outside.path().join("secret.txt");
    std::fs::write(&outside_file, "outside workspace").unwrap();
    let link = rig.workspace.path().join("outside.txt");
    if create_file_symlink(&outside_file, &link).is_err() {
        return;
    }
    let result = read(&rig, "outside.txt", Some(grant(&rig).await)).await;
    assert_eq!(result.status, CapabilityRequestStatus::Failed);
}
