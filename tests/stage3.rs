use std::path::PathBuf;
use std::process::Command;

use tempfile::TempDir;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::context::ContextCompiler;
use wyvern::domain::{AgentId, MissionId, RuntimeId};
use wyvern::events::RuntimeEvent;
use wyvern::kernel::{Kernel, RuntimeHandle};
use wyvern::memory::{
    MemoryDraft, MemoryMetadataValue, MemoryProvenance, MemoryScope, MemoryStatus, MemoryType,
};

async fn setup() -> (
    Kernel,
    TempDir,
    PathBuf,
    MissionId,
    RuntimeHandle,
    RuntimeHandle,
    RuntimeHandle,
) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stage3.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("memory substrate").await.unwrap();
    let root_agent = kernel
        .register_agent(AgentDefinition::new("root", AgentKind::Commander))
        .await;
    let branch_agent = kernel
        .register_agent(AgentDefinition::new("branch", AgentKind::WorkerA))
        .await;
    let root = kernel
        .create_runtime(mission.id, None, &root_agent, "root")
        .await
        .unwrap();
    let branch_a = kernel
        .spawn_child(root.id(), &branch_agent, "branch a")
        .await
        .unwrap();
    let branch_b = kernel
        .spawn_child(root.id(), &branch_agent, "branch b")
        .await
        .unwrap();
    (kernel, dir, db, mission.id, root, branch_a, branch_b)
}

fn draft(
    scope: MemoryScope,
    mission_id: MissionId,
    source_runtime_id: RuntimeId,
    memory_type: MemoryType,
    content: &str,
) -> MemoryDraft {
    let mut draft = MemoryDraft::new(
        scope,
        mission_id,
        memory_type,
        content,
        MemoryProvenance {
            source_runtime_id: Some(source_runtime_id),
            source_agent_id: None,
            source_event_id: Some("event-source-1".into()),
            created_at: 1234,
        },
    );
    match scope {
        MemoryScope::Mission => {}
        MemoryScope::Runtime => draft.runtime_id = Some(source_runtime_id),
        MemoryScope::Branch => draft.branch_id = Some(source_runtime_id),
        MemoryScope::Agent => draft.agent_id = Some(AgentId::new()),
    }
    draft
}

#[tokio::test]
async fn typed_memory_creation_query_and_provenance_work() {
    let (kernel, _dir, _db, mission_id, root, _, _) = setup().await;
    let mut memory = draft(
        MemoryScope::Runtime,
        mission_id,
        root.id(),
        MemoryType::Finding,
        "endpoint accepts parameter q",
    );
    memory
        .metadata
        .insert("verified".into(), MemoryMetadataValue::Boolean(true));
    let created = kernel.create_memory(memory).await.unwrap();
    assert!(created.memory_id.to_string().starts_with("mem_"));
    assert_eq!(created.scope, MemoryScope::Runtime);
    assert_eq!(created.memory_type, MemoryType::Finding);
    assert_eq!(created.provenance.source_runtime_id, Some(root.id()));
    assert_eq!(
        kernel.memories().get(created.memory_id).await.unwrap(),
        Some(created.clone())
    );
    let queried = kernel
        .memories()
        .query(wyvern::MemoryQuery {
            mission_id: Some(mission_id),
            memory_types: vec![MemoryType::Finding],
            statuses: vec![MemoryStatus::Active],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(queried, vec![created]);
}

#[tokio::test]
async fn memory_scope_visibility_prevents_sibling_leakage() {
    let (kernel, _dir, _db, mission_id, root, branch_a, branch_b) = setup().await;
    let leaf_agent = kernel
        .register_agent(AgentDefinition::new("leaf", AgentKind::WorkerA1))
        .await;
    let leaf_a = kernel
        .spawn_child(branch_a.id(), &leaf_agent, "leaf a")
        .await
        .unwrap();

    let mut branch_memory = draft(
        MemoryScope::Branch,
        mission_id,
        branch_a.id(),
        MemoryType::Finding,
        "branch A finding",
    );
    branch_memory.branch_id = Some(branch_a.id());
    let branch_item = kernel.create_memory(branch_memory).await.unwrap();
    let mut runtime_memory = draft(
        MemoryScope::Runtime,
        mission_id,
        branch_a.id(),
        MemoryType::Failure,
        "private runtime attempt",
    );
    runtime_memory.runtime_id = Some(branch_a.id());
    let private_item = kernel.create_memory(runtime_memory).await.unwrap();
    let shared = kernel
        .create_memory(draft(
            MemoryScope::Mission,
            mission_id,
            root.id(),
            MemoryType::Constraint,
            "stay in authorized scope",
        ))
        .await
        .unwrap();
    let agent_id = kernel.get_runtime(leaf_a.id()).await.unwrap().agent_id;
    let mut agent_memory = draft(
        MemoryScope::Agent,
        mission_id,
        branch_a.id(),
        MemoryType::Decision,
        "worker role policy",
    );
    agent_memory.agent_id = Some(agent_id);
    let agent_item = kernel.create_memory(agent_memory).await.unwrap();

    let a_visible = kernel.visible_memories(leaf_a.id()).await.unwrap();
    let b_visible = kernel.visible_memories(branch_b.id()).await.unwrap();
    assert!(a_visible
        .iter()
        .any(|item| item.memory_id == branch_item.memory_id));
    assert!(a_visible
        .iter()
        .any(|item| item.memory_id == shared.memory_id));
    assert!(!a_visible
        .iter()
        .any(|item| item.memory_id == private_item.memory_id));
    assert!(b_visible
        .iter()
        .any(|item| item.memory_id == shared.memory_id));
    assert!(!b_visible
        .iter()
        .any(|item| item.memory_id == branch_item.memory_id));
    assert!(!b_visible
        .iter()
        .any(|item| item.memory_id == private_item.memory_id));
    assert!(a_visible
        .iter()
        .any(|item| item.memory_id == agent_item.memory_id));
    assert!(!b_visible
        .iter()
        .any(|item| item.memory_id == agent_item.memory_id));
}

#[tokio::test]
async fn supersede_invalidate_archive_and_history_preserve_records() {
    let (kernel, _dir, _db, mission_id, root, _, _) = setup().await;
    let mut receiver = kernel.events().subscribe_envelopes();
    let original = kernel
        .create_memory(draft(
            MemoryScope::Runtime,
            mission_id,
            root.id(),
            MemoryType::Finding,
            "parameter q appears injectable",
        ))
        .await
        .unwrap();
    let replacement = kernel
        .supersede_memory(
            original.memory_id,
            draft(
                MemoryScope::Runtime,
                mission_id,
                root.id(),
                MemoryType::Finding,
                "retest found no exploitable behavior",
            ),
        )
        .await
        .unwrap();
    assert_eq!(replacement.version, original.version + 1);
    assert_eq!(replacement.supersedes_memory_id, Some(original.memory_id));
    let old = kernel
        .memories()
        .get(original.memory_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old.status, MemoryStatus::Superseded);
    assert_eq!(old.superseded_by_memory_id, Some(replacement.memory_id));
    assert_eq!(old.content, original.content);
    let history = kernel
        .memories()
        .history(replacement.memory_id)
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert!(history
        .iter()
        .any(|item| item.memory_id == original.memory_id));

    let invalidated = kernel
        .create_memory(draft(
            MemoryScope::Runtime,
            mission_id,
            root.id(),
            MemoryType::Observation,
            "temporary observation",
        ))
        .await
        .unwrap();
    let invalidator = kernel
        .create_memory(draft(
            MemoryScope::Runtime,
            mission_id,
            root.id(),
            MemoryType::Finding,
            "later evidence invalidates the observation",
        ))
        .await
        .unwrap();
    let invalidated = kernel
        .memories()
        .invalidate_by(invalidated.memory_id, invalidator.memory_id)
        .await
        .unwrap();
    assert_eq!(invalidated.status, MemoryStatus::Invalidated);
    assert_eq!(
        invalidated.invalidated_by_memory_id,
        Some(invalidator.memory_id)
    );
    assert_eq!(
        kernel
            .memories()
            .history(invalidated.memory_id)
            .await
            .unwrap()
            .len(),
        2
    );
    let archived = kernel
        .create_memory(draft(
            MemoryScope::Runtime,
            mission_id,
            root.id(),
            MemoryType::Note,
            "old note",
        ))
        .await
        .unwrap();
    assert_eq!(
        kernel
            .memories()
            .archive(archived.memory_id)
            .await
            .unwrap()
            .status,
        MemoryStatus::Archived
    );

    let envelopes: Vec<_> = std::iter::from_fn(|| receiver.try_recv().ok()).collect();
    let mut event_ids: Vec<_> = envelopes.iter().map(|event| &event.event_id).collect();
    event_ids.sort();
    event_ids.dedup();
    assert_eq!(event_ids.len(), envelopes.len());
    assert!(envelopes
        .iter()
        .all(|event| event.mission_id == mission_id && event.runtime_id == root.id()));
    assert!(envelopes
        .iter()
        .any(|event| event.event_type == "memory.created"));
    assert!(envelopes
        .iter()
        .any(|event| event.event_type == "memory.superseded"));
    assert!(envelopes
        .iter()
        .any(|event| event.event_type == "memory.invalidated"));
    assert!(envelopes
        .iter()
        .any(|event| event.event_type == "memory.archived"));
    assert!(envelopes.iter().all(|event| event.runtime_id == root.id()));
}

#[tokio::test]
async fn promotion_copies_memory_to_parent_and_keeps_sibling_private() {
    let (kernel, _dir, _db, mission_id, root, branch_a, branch_b) = setup().await;
    let leaf_agent = kernel
        .register_agent(AgentDefinition::new("leaf", AgentKind::WorkerA1))
        .await;
    let leaf = kernel
        .spawn_child(branch_a.id(), &leaf_agent, "leaf")
        .await
        .unwrap();
    let mut child_draft = draft(
        MemoryScope::Runtime,
        mission_id,
        leaf.id(),
        MemoryType::Failure,
        "tested q with methods x and y; no injection observed",
    );
    child_draft.runtime_id = Some(leaf.id());
    let source = kernel.create_memory(child_draft).await.unwrap();
    let mut events = kernel.events().subscribe_envelopes();
    let promoted = kernel
        .promote_memory(source.memory_id, branch_a.id())
        .await
        .unwrap();
    assert_ne!(promoted.memory_id, source.memory_id);
    assert_eq!(promoted.promoted_from_memory_id, Some(source.memory_id));
    assert_eq!(promoted.runtime_id, Some(branch_a.id()));
    assert_eq!(promoted.provenance, source.provenance);
    assert_eq!(
        kernel.memories().get(source.memory_id).await.unwrap(),
        Some(source.clone())
    );
    assert!(kernel
        .visible_memories(branch_a.id())
        .await
        .unwrap()
        .iter()
        .any(|item| item.memory_id == promoted.memory_id));
    assert!(!kernel
        .visible_memories(branch_b.id())
        .await
        .unwrap()
        .iter()
        .any(|item| item.memory_id == promoted.memory_id));
    assert!(kernel
        .visible_memories(root.id())
        .await
        .unwrap()
        .iter()
        .all(|item| item.memory_id != promoted.memory_id));
    let envelopes: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
    assert!(envelopes.iter().any(|envelope| matches!(
        envelope.payload,
        RuntimeEvent::MemoryPromoted { memory_id, source_memory_id, .. }
            if memory_id == promoted.memory_id && source_memory_id == source.memory_id
    )));
}

#[tokio::test]
async fn only_explicitly_selected_memory_enters_compiled_context() {
    let (kernel, _dir, _db, mission_id, _root, branch_a, _) = setup().await;
    let selected = kernel
        .create_memory(draft(
            MemoryScope::Runtime,
            mission_id,
            branch_a.id(),
            MemoryType::Fact,
            "selected fact",
        ))
        .await
        .unwrap();
    let _unselected = kernel
        .create_memory(draft(
            MemoryScope::Runtime,
            mission_id,
            branch_a.id(),
            MemoryType::Failure,
            "unselected failure",
        ))
        .await
        .unwrap();
    let context = kernel
        .compile_context_with_memory(
            branch_a.id(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            vec![selected.clone()],
        )
        .await
        .unwrap();
    assert_eq!(context.selected_memories, vec![selected.clone()]);
    let compiled = ContextCompiler::compile(wyvern::context::ContextCompileInput {
        selected_memories: vec![selected.clone()],
        ..Default::default()
    });
    assert_eq!(
        compiled.selected_memories[0].provenance,
        selected.provenance
    );

    let mut legacy = serde_json::to_value(wyvern::RuntimeContext::default()).unwrap();
    legacy.as_object_mut().unwrap().remove("selected_memories");
    let recovered: wyvern::RuntimeContext = serde_json::from_value(legacy).unwrap();
    assert!(recovered.selected_memories.is_empty());
}

#[tokio::test]
async fn memory_history_and_provenance_survive_database_reopen() {
    let (kernel, dir, db, mission_id, root, _, _) = setup().await;
    let mut original_draft = draft(
        MemoryScope::Runtime,
        mission_id,
        root.id(),
        MemoryType::Finding,
        "initial finding",
    );
    original_draft.provenance.source_agent_id = Some(root_agent_id(&kernel, root.id()).await);
    let original = kernel.create_memory(original_draft).await.unwrap();
    let replacement = kernel
        .supersede_memory(
            original.memory_id,
            draft(
                MemoryScope::Runtime,
                mission_id,
                root.id(),
                MemoryType::Finding,
                "revised finding",
            ),
        )
        .await
        .unwrap();
    drop(kernel);

    let reopened = Kernel::new_sqlite(&db).await.unwrap();
    let recovered = reopened
        .memories()
        .get(replacement.memory_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(recovered.supersedes_memory_id, Some(original.memory_id));
    assert_eq!(recovered.scope, original.scope);
    assert_eq!(
        recovered.provenance.source_event_id.as_deref(),
        Some("event-source-1")
    );
    assert_eq!(
        reopened
            .memories()
            .get(original.memory_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        MemoryStatus::Superseded
    );
    assert_eq!(
        reopened
            .memories()
            .history(original.memory_id)
            .await
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        reopened.get_runtime(root.id()).await.unwrap().mission_id,
        mission_id
    );
    drop(reopened);
    drop(dir);
}

async fn root_agent_id(kernel: &Kernel, root_id: RuntimeId) -> AgentId {
    kernel.get_runtime(root_id).await.unwrap().agent_id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_branch_memory_writes_keep_scope_and_provenance() {
    let (kernel, _dir, _db, mission_id, root, branch_a, branch_b) = setup().await;
    let leaf_agent = kernel
        .register_agent(AgentDefinition::new("leaf", AgentKind::WorkerA1))
        .await;
    let leaf_a1 = kernel
        .spawn_child(branch_a.id(), &leaf_agent, "a1")
        .await
        .unwrap();
    let leaf_a2 = kernel
        .spawn_child(branch_a.id(), &leaf_agent, "a2")
        .await
        .unwrap();
    let writers = [
        root.id(),
        branch_a.id(),
        branch_b.id(),
        leaf_a1.id(),
        leaf_a2.id(),
    ];
    let mut tasks = Vec::new();
    for (index, runtime_id) in writers.into_iter().enumerate() {
        let kernel = kernel.clone();
        tasks.push(tokio::spawn(async move {
            let draft = draft(
                MemoryScope::Runtime,
                mission_id,
                runtime_id,
                MemoryType::Observation,
                &format!("observation {index}"),
            );
            kernel.create_memory(draft).await.unwrap()
        }));
    }
    let mut created = Vec::new();
    for task in tasks {
        created.push(task.await.unwrap());
    }
    assert_eq!(
        kernel
            .memories()
            .query(wyvern::MemoryQuery {
                mission_id: Some(mission_id),
                ..Default::default()
            })
            .await
            .unwrap()
            .len(),
        5
    );
    for item in created {
        assert_eq!(item.mission_id, mission_id);
        assert_eq!(item.runtime_id, item.provenance.source_runtime_id);
        assert_eq!(
            kernel.memories().get(item.memory_id).await.unwrap(),
            Some(item)
        );
    }
    let branch_b_visible = kernel.visible_memories(branch_b.id()).await.unwrap();
    assert!(
        branch_b_visible
            .iter()
            .all(|item| item.runtime_id != Some(leaf_a1.id())
                && item.runtime_id != Some(leaf_a2.id()))
    );
}

#[tokio::test]
async fn memory_cli_inspection_commands_are_exposed() {
    let (kernel, _dir, db, mission_id, root, _, _) = setup().await;
    let memory = kernel
        .create_memory(draft(
            MemoryScope::Mission,
            mission_id,
            root.id(),
            MemoryType::Note,
            "CLI note",
        ))
        .await
        .unwrap();
    drop(kernel);
    let exe = std::env::var("CARGO_BIN_EXE_wyvern").unwrap();
    let output = Command::new(&exe)
        .arg("--database")
        .arg(&db)
        .arg("memory")
        .arg("inspect")
        .arg(memory.memory_id.to_string())
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("CLI note"));

    let listed = Command::new(&exe)
        .arg("--database")
        .arg(&db)
        .arg("memory")
        .arg("list")
        .arg(mission_id.to_string())
        .arg("mission")
        .output()
        .unwrap();
    assert!(listed.status.success());
    assert!(String::from_utf8_lossy(&listed.stdout).contains("CLI note"));

    let history = Command::new(&exe)
        .arg("--database")
        .arg(&db)
        .arg("memory")
        .arg("history")
        .arg(memory.memory_id.to_string())
        .output()
        .unwrap();
    assert!(history.status.success());
    assert!(String::from_utf8_lossy(&history.stdout).contains("CLI note"));
}
