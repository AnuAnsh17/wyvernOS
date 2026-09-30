use std::path::PathBuf;

use tempfile::TempDir;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::context::{
    ContextCompileInput, ContextCompiler, ContextProvenance, ContextSourceType, ContextValue,
};
use wyvern::domain::RuntimeId;
use wyvern::events::RuntimeEvent;
use wyvern::kernel::Kernel;
use wyvern::result::RuntimeResult;

async fn kernel() -> (Kernel, TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stage2.sqlite");
    (Kernel::new_sqlite(&db).await.unwrap(), dir, db)
}

#[tokio::test]
async fn context_compiler_is_deterministic_and_keeps_provenance() {
    let item = wyvern::ContextItem {
        key: "finding".into(),
        value: ContextValue::Text("local".into()),
        provenance: ContextProvenance {
            source_type: ContextSourceType::LocalContext,
            source_runtime_id: Some(RuntimeId::new()),
            source_event_id: None,
            created_at: 10,
        },
        priority: Some(3),
    };
    let input = ContextCompileInput {
        objective: "inspect branch".into(),
        local_facts: vec![item],
        ..Default::default()
    };
    assert_eq!(
        ContextCompiler::compile(input.clone()),
        ContextCompiler::compile(input)
    );
}

#[tokio::test]
async fn sibling_context_isolation_and_explicit_parent_propagation_work() {
    let (kernel, _dir, _db) = kernel().await;
    let mission = kernel.create_mission("branch isolation").await.unwrap();
    let root_agent = kernel
        .register_agent(AgentDefinition::new("root", AgentKind::Commander))
        .await
        .unwrap();
    let child_agent = kernel
        .register_agent(AgentDefinition::new("child", AgentKind::WorkerA))
        .await
        .unwrap();
    let leaf_agent = kernel
        .register_agent(AgentDefinition::new("leaf", AgentKind::WorkerA1))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &root_agent, "root objective")
        .await
        .unwrap();
    let branch_a = kernel
        .spawn_child(root.id(), &child_agent, "branch a")
        .await
        .unwrap();
    let branch_b = kernel
        .spawn_child(root.id(), &child_agent, "branch b")
        .await
        .unwrap();
    let grandchild = kernel
        .spawn_child(branch_a.id(), &leaf_agent, "leaf")
        .await
        .unwrap();

    kernel
        .add_context_fact(root.id(), "shared", ContextValue::Text("selected".into()))
        .await
        .unwrap();
    kernel
        .add_context_fact(
            root.id(),
            "private",
            ContextValue::Text("not inherited".into()),
        )
        .await
        .unwrap();
    kernel
        .inherit_context(root.id(), branch_a.id(), &["shared".into()])
        .await
        .unwrap();
    let inherited = kernel.context(branch_a.id()).await.unwrap();
    assert_eq!(inherited.inherited_context.len(), 1);
    assert_eq!(
        inherited.inherited_context[0].provenance.source_runtime_id,
        Some(root.id())
    );
    assert!(kernel
        .context(branch_b.id())
        .await
        .unwrap()
        .inherited_context
        .is_empty());

    kernel
        .add_context_fact(branch_a.id(), "branch_only", ContextValue::Boolean(true))
        .await
        .unwrap();
    assert!(kernel
        .context(root.id())
        .await
        .unwrap()
        .local_facts
        .iter()
        .all(|fact| fact.key != "branch_only"));
    assert!(kernel
        .context(branch_b.id())
        .await
        .unwrap()
        .local_facts
        .is_empty());

    let (fact_a, fact_b) = tokio::join!(
        kernel.add_context_fact(branch_a.id(), "parallel-a", ContextValue::Boolean(true)),
        kernel.add_context_fact(branch_b.id(), "parallel-b", ContextValue::Boolean(true)),
    );
    fact_a.unwrap();
    fact_b.unwrap();
    assert!(kernel
        .context(branch_a.id())
        .await
        .unwrap()
        .local_facts
        .iter()
        .any(|item| item.key == "parallel-a"));
    assert!(kernel
        .context(branch_b.id())
        .await
        .unwrap()
        .local_facts
        .iter()
        .any(|item| item.key == "parallel-b"));
    assert!(kernel
        .context(branch_a.id())
        .await
        .unwrap()
        .local_facts
        .iter()
        .all(|item| item.key != "parallel-b"));

    let child_result = RuntimeResult::completed(
        grandchild.id(),
        "structured",
        serde_json::json!({"count": 2}),
        vec![],
    );
    kernel
        .propagate_child_result(branch_a.id(), &child_result, "recon_findings")
        .await
        .unwrap();
    assert_eq!(
        kernel
            .context(branch_a.id())
            .await
            .unwrap()
            .child_outputs
            .len(),
        1
    );
    assert!(kernel
        .context(root.id())
        .await
        .unwrap()
        .child_outputs
        .is_empty());
    assert!(kernel
        .context(branch_b.id())
        .await
        .unwrap()
        .child_outputs
        .is_empty());
}

#[tokio::test]
async fn concurrent_updates_are_not_lost_and_snapshots_are_immutable() {
    let (kernel, _dir, _db) = kernel().await;
    let mission = kernel.create_mission("concurrent").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "objective")
        .await
        .unwrap();
    let mut tasks = Vec::new();
    for n in 0..32 {
        let kernel = kernel.clone();
        let id = runtime.id();
        tasks.push(tokio::spawn(async move {
            kernel
                .add_context_fact(id, format!("fact-{n}"), ContextValue::Number(n as f64))
                .await
                .unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(
        kernel
            .context(runtime.id())
            .await
            .unwrap()
            .local_facts
            .len(),
        32
    );

    let before = kernel
        .contexts()
        .create_snapshot(runtime.id(), None)
        .await
        .unwrap();
    kernel
        .add_context_fact(runtime.id(), "later", ContextValue::Boolean(true))
        .await
        .unwrap();
    let after = kernel
        .contexts()
        .latest_snapshot(runtime.id())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before, after);
    let next_snapshot = kernel
        .contexts()
        .create_snapshot(runtime.id(), None)
        .await
        .unwrap();
    assert!(next_snapshot.version > before.version);
    assert_eq!(
        kernel
            .context(runtime.id())
            .await
            .unwrap()
            .local_facts
            .len(),
        33
    );
}

#[tokio::test]
async fn context_and_snapshots_survive_reopen_and_events_have_envelopes() {
    let (kernel, _dir, db) = kernel().await;
    let receiver = kernel.events().subscribe_envelopes();
    let mission = kernel.create_mission("persist").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "objective")
        .await
        .unwrap();
    kernel
        .add_context_fact(runtime.id(), "durable", ContextValue::Text("value".into()))
        .await
        .unwrap();
    let snapshot = kernel
        .contexts()
        .create_snapshot(runtime.id(), None)
        .await
        .unwrap();

    let mut receiver = receiver;
    let mut envelopes = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        envelopes.push(event);
    }
    assert!(envelopes.len() >= 3);
    let mut event_ids: Vec<_> = envelopes.iter().map(|event| &event.event_id).collect();
    event_ids.sort();
    event_ids.dedup();
    assert_eq!(event_ids.len(), envelopes.len());
    let context_event = envelopes
        .iter()
        .find(|event| event.event_type == "context.updated")
        .unwrap();
    assert_eq!(context_event.mission_id, mission.id);
    assert_eq!(context_event.runtime_id, runtime.id());
    assert_eq!(context_event.event_type, context_event.payload.event_type());

    drop(kernel);
    let reopened = Kernel::new_sqlite(&db).await.unwrap();
    let context = reopened.context(runtime.id()).await.unwrap();
    assert_eq!(context.local_facts[0].key, "durable");
    assert_eq!(
        reopened
            .contexts()
            .latest_snapshot(runtime.id())
            .await
            .unwrap()
            .unwrap(),
        snapshot
    );
    assert_eq!(
        reopened.get_runtime(runtime.id()).await.unwrap().mission_id,
        mission.id
    );
}

#[tokio::test]
async fn context_updates_publish_machine_readable_events() {
    let (kernel, _dir, _db) = kernel().await;
    let mut events = kernel.events().subscribe();
    let mission = kernel.create_mission("context event").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "objective")
        .await
        .unwrap();
    kernel
        .add_context_fact(runtime.id(), "k", ContextValue::Boolean(true))
        .await
        .unwrap();
    let mut found = false;
    while let Ok(event) = events.try_recv() {
        if let RuntimeEvent::ContextUpdated {
            mission_id,
            runtime_id,
            keys,
            ..
        } = event
        {
            assert_eq!(mission_id, mission.id);
            assert_eq!(runtime_id, runtime.id());
            assert_eq!(keys, vec!["k"]);
            found = true;
        }
    }
    assert!(found);
}
