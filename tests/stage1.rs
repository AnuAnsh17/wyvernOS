use std::time::Duration;

use tokio::time::timeout;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::domain::{MissionId, RuntimeId, RuntimeState};
use wyvern::events::RuntimeEvent;
use wyvern::kernel::{Kernel, Runtime};
use wyvern::result::RuntimeOutcome;

struct TestEnv {
    kernel: Kernel,
    _tempdir: tempfile::TempDir,
}

async fn test_env() -> TestEnv {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let database = tempdir.path().join("wyvern.sqlite");
    let kernel = Kernel::new_sqlite(&database).await.expect("kernel");
    TestEnv {
        kernel,
        _tempdir: tempdir,
    }
}

async fn demo_with_events(kernel: Kernel) -> (wyvern::RuntimeResult, Vec<RuntimeEvent>) {
    let mut receiver = kernel.events().subscribe();
    let task = tokio::spawn(async move { kernel.demo_recursive().await });
    let mut events = Vec::new();

    loop {
        let event = timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("event timeout")
            .expect("event stream closed");
        let is_root_complete = matches!(
            &event,
            RuntimeEvent::RuntimeCompleted {
                parent_id: None,
                ..
            }
        );
        events.push(event);
        if is_root_complete {
            break;
        }
    }

    let result = task.await.expect("demo task join").expect("demo result");
    (result, events)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_registry_tracks_parent_child_relationships() {
    let env = test_env().await;
    let mission = env.kernel.create_mission("registry test").await.unwrap();
    let commander = env
        .kernel
        .register_agent(AgentDefinition::new("Commander", AgentKind::Commander))
        .await;
    let worker = env
        .kernel
        .register_agent(AgentDefinition::new("WorkerA", AgentKind::WorkerA))
        .await;

    let root = env
        .kernel
        .create_runtime(mission.id, None, &commander, "root objective")
        .await
        .unwrap();
    let child = env
        .kernel
        .spawn_child(root.id(), &worker, "child objective")
        .await
        .unwrap();

    let root_runtime = env.kernel.get_runtime(root.id()).await.unwrap();
    let child_runtime = env.kernel.get_runtime(child.id()).await.unwrap();

    assert_eq!(root_runtime.mission_id, mission.id);
    assert!(root_runtime.children.contains(&child.id()));
    assert_eq!(child_runtime.parent_id, Some(root.id()));
    assert_eq!(child_runtime.mission_id, mission.id);

    let by_mission = env.kernel.list_runtimes().await;
    assert_eq!(by_mission.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_transitions_and_results_are_structured() {
    let env = test_env().await;
    let mission_id = MissionId::new();
    let agent_id = wyvern::AgentId::new();
    let mut runtime = Runtime::new(mission_id, agent_id, None, "objective");

    assert_eq!(runtime.state, RuntimeState::Created);
    runtime.transition_to(RuntimeState::Ready).unwrap();
    runtime.transition_to(RuntimeState::Running).unwrap();
    runtime.transition_to(RuntimeState::Waiting).unwrap();
    runtime.transition_to(RuntimeState::Completed).unwrap();
    assert!(runtime.transition_to(RuntimeState::Running).is_err());

    let result = wyvern::RuntimeResult::completed(
        RuntimeId::new(),
        "summary",
        serde_json::json!({"a": 1}),
        Vec::new(),
    );
    assert_eq!(result.status, RuntimeOutcome::Completed);

    let _ = env;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_demo_executes_tree_and_persists_results() {
    let env = test_env().await;
    let (result, _events) = demo_with_events(env.kernel.clone()).await;

    assert_eq!(result.status, RuntimeOutcome::Completed);
    assert_eq!(result.child_results.len(), 2);
    assert_eq!(result.child_results[0].child_results.len(), 2);
    assert_eq!(result.child_results[1].child_results.len(), 0);

    let persisted_missions = env.kernel.persisted_missions().await.unwrap();
    let persisted_runtimes = env.kernel.persisted_runtimes().await.unwrap();
    assert_eq!(persisted_missions.len(), 1);
    assert_eq!(persisted_runtimes.len(), 5);

    let root_runtime = env
        .kernel
        .persisted_runtime(result.runtime_id)
        .await
        .unwrap()
        .unwrap();
    assert!(root_runtime.result_json.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_children_execute_concurrently() {
    let env = test_env().await;
    let (result, events) = demo_with_events(env.kernel.clone()).await;
    let root_id = result.runtime_id;

    let child_starts: Vec<_> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| matches!(event, RuntimeEvent::RuntimeStarted { parent_id: Some(parent), .. } if *parent == root_id))
        .map(|(index, _)| index)
        .collect();
    let child_completions: Vec<_> = events
        .iter()
        .enumerate()
        .filter(|(_, event)| matches!(event, RuntimeEvent::RuntimeCompleted { parent_id: Some(parent), .. } if *parent == root_id))
        .map(|(index, _)| index)
        .collect();

    assert_eq!(child_starts.len(), 2);
    assert_eq!(child_completions.len(), 2);
    assert!(child_starts
        .iter()
        .all(|start| *start < *child_completions.iter().min().unwrap()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_bus_emits_lifecycle_events() {
    let env = test_env().await;
    let (result, events) = demo_with_events(env.kernel.clone()).await;
    let mut seen_created = false;
    let mut seen_started = false;
    let mut seen_child_spawned = false;
    let mut seen_completed = false;

    for event in events {
        match event {
            RuntimeEvent::RuntimeCreated { .. } => seen_created = true,
            RuntimeEvent::RuntimeStarted { .. } => seen_started = true,
            RuntimeEvent::RuntimeChildSpawned { .. } => seen_child_spawned = true,
            RuntimeEvent::RuntimeCompleted { .. } => seen_completed = true,
            _ => {}
        }
    }

    assert!(seen_created);
    assert!(seen_started);
    assert!(seen_child_spawned);
    assert!(seen_completed);
    assert_eq!(result.status, RuntimeOutcome::Completed);
}
