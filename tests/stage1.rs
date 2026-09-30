use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use tempfile::TempDir;
use tokio::sync::Barrier;
use tokio::time::timeout;
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::behavior::demo::{PauseGate, RuntimeHooks};
use wyvern::domain::{MissionId, MissionState, RuntimeId, RuntimeState};
use wyvern::events::RuntimeEvent;
use wyvern::kernel::Kernel;
use wyvern::result::{RuntimeOutcome, RuntimeResult};

#[derive(Clone)]
struct DemoAgents {
    commander: AgentDefinition,
    worker_a: AgentDefinition,
    worker_b: AgentDefinition,
    worker_a1: AgentDefinition,
    worker_a2: AgentDefinition,
}

async fn make_kernel() -> (Kernel, TempDir, PathBuf) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let database = tempdir.path().join("wyvern.sqlite");
    let kernel = Kernel::new_sqlite(&database).await.expect("kernel");
    (kernel, tempdir, database)
}

async fn register_demo_agents(kernel: &Kernel) -> DemoAgents {
    DemoAgents {
        commander: kernel
            .register_agent(AgentDefinition::new("L1 Commander", AgentKind::Commander))
            .await
            .unwrap(),
        worker_a: kernel
            .register_agent(AgentDefinition::new("L2 Worker A", AgentKind::WorkerA))
            .await
            .unwrap(),
        worker_b: kernel
            .register_agent(AgentDefinition::new("L2 Worker B", AgentKind::WorkerB))
            .await
            .unwrap(),
        worker_a1: kernel
            .register_agent(AgentDefinition::new("L3 Worker A1", AgentKind::WorkerA1))
            .await
            .unwrap(),
        worker_a2: kernel
            .register_agent(AgentDefinition::new("L3 Worker A2", AgentKind::WorkerA2))
            .await
            .unwrap(),
    }
}

async fn prepare_recursive_root(
    kernel: &Kernel,
    agents: &DemoAgents,
    objective: &str,
) -> (MissionId, RuntimeId) {
    let mission = kernel.create_mission(objective).await.expect("mission");
    let root = kernel
        .create_runtime(
            mission.id,
            None,
            &agents.commander,
            "Coordinate recursive runtime demo",
        )
        .await
        .expect("root runtime");
    (mission.id, root.id())
}

async fn collect_until_root_complete(
    mut receiver: tokio::sync::broadcast::Receiver<RuntimeEvent>,
) -> Vec<RuntimeEvent> {
    let mut events = Vec::new();
    loop {
        let event = timeout(Duration::from_secs(10), receiver.recv())
            .await
            .expect("event timeout")
            .expect("event stream closed");
        let done = matches!(
            event,
            RuntimeEvent::RuntimeCompleted {
                parent_id: None,
                ..
            }
        );
        events.push(event);
        if done {
            break;
        }
    }
    events
}

async fn run_root_with_hooks(
    kernel: Kernel,
    root_id: RuntimeId,
    hooks: RuntimeHooks,
) -> RuntimeResult {
    kernel
        .executor()
        .run_with_hooks(root_id, hooks)
        .await
        .expect("runtime execution")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_creation_and_mission_creation_work() {
    let (kernel, _tempdir, _database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let mission = kernel
        .create_mission("mission creation")
        .await
        .expect("mission");
    assert_eq!(mission.state, MissionState::Created);

    let runtime = kernel
        .create_runtime(mission.id, None, &agents.commander, "root objective")
        .await
        .expect("runtime");

    let loaded_mission = kernel
        .get_mission(mission.id)
        .await
        .expect("mission lookup");
    let loaded_runtime = kernel
        .get_runtime(runtime.id())
        .await
        .expect("runtime lookup");
    assert_eq!(loaded_mission.id, mission.id);
    assert_eq!(loaded_runtime.parent_id, None);
    assert_eq!(loaded_runtime.mission_id, mission.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ids_are_unique_and_restart_safe() {
    let (kernel, tempdir, database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let mission = kernel.create_mission("unique ids").await.expect("mission");
    let runtime = kernel
        .create_runtime(mission.id, None, &agents.commander, "root")
        .await
        .expect("runtime");

    let mission_id = mission.id;
    let runtime_id = runtime.id();
    drop(kernel);
    drop(tempdir);

    let reopened = Kernel::new_sqlite(&database).await.expect("reopen kernel");
    let agents = register_demo_agents(&reopened).await;
    let new_mission = reopened
        .create_mission("post restart")
        .await
        .expect("mission");
    let new_runtime = reopened
        .create_runtime(new_mission.id, None, &agents.commander, "root")
        .await
        .expect("runtime");

    assert_ne!(mission_id, new_mission.id);
    assert_ne!(runtime_id, new_runtime.id());
    assert!(new_runtime.id().to_string().starts_with("rt_"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn runtime_lifecycle_valid_and_invalid_transitions() {
    let mission_id = MissionId::new();
    let agent_id = wyvern::AgentId::new();
    let mut runtime = wyvern::Runtime::new(mission_id, agent_id, None, "objective");

    runtime
        .transition_to(RuntimeState::Ready)
        .expect("created -> ready");
    runtime
        .transition_to(RuntimeState::Running)
        .expect("ready -> running");
    runtime
        .transition_to(RuntimeState::Waiting)
        .expect("running -> waiting");
    runtime
        .transition_to(RuntimeState::Running)
        .expect("waiting -> running");
    runtime
        .transition_to(RuntimeState::Completed)
        .expect("running -> completed");

    let invalid_pairs = [
        (RuntimeState::Completed, RuntimeState::Running),
        (RuntimeState::Completed, RuntimeState::Failed),
        (RuntimeState::Failed, RuntimeState::Running),
        (RuntimeState::Cancelled, RuntimeState::Running),
        (RuntimeState::TimedOut, RuntimeState::Running),
        (RuntimeState::Interrupted, RuntimeState::Running),
        (RuntimeState::Created, RuntimeState::Completed),
    ];

    for (from, to) in invalid_pairs {
        let mut runtime = wyvern::Runtime::new(mission_id, agent_id, None, "objective");
        match from {
            RuntimeState::Created => {}
            RuntimeState::Ready => runtime.transition_to(RuntimeState::Ready).unwrap(),
            RuntimeState::Running => {
                runtime.transition_to(RuntimeState::Ready).unwrap();
                runtime.transition_to(RuntimeState::Running).unwrap();
            }
            RuntimeState::Waiting => {
                runtime.transition_to(RuntimeState::Ready).unwrap();
                runtime.transition_to(RuntimeState::Running).unwrap();
                runtime.transition_to(RuntimeState::Waiting).unwrap();
            }
            RuntimeState::Completed => {
                runtime.transition_to(RuntimeState::Ready).unwrap();
                runtime.transition_to(RuntimeState::Running).unwrap();
                runtime.transition_to(RuntimeState::Completed).unwrap();
            }
            RuntimeState::Failed => {
                runtime.transition_to(RuntimeState::Ready).unwrap();
                runtime.transition_to(RuntimeState::Running).unwrap();
                runtime.transition_to(RuntimeState::Failed).unwrap();
            }
            RuntimeState::Cancelled => {
                runtime.transition_to(RuntimeState::Ready).unwrap();
                runtime.transition_to(RuntimeState::Running).unwrap();
                runtime.transition_to(RuntimeState::Cancelled).unwrap();
            }
            RuntimeState::TimedOut => {
                runtime.transition_to(RuntimeState::Ready).unwrap();
                runtime.transition_to(RuntimeState::Running).unwrap();
                runtime.transition_to(RuntimeState::TimedOut).unwrap();
            }
            RuntimeState::Interrupted => {
                runtime.transition_to(RuntimeState::Ready).unwrap();
                runtime.transition_to(RuntimeState::Running).unwrap();
                runtime.transition_to(RuntimeState::Interrupted).unwrap();
            }
        }
        assert!(
            runtime.transition_to(to).is_err(),
            "expected {from} -> {to} to fail"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_child_integrity_and_cross_mission_rejection_work() {
    let (kernel, _tempdir, _database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let mission_a = kernel.create_mission("mission a").await.expect("mission a");
    let mission_b = kernel.create_mission("mission b").await.expect("mission b");

    let root_a = kernel
        .create_runtime(mission_a.id, None, &agents.commander, "root a")
        .await
        .expect("root a");
    let child_a = kernel
        .spawn_child(root_a.id(), &agents.worker_a, "child a")
        .await
        .expect("child a");

    assert!(kernel
        .get_runtime(root_a.id())
        .await
        .unwrap()
        .parent_id
        .is_none());
    assert_eq!(
        kernel.get_runtime(child_a.id()).await.unwrap().parent_id,
        Some(root_a.id())
    );

    let cross_mission = kernel
        .create_runtime(
            mission_b.id,
            Some(root_a.id()),
            &agents.worker_b,
            "bad child",
        )
        .await;
    assert!(matches!(
        cross_mission,
        Err(wyvern::WyvernError::CrossMissionHierarchyError { .. })
    ));

    let missing_parent = kernel
        .create_runtime(
            mission_b.id,
            Some(RuntimeId::new()),
            &agents.worker_b,
            "bad parent",
        )
        .await;
    assert!(matches!(
        missing_parent,
        Err(wyvern::WyvernError::ParentNotFound(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recursive_tree_executes_with_correct_structure_and_persistence() {
    let (kernel, _tempdir, _database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let (_mission_id, root_id) = prepare_recursive_root(&kernel, &agents, "recursive tree").await;

    let result = run_root_with_hooks(
        kernel.clone(),
        root_id,
        RuntimeHooks::recursive_demo(std::sync::Arc::new(Barrier::new(2))),
    )
    .await;

    assert_eq!(result.status, RuntimeOutcome::Completed);
    assert_eq!(result.child_results.len(), 2);
    assert_eq!(result.child_results[0].child_results.len(), 2);
    assert_eq!(result.child_results[1].child_results.len(), 0);

    let runtime_root = kernel.get_runtime(root_id).await.expect("root runtime");
    let child_a = kernel
        .get_runtime(runtime_root.children[0])
        .await
        .expect("child a");
    let child_b = kernel
        .get_runtime(runtime_root.children[1])
        .await
        .expect("child b");
    assert!(runtime_root.parent_id.is_none());
    assert_eq!(child_a.parent_id, Some(root_id));
    assert_eq!(child_b.parent_id, Some(root_id));
    assert_eq!(child_a.mission_id, runtime_root.mission_id);
    assert_eq!(child_b.mission_id, runtime_root.mission_id);

    let persisted_root = kernel
        .persisted_runtime(root_id)
        .await
        .expect("persisted root")
        .expect("row");
    assert!(persisted_root.result_json.is_some());
    let persisted_missions = kernel.persisted_missions().await.expect("missions");
    let persisted_runtimes = kernel.persisted_runtimes().await.expect("runtimes");
    assert_eq!(persisted_missions.len(), 1);
    assert_eq!(persisted_runtimes.len(), 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_children_and_missions_execute_without_crossing_boundaries() {
    let (kernel, _tempdir, _database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let shared_barrier = std::sync::Arc::new(Barrier::new(3));

    let mut roots = Vec::new();
    for mission_name in ["mission a", "mission b", "mission c"] {
        roots.push(prepare_recursive_root(&kernel, &agents, mission_name).await);
    }

    let tasks: Vec<_> = roots
        .iter()
        .map(|(_, root_id)| {
            let kernel = kernel.clone();
            let hooks = RuntimeHooks::default().with_root_barrier(shared_barrier.clone());
            let root_id = *root_id;
            tokio::spawn(async move {
                kernel
                    .executor()
                    .run_with_hooks(root_id, hooks)
                    .await
                    .expect("root")
            })
        })
        .collect();

    let mut results = Vec::new();
    for task in tasks {
        results.push(task.await.expect("join"));
    }

    assert!(results
        .iter()
        .all(|result| result.status == RuntimeOutcome::Completed));

    let runtimes = kernel.list_runtimes().await;
    let mut mission_counts: HashMap<_, usize> = HashMap::new();
    for runtime in runtimes {
        *mission_counts.entry(runtime.mission_id).or_insert(0) += 1;
    }
    assert_eq!(mission_counts.len(), 3);
    assert!(mission_counts.values().all(|count| *count >= 1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_child_results_remain_distinct_and_do_not_corrupt_siblings() {
    let (kernel, _tempdir, _database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let (_mission_id, root_id) =
        prepare_recursive_root(&kernel, &agents, "failure semantics").await;

    let result = run_root_with_hooks(
        kernel.clone(),
        root_id,
        RuntimeHooks::recursive_demo(std::sync::Arc::new(Barrier::new(2)))
            .with_failure_kind(agents.worker_a2.kind),
    )
    .await;

    assert_eq!(result.status, RuntimeOutcome::Completed);
    let child_a = &result.child_results[0];
    let child_b = &result.child_results[1];
    assert_eq!(child_a.status, RuntimeOutcome::Completed);
    assert_eq!(child_b.status, RuntimeOutcome::Completed);
    assert_eq!(child_a.child_results.len(), 2);
    assert_eq!(child_a.child_results[1].status, RuntimeOutcome::Failed);

    let root_runtime = kernel.get_runtime(root_id).await.expect("root runtime");
    let child_a_runtime = kernel
        .get_runtime(root_runtime.children[0])
        .await
        .expect("child a");
    let child_b_runtime = kernel
        .get_runtime(root_runtime.children[1])
        .await
        .expect("child b");
    assert_eq!(child_a_runtime.state, RuntimeState::Completed);
    assert_eq!(child_b_runtime.state, RuntimeState::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_runtime_can_be_cancelled() {
    let (kernel, _tempdir, _database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let mission = kernel
        .create_mission("cancellation")
        .await
        .expect("mission");
    let runtime = kernel
        .create_runtime(mission.id, None, &agents.worker_a1, "leaf objective")
        .await
        .expect("runtime");
    let runtime_id = runtime.id();
    let gate = PauseGate::new();
    let hooks = RuntimeHooks::default().with_pause_gate(gate.clone());
    let kernel_for_task = kernel.clone();
    let task = tokio::spawn(async move {
        kernel_for_task
            .executor()
            .run_with_hooks(runtime_id, hooks)
            .await
            .expect("leaf run")
    });

    timeout(Duration::from_secs(5), gate.started.notified())
        .await
        .expect("started timeout");
    let cancelled = kernel
        .cancel_runtime(runtime_id)
        .await
        .expect("cancel runtime");
    assert_eq!(cancelled.state, RuntimeState::Cancelled);
    gate.release.notify_waiters();
    let result = task.await.expect("task");
    assert_eq!(result.status, RuntimeOutcome::Cancelled);

    let persisted = kernel
        .persisted_runtime(runtime_id)
        .await
        .expect("persisted")
        .expect("row");
    assert_eq!(persisted.state, "Cancelled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_event_subscribers_receive_the_same_stream() {
    let (kernel, _tempdir, _database) = make_kernel().await;
    let rx_one = kernel.events().subscribe();
    let rx_two = kernel.events().subscribe();

    let kernel_for_task = kernel.clone();
    let task = tokio::spawn(async move { kernel_for_task.demo_recursive().await.expect("demo") });

    let (events_one, events_two) = tokio::join!(
        collect_until_root_complete(rx_one),
        collect_until_root_complete(rx_two),
    );

    let result = task.await.expect("join");
    assert_eq!(result.status, RuntimeOutcome::Completed);
    assert_eq!(events_one.len(), events_two.len());
    assert!(events_one
        .iter()
        .any(|event| matches!(event, RuntimeEvent::RuntimeChildSpawned { .. })));
    assert!(events_two
        .iter()
        .any(|event| matches!(event, RuntimeEvent::RuntimeChildSpawned { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registry_supports_concurrent_queries_while_runtime_is_running() {
    let (kernel, _tempdir, _database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let (_mission_id, root_id) =
        prepare_recursive_root(&kernel, &agents, "registry concurrency").await;
    let gate = PauseGate::new();
    let hooks = RuntimeHooks::default().with_pause_gate(gate.clone());
    let kernel_for_task = kernel.clone();
    let task = tokio::spawn(async move {
        kernel_for_task
            .executor()
            .run_with_hooks(root_id, hooks)
            .await
            .expect("run")
    });

    timeout(Duration::from_secs(5), gate.started.notified())
        .await
        .expect("started timeout");

    let mut queries = Vec::new();
    for _ in 0..8 {
        let kernel = kernel.clone();
        queries.push(tokio::spawn(async move {
            for _ in 0..32 {
                let runtime = kernel.get_runtime(root_id).await.expect("runtime");
                assert!(matches!(
                    runtime.state,
                    RuntimeState::Running | RuntimeState::Waiting
                ));
                let _ = kernel.list_runtimes().await;
                let _ = kernel
                    .persisted_runtimes()
                    .await
                    .expect("persisted runtimes");
            }
        }));
    }

    for query in queries {
        query.await.expect("query");
    }

    gate.release.notify_waiters();
    let result = task.await.expect("task");
    assert_eq!(result.status, RuntimeOutcome::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_recovery_loads_persisted_state() {
    let (kernel, tempdir, database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let (_mission_id, root_id) = prepare_recursive_root(&kernel, &agents, "restart recovery").await;

    let result = run_root_with_hooks(
        kernel.clone(),
        root_id,
        RuntimeHooks::recursive_demo(std::sync::Arc::new(Barrier::new(2))),
    )
    .await;
    assert_eq!(result.status, RuntimeOutcome::Completed);

    drop(kernel);

    let reopened = Kernel::new_sqlite(&database).await.expect("reopen kernel");
    let missions = reopened.list_missions().await;
    let runtimes = reopened.list_runtimes().await;
    assert_eq!(missions.len(), 1);
    assert_eq!(runtimes.len(), 5);

    let root_runtime = reopened.get_runtime(root_id).await.expect("loaded root");
    assert_eq!(root_runtime.state, RuntimeState::Completed);
    assert!(root_runtime.result.is_some());

    let agents = DemoAgents {
        commander: reopened.agent_by_kind(AgentKind::Commander).await.unwrap(),
        worker_a: reopened.agent_by_kind(AgentKind::WorkerA).await.unwrap(),
        worker_b: reopened.agent_by_kind(AgentKind::WorkerB).await.unwrap(),
        worker_a1: reopened.agent_by_kind(AgentKind::WorkerA1).await.unwrap(),
        worker_a2: reopened.agent_by_kind(AgentKind::WorkerA2).await.unwrap(),
    };
    let new_mission = reopened
        .create_mission("post restart")
        .await
        .expect("mission");
    let new_root = reopened
        .create_runtime(
            new_mission.id,
            None,
            &agents.commander,
            "root after restart",
        )
        .await
        .expect("runtime");
    assert_ne!(new_root.id(), root_id);
    drop(tempdir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_inspection_and_recursive_demo_work() {
    let (kernel, tempdir, database) = make_kernel().await;
    let agents = register_demo_agents(&kernel).await;
    let mission = kernel.create_mission("cli check").await.expect("mission");
    let runtime = kernel
        .create_runtime(mission.id, None, &agents.commander, "root")
        .await
        .expect("runtime");
    drop(kernel);
    drop(tempdir);

    let exe = std::env::var("CARGO_BIN_EXE_wyvern").expect("binary path");

    let inspect_output = Command::new(&exe)
        .arg("--database")
        .arg(&database)
        .arg("runtime")
        .arg("inspect")
        .arg(runtime.id().to_string())
        .output()
        .expect("inspect run");
    assert!(inspect_output.status.success());
    let inspect_text = String::from_utf8_lossy(&inspect_output.stdout);
    assert!(inspect_text.contains("root"));

    let recursive_output = Command::new(&exe)
        .arg("--database")
        .arg(&database)
        .arg("demo")
        .arg("recursive")
        .output()
        .expect("demo run");
    assert!(recursive_output.status.success());
    let demo_text = String::from_utf8_lossy(&recursive_output.stdout);
    let json_start = demo_text.find('{').expect("demo json start");
    let result: RuntimeResult = serde_json::from_str(&demo_text[json_start..]).expect("demo json");
    assert_eq!(result.status, RuntimeOutcome::Completed);

    let list_output = Command::new(&exe)
        .arg("--database")
        .arg(&database)
        .arg("runtime")
        .arg("list")
        .output()
        .expect("list run");
    assert!(list_output.status.success());
}
