use std::sync::Arc;

use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::decision::{DecisionContext, DecisionOption, DecisionType, MockDecisionEngine};
use wyvern::domain::{EventId, RuntimeState};
use wyvern::events::RuntimeEvent;
use wyvern::kernel::Kernel;

#[tokio::test]
async fn agents_and_runtime_hierarchy_survive_reopen_without_replacement_ids() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("stage6d-agents.sqlite");
    let first = Kernel::new_sqlite(&database).await.unwrap();

    let commander = first
        .register_agent(AgentDefinition::new(
            "Durable commander",
            AgentKind::Commander,
        ))
        .await
        .unwrap();
    let worker = first
        .register_agent(AgentDefinition::new("Durable worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let mission = first
        .create_mission("agent restart recovery")
        .await
        .unwrap();
    let root = first
        .create_runtime(mission.id, None, &commander, "root")
        .await
        .unwrap();
    let child = first
        .spawn_child(root.id(), &worker, "child")
        .await
        .unwrap();
    let root_id = root.id();
    let child_id = child.id();
    let mission_id = mission.id;
    drop(child);
    drop(root);
    drop(first);

    let reopened = Kernel::new_sqlite(&database).await.unwrap();
    let recovered_commander = reopened.agent(commander.id).await.unwrap();
    let recovered_worker = reopened.agent(worker.id).await.unwrap();
    assert_eq!(recovered_commander.id, commander.id);
    assert_eq!(recovered_commander.name, commander.name);
    assert_eq!(recovered_commander.kind, commander.kind);
    assert_eq!(recovered_commander.created_at, commander.created_at);
    assert_eq!(recovered_worker.id, worker.id);
    assert_eq!(recovered_worker.name, worker.name);
    assert_eq!(recovered_worker.kind, worker.kind);
    assert_eq!(recovered_worker.created_at, worker.created_at);

    let recovered_root = reopened.get_runtime(root_id).await.unwrap();
    let recovered_child = reopened.get_runtime(child_id).await.unwrap();
    assert_eq!(recovered_root.mission_id, mission_id);
    assert_eq!(recovered_root.agent_id, commander.id);
    assert_eq!(recovered_root.children, vec![child_id]);
    assert_eq!(recovered_child.agent_id, worker.id);
    assert_eq!(recovered_child.parent_id, Some(root_id));
    assert_eq!(
        reopened.agent(recovered_root.agent_id).await.unwrap().id,
        commander.id
    );
    assert_eq!(
        reopened.agent(recovered_child.agent_id).await.unwrap().id,
        worker.id
    );

    assert!(matches!(
        reopened.register_agent(commander.clone()).await,
        Err(wyvern::WyvernError::DuplicateAgent(id)) if id == commander.id
    ));
}

#[tokio::test]
async fn event_log_survives_restart_with_order_ids_and_scope_filters() {
    let directory = tempfile::tempdir().unwrap();
    let database = directory.path().join("stage6d-events.sqlite");
    let first = Kernel::new_sqlite_with_decision_engine(&database, Arc::new(MockDecisionEngine))
        .await
        .unwrap();

    let agent_a = first
        .register_agent(AgentDefinition::new("event worker A", AgentKind::WorkerA))
        .await
        .unwrap();
    let agent_b = first
        .register_agent(AgentDefinition::new("event worker B", AgentKind::WorkerB))
        .await
        .unwrap();
    let mission_a = first.create_mission("mission A").await.unwrap();
    let mission_b = first.create_mission("mission B").await.unwrap();
    let runtime_a = first
        .create_runtime(mission_a.id, None, &agent_a, "runtime A")
        .await
        .unwrap();
    let child_a = first
        .spawn_child(runtime_a.id(), &agent_b, "child A")
        .await
        .unwrap();
    let runtime_b = first
        .create_runtime(mission_b.id, None, &agent_b, "runtime B")
        .await
        .unwrap();
    let runtime_b_id = runtime_b.id();

    runtime_a
        .decide(
            DecisionType::ExecutionStrategy,
            vec![DecisionOption::new("continue", "Continue").unwrap()],
            DecisionContext::default(),
            Vec::new(),
        )
        .await
        .unwrap();
    first
        .transition_runtime(child_a.id(), RuntimeState::Running)
        .await
        .unwrap();
    for _ in 0..4 {
        first
            .transition_runtime(child_a.id(), RuntimeState::Waiting)
            .await
            .unwrap();
        first
            .transition_runtime(child_a.id(), RuntimeState::Running)
            .await
            .unwrap();
    }

    let before_restart = first.persisted_events().await.unwrap();
    assert!(before_restart.len() >= 8);
    assert!(before_restart
        .windows(2)
        .all(|pair| pair[0].sequence_id < pair[1].sequence_id));
    let mut event_ids: Vec<_> = before_restart
        .iter()
        .map(|event| event.envelope.event_id)
        .collect();
    event_ids.sort();
    event_ids.dedup();
    assert_eq!(event_ids.len(), before_restart.len());
    assert!(before_restart.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::DecisionRequested { .. }
    )));
    assert!(before_restart.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::DecisionCompleted { .. }
    )));
    assert!(before_restart
        .iter()
        .any(|event| matches!(event.envelope.payload, RuntimeEvent::RuntimeCreated { .. })));
    assert!(first
        .events_for_runtime(runtime_a.id())
        .await
        .unwrap()
        .iter()
        .all(|event| event.envelope.runtime_id == runtime_a.id()));
    assert!(first
        .events_for_mission(mission_a.id)
        .await
        .unwrap()
        .iter()
        .all(|event| event.envelope.mission_id == mission_a.id));
    assert!(first
        .events_for_runtime(runtime_b.id())
        .await
        .unwrap()
        .iter()
        .all(|event| event.envelope.mission_id == mission_b.id));

    let persisted_ids: Vec<_> = before_restart
        .iter()
        .map(|event| event.envelope.event_id)
        .collect();
    let persisted_sequences: Vec<_> = before_restart
        .iter()
        .map(|event| event.sequence_id)
        .collect();
    drop(runtime_b);
    drop(child_a);
    drop(runtime_a);
    drop(first);

    let reopened = Kernel::new_sqlite(&database).await.unwrap();
    let after_restart = reopened.persisted_events().await.unwrap();
    assert_eq!(
        after_restart
            .iter()
            .map(|event| event.envelope.event_id)
            .collect::<Vec<EventId>>(),
        persisted_ids
    );
    assert_eq!(
        after_restart
            .iter()
            .map(|event| event.sequence_id)
            .collect::<Vec<_>>(),
        persisted_sequences
    );
    assert!(after_restart.iter().any(|event| matches!(
        event.envelope.payload,
        RuntimeEvent::DecisionRequested { .. }
    )));
    assert_eq!(
        reopened
            .events_for_mission(mission_b.id)
            .await
            .unwrap()
            .iter()
            .filter(|event| event.envelope.runtime_id == runtime_b_id)
            .count(),
        2
    );
}
