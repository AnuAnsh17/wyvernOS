use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::domain::RuntimeState;
use wyvern::events::RuntimeEvent;
use wyvern::kernel::Kernel;

#[tokio::test]
async fn runtime_state_transition_persists_the_same_event_that_is_broadcast() {
    let directory = tempfile::tempdir().unwrap();
    let kernel = Kernel::new_sqlite(directory.path().join("stage7b.sqlite"))
        .await
        .unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("stage 7b worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let mission = kernel
        .create_mission("atomic runtime transition")
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "transition atomically")
        .await
        .unwrap();
    let mut events = kernel.events().subscribe_envelopes();

    kernel
        .transition_runtime(runtime.id(), RuntimeState::Running)
        .await
        .unwrap();

    let broadcast = events.try_recv().unwrap();
    assert!(matches!(
        broadcast.payload,
        RuntimeEvent::RuntimeStateChanged {
            from: RuntimeState::Ready,
            to: RuntimeState::Running,
            ..
        }
    ));
    let persisted = kernel
        .persisted_events()
        .await
        .unwrap()
        .into_iter()
        .find(|event| event.envelope.event_id == broadcast.event_id)
        .expect("state event should be durably persisted");
    assert_eq!(persisted.envelope, broadcast);
    assert_eq!(
        kernel.get_runtime(runtime.id()).await.unwrap().state,
        RuntimeState::Running
    );
}
