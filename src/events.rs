use crate::domain::{AgentId, MissionId, RuntimeId, RuntimeState, Timestamp};
use crate::error::Result;
use crate::result::RuntimeOutcome;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RuntimeEvent {
    RuntimeCreated {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        agent_id: AgentId,
        timestamp: Timestamp,
    },
    RuntimeStarted {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        agent_id: AgentId,
        timestamp: Timestamp,
    },
    RuntimeStateChanged {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        from: RuntimeState,
        to: RuntimeState,
        timestamp: Timestamp,
    },
    RuntimeChildSpawned {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        child_runtime_id: RuntimeId,
        child_agent_id: AgentId,
        timestamp: Timestamp,
    },
    RuntimeCompleted {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        status: RuntimeOutcome,
        timestamp: Timestamp,
    },
    RuntimeFailed {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        error: String,
        timestamp: Timestamp,
    },
    RuntimeCancelled {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        timestamp: Timestamp,
    },
}

impl RuntimeEvent {
    pub fn timestamp(&self) -> Timestamp {
        match self {
            Self::RuntimeCreated { timestamp, .. }
            | Self::RuntimeStarted { timestamp, .. }
            | Self::RuntimeStateChanged { timestamp, .. }
            | Self::RuntimeChildSpawned { timestamp, .. }
            | Self::RuntimeCompleted { timestamp, .. }
            | Self::RuntimeFailed { timestamp, .. }
            | Self::RuntimeCancelled { timestamp, .. } => *timestamp,
        }
    }

    pub fn runtime_id(&self) -> RuntimeId {
        match self {
            Self::RuntimeCreated { runtime_id, .. }
            | Self::RuntimeStarted { runtime_id, .. }
            | Self::RuntimeStateChanged { runtime_id, .. }
            | Self::RuntimeChildSpawned { runtime_id, .. }
            | Self::RuntimeCompleted { runtime_id, .. }
            | Self::RuntimeFailed { runtime_id, .. }
            | Self::RuntimeCancelled { runtime_id, .. } => *runtime_id,
        }
    }
}

#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<RuntimeEvent>,
}

impl EventBus {
    pub fn new(buffer: usize) -> Self {
        let (sender, _) = broadcast::channel(buffer);
        Self { sender }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.sender.subscribe()
    }

    pub fn publish(&self, event: RuntimeEvent) -> Result<()> {
        self.log(&event);
        let _ = self.sender.send(event);
        Ok(())
    }

    fn log(&self, event: &RuntimeEvent) {
        match event {
            RuntimeEvent::RuntimeCreated {
                mission_id,
                runtime_id,
                parent_id,
                agent_id,
                ..
            } => {
                tracing::info!(
                    mission_id = %mission_id,
                    runtime_id = %runtime_id,
                    parent_id = ?parent_id,
                    agent_id = %agent_id,
                    "runtime.created"
                );
            }
            RuntimeEvent::RuntimeStarted {
                mission_id,
                runtime_id,
                parent_id,
                agent_id,
                ..
            } => {
                tracing::info!(
                    mission_id = %mission_id,
                    runtime_id = %runtime_id,
                    parent_id = ?parent_id,
                    agent_id = %agent_id,
                    "runtime.started"
                );
            }
            RuntimeEvent::RuntimeStateChanged {
                mission_id,
                runtime_id,
                parent_id,
                from,
                to,
                ..
            } => {
                tracing::info!(
                    mission_id = %mission_id,
                    runtime_id = %runtime_id,
                    parent_id = ?parent_id,
                    from = %from,
                    to = %to,
                    "runtime.state_changed"
                );
            }
            RuntimeEvent::RuntimeChildSpawned {
                mission_id,
                runtime_id,
                parent_id,
                child_runtime_id,
                child_agent_id,
                ..
            } => {
                tracing::info!(
                    mission_id = %mission_id,
                    runtime_id = %runtime_id,
                    parent_id = ?parent_id,
                    child_runtime_id = %child_runtime_id,
                    child_agent_id = %child_agent_id,
                    "runtime.child_spawned"
                );
            }
            RuntimeEvent::RuntimeCompleted {
                mission_id,
                runtime_id,
                parent_id,
                status,
                ..
            } => {
                tracing::info!(
                    mission_id = %mission_id,
                    runtime_id = %runtime_id,
                    parent_id = ?parent_id,
                    status = ?status,
                    "runtime.completed"
                );
            }
            RuntimeEvent::RuntimeFailed {
                mission_id,
                runtime_id,
                parent_id,
                error,
                ..
            } => {
                tracing::error!(
                    mission_id = %mission_id,
                    runtime_id = %runtime_id,
                    parent_id = ?parent_id,
                    error = %error,
                    "runtime.failed"
                );
            }
            RuntimeEvent::RuntimeCancelled {
                mission_id,
                runtime_id,
                parent_id,
                ..
            } => {
                tracing::warn!(
                    mission_id = %mission_id,
                    runtime_id = %runtime_id,
                    parent_id = ?parent_id,
                    "runtime.cancelled"
                );
            }
        }
    }
}
