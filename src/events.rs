use crate::domain::{AgentId, MemoryId, MissionId, RuntimeId, RuntimeState, Timestamp};
use crate::error::Result;
use crate::result::RuntimeOutcome;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event_id: String,
    pub event_type: String,
    pub timestamp: Timestamp,
    pub mission_id: MissionId,
    pub runtime_id: RuntimeId,
    pub parent_runtime_id: Option<RuntimeId>,
    pub payload: RuntimeEvent,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
    ContextUpdated {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        keys: Vec<String>,
        timestamp: Timestamp,
    },
    MemoryCreated {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        memory_id: MemoryId,
        timestamp: Timestamp,
    },
    MemorySuperseded {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        memory_id: MemoryId,
        replacement_memory_id: MemoryId,
        timestamp: Timestamp,
    },
    MemoryInvalidated {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        memory_id: MemoryId,
        timestamp: Timestamp,
    },
    MemoryArchived {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        memory_id: MemoryId,
        timestamp: Timestamp,
    },
    MemoryPromoted {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        memory_id: MemoryId,
        source_memory_id: MemoryId,
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
            Self::ContextUpdated { timestamp, .. }
            | Self::MemoryCreated { timestamp, .. }
            | Self::MemorySuperseded { timestamp, .. }
            | Self::MemoryInvalidated { timestamp, .. }
            | Self::MemoryArchived { timestamp, .. }
            | Self::MemoryPromoted { timestamp, .. } => *timestamp,
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
            Self::ContextUpdated { runtime_id, .. }
            | Self::MemoryCreated { runtime_id, .. }
            | Self::MemorySuperseded { runtime_id, .. }
            | Self::MemoryInvalidated { runtime_id, .. }
            | Self::MemoryArchived { runtime_id, .. }
            | Self::MemoryPromoted { runtime_id, .. } => *runtime_id,
        }
    }
}

#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<RuntimeEvent>,
    envelope_sender: broadcast::Sender<EventEnvelope>,
}

impl EventBus {
    pub fn new(buffer: usize) -> Self {
        let (sender, _) = broadcast::channel(buffer);
        let (envelope_sender, _) = broadcast::channel(buffer);
        Self {
            sender,
            envelope_sender,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.sender.subscribe()
    }

    pub fn subscribe_envelopes(&self) -> broadcast::Receiver<EventEnvelope> {
        self.envelope_sender.subscribe()
    }

    pub fn publish(&self, event: RuntimeEvent) -> Result<()> {
        self.log(&event);
        let envelope = EventEnvelope {
            event_id: uuid::Uuid::now_v7().to_string(),
            event_type: event.event_type().to_string(),
            timestamp: event.timestamp(),
            mission_id: event.mission_id(),
            runtime_id: event.runtime_id(),
            parent_runtime_id: event.parent_id(),
            payload: event.clone(),
        };
        let _ = self.envelope_sender.send(envelope);
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
            RuntimeEvent::ContextUpdated {
                mission_id,
                runtime_id,
                parent_id,
                keys,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, parent_id = ?parent_id, keys = ?keys, "context.updated");
            }
            RuntimeEvent::MemoryCreated {
                mission_id,
                runtime_id,
                memory_id,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, memory_id = %memory_id, "memory.created")
            }
            RuntimeEvent::MemorySuperseded {
                mission_id,
                runtime_id,
                memory_id,
                replacement_memory_id,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, memory_id = %memory_id, replacement_memory_id = %replacement_memory_id, "memory.superseded")
            }
            RuntimeEvent::MemoryInvalidated {
                mission_id,
                runtime_id,
                memory_id,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, memory_id = %memory_id, "memory.invalidated")
            }
            RuntimeEvent::MemoryArchived {
                mission_id,
                runtime_id,
                memory_id,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, memory_id = %memory_id, "memory.archived")
            }
            RuntimeEvent::MemoryPromoted {
                mission_id,
                runtime_id,
                memory_id,
                source_memory_id,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, memory_id = %memory_id, source_memory_id = %source_memory_id, "memory.promoted")
            }
        }
    }
}

impl RuntimeEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::RuntimeCreated { .. } => "runtime.created",
            Self::RuntimeStarted { .. } => "runtime.started",
            Self::RuntimeStateChanged { .. } => "runtime.state_changed",
            Self::RuntimeChildSpawned { .. } => "runtime.child_spawned",
            Self::RuntimeCompleted { .. } => "runtime.completed",
            Self::RuntimeFailed { .. } => "runtime.failed",
            Self::RuntimeCancelled { .. } => "runtime.cancelled",
            Self::ContextUpdated { .. } => "context.updated",
            Self::MemoryCreated { .. } => "memory.created",
            Self::MemorySuperseded { .. } => "memory.superseded",
            Self::MemoryInvalidated { .. } => "memory.invalidated",
            Self::MemoryArchived { .. } => "memory.archived",
            Self::MemoryPromoted { .. } => "memory.promoted",
        }
    }

    pub fn mission_id(&self) -> MissionId {
        match self {
            Self::RuntimeCreated { mission_id, .. }
            | Self::RuntimeStarted { mission_id, .. }
            | Self::RuntimeStateChanged { mission_id, .. }
            | Self::RuntimeChildSpawned { mission_id, .. }
            | Self::RuntimeCompleted { mission_id, .. }
            | Self::RuntimeFailed { mission_id, .. }
            | Self::RuntimeCancelled { mission_id, .. } => *mission_id,
            Self::ContextUpdated { mission_id, .. }
            | Self::MemoryCreated { mission_id, .. }
            | Self::MemorySuperseded { mission_id, .. }
            | Self::MemoryInvalidated { mission_id, .. }
            | Self::MemoryArchived { mission_id, .. }
            | Self::MemoryPromoted { mission_id, .. } => *mission_id,
        }
    }

    pub fn parent_id(&self) -> Option<RuntimeId> {
        match self {
            Self::RuntimeCreated { parent_id, .. }
            | Self::RuntimeStarted { parent_id, .. }
            | Self::RuntimeStateChanged { parent_id, .. }
            | Self::RuntimeChildSpawned { parent_id, .. }
            | Self::RuntimeCompleted { parent_id, .. }
            | Self::RuntimeFailed { parent_id, .. }
            | Self::RuntimeCancelled { parent_id, .. } => *parent_id,
            Self::ContextUpdated { parent_id, .. } => *parent_id,
            Self::MemoryCreated { .. }
            | Self::MemorySuperseded { .. }
            | Self::MemoryInvalidated { .. }
            | Self::MemoryArchived { .. }
            | Self::MemoryPromoted { .. } => None,
        }
    }
}
