use crate::decision::{DecisionEngineProvenance, DecisionError, DecisionOptionId, DecisionType};
use crate::domain::{
    AgentId, CapabilityId, CapabilityRequestId, DecisionRequestId, MemoryId, MissionId, RuntimeId,
    RuntimeState, Timestamp, ToolId,
};
use crate::error::Result;
use crate::result::RuntimeOutcome;
use crate::scheduler::SchedulerState;
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
    DecisionRequested {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: DecisionRequestId,
        decision_type: DecisionType,
        timestamp: Timestamp,
    },
    DecisionCompleted {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: DecisionRequestId,
        selected_option: DecisionOptionId,
        engine: DecisionEngineProvenance,
        timestamp: Timestamp,
    },
    DecisionFailed {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: DecisionRequestId,
        error: DecisionError,
        timestamp: Timestamp,
    },
    DecisionAccepted {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: DecisionRequestId,
        selected_option: DecisionOptionId,
        from: RuntimeState,
        to: RuntimeState,
        changed_state: bool,
        timestamp: Timestamp,
    },
    DecisionRejected {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: DecisionRequestId,
        selected_option: DecisionOptionId,
        error: DecisionError,
        timestamp: Timestamp,
    },
    DecisionStale {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: DecisionRequestId,
        error: DecisionError,
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
    CapabilityRequested {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: CapabilityRequestId,
        capability_id: CapabilityId,
        tool_id: ToolId,
        timestamp: Timestamp,
    },
    CapabilityStarted {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: CapabilityRequestId,
        capability_id: CapabilityId,
        tool_id: ToolId,
        timestamp: Timestamp,
    },
    CapabilitySucceeded {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: CapabilityRequestId,
        capability_id: CapabilityId,
        tool_id: ToolId,
        timestamp: Timestamp,
    },
    CapabilityFailed {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: CapabilityRequestId,
        capability_id: CapabilityId,
        tool_id: ToolId,
        error: String,
        timestamp: Timestamp,
    },
    CapabilityCancelled {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: CapabilityRequestId,
        capability_id: CapabilityId,
        tool_id: ToolId,
        timed_out: bool,
        timestamp: Timestamp,
    },
    CapabilityRejected {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        request_id: CapabilityRequestId,
        capability_id: CapabilityId,
        tool_id: ToolId,
        error: String,
        timestamp: Timestamp,
    },
    SchedulerStateChanged {
        mission_id: MissionId,
        runtime_id: RuntimeId,
        parent_id: Option<RuntimeId>,
        state: SchedulerState,
        attempt: u32,
        detail: Option<String>,
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
            | Self::DecisionRequested { timestamp, .. }
            | Self::DecisionCompleted { timestamp, .. }
            | Self::DecisionFailed { timestamp, .. }
            | Self::DecisionAccepted { timestamp, .. }
            | Self::DecisionRejected { timestamp, .. }
            | Self::DecisionStale { timestamp, .. }
            | Self::MemoryCreated { timestamp, .. }
            | Self::MemorySuperseded { timestamp, .. }
            | Self::MemoryInvalidated { timestamp, .. }
            | Self::MemoryArchived { timestamp, .. }
            | Self::MemoryPromoted { timestamp, .. } => *timestamp,
            Self::CapabilityRequested { timestamp, .. }
            | Self::CapabilityStarted { timestamp, .. }
            | Self::CapabilitySucceeded { timestamp, .. }
            | Self::CapabilityFailed { timestamp, .. }
            | Self::CapabilityCancelled { timestamp, .. }
            | Self::CapabilityRejected { timestamp, .. } => *timestamp,
            Self::SchedulerStateChanged { timestamp, .. } => *timestamp,
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
            | Self::DecisionRequested { runtime_id, .. }
            | Self::DecisionCompleted { runtime_id, .. }
            | Self::DecisionFailed { runtime_id, .. }
            | Self::DecisionAccepted { runtime_id, .. }
            | Self::DecisionRejected { runtime_id, .. }
            | Self::DecisionStale { runtime_id, .. }
            | Self::MemoryCreated { runtime_id, .. }
            | Self::MemorySuperseded { runtime_id, .. }
            | Self::MemoryInvalidated { runtime_id, .. }
            | Self::MemoryArchived { runtime_id, .. }
            | Self::MemoryPromoted { runtime_id, .. } => *runtime_id,
            Self::CapabilityRequested { runtime_id, .. }
            | Self::CapabilityStarted { runtime_id, .. }
            | Self::CapabilitySucceeded { runtime_id, .. }
            | Self::CapabilityFailed { runtime_id, .. }
            | Self::CapabilityCancelled { runtime_id, .. }
            | Self::CapabilityRejected { runtime_id, .. } => *runtime_id,
            Self::SchedulerStateChanged { runtime_id, .. } => *runtime_id,
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
            RuntimeEvent::CapabilityRequested {
                mission_id,
                runtime_id,
                request_id,
                capability_id,
                tool_id,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, capability_id = %capability_id, tool_id = %tool_id, "capability.requested")
            }
            RuntimeEvent::CapabilityStarted {
                mission_id,
                runtime_id,
                request_id,
                capability_id,
                tool_id,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, capability_id = %capability_id, tool_id = %tool_id, "capability.started")
            }
            RuntimeEvent::CapabilitySucceeded {
                mission_id,
                runtime_id,
                request_id,
                capability_id,
                tool_id,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, capability_id = %capability_id, tool_id = %tool_id, "capability.succeeded")
            }
            RuntimeEvent::CapabilityFailed {
                mission_id,
                runtime_id,
                request_id,
                capability_id,
                tool_id,
                error,
                ..
            } => {
                tracing::warn!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, capability_id = %capability_id, tool_id = %tool_id, error = %error, "capability.failed")
            }
            RuntimeEvent::CapabilityCancelled {
                mission_id,
                runtime_id,
                request_id,
                capability_id,
                tool_id,
                timed_out,
                ..
            } => {
                tracing::warn!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, capability_id = %capability_id, tool_id = %tool_id, timed_out = *timed_out, "capability.cancelled")
            }
            RuntimeEvent::CapabilityRejected {
                mission_id,
                runtime_id,
                request_id,
                capability_id,
                tool_id,
                error,
                ..
            } => {
                tracing::warn!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, capability_id = %capability_id, tool_id = %tool_id, error = %error, "capability.rejected")
            }
            RuntimeEvent::SchedulerStateChanged {
                mission_id,
                runtime_id,
                state,
                attempt,
                detail,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, state = ?state, attempt = *attempt, detail = ?detail, "scheduler.runtime_state")
            }
            RuntimeEvent::DecisionRequested {
                mission_id,
                runtime_id,
                request_id,
                decision_type,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, decision_type = ?decision_type, "decision.requested")
            }
            RuntimeEvent::DecisionCompleted {
                mission_id,
                runtime_id,
                request_id,
                selected_option,
                engine,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, selected_option = %selected_option.as_str(), engine_id = %engine.engine_id, "decision.completed")
            }
            RuntimeEvent::DecisionFailed {
                mission_id,
                runtime_id,
                request_id,
                error,
                ..
            } => {
                tracing::warn!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, error = %error, "decision.failed")
            }
            RuntimeEvent::DecisionAccepted {
                mission_id,
                runtime_id,
                request_id,
                selected_option,
                from,
                to,
                changed_state,
                ..
            } => {
                tracing::info!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, selected_option = %selected_option.as_str(), from = %from, to = %to, changed_state = *changed_state, "decision.accepted")
            }
            RuntimeEvent::DecisionRejected {
                mission_id,
                runtime_id,
                request_id,
                error,
                ..
            } => {
                tracing::warn!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, error = %error, "decision.rejected")
            }
            RuntimeEvent::DecisionStale {
                mission_id,
                runtime_id,
                request_id,
                error,
                ..
            } => {
                tracing::warn!(mission_id = %mission_id, runtime_id = %runtime_id, request_id = %request_id, error = %error, "decision.stale")
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
            Self::DecisionRequested { .. } => "decision.requested",
            Self::DecisionCompleted { .. } => "decision.completed",
            Self::DecisionFailed { .. } => "decision.failed",
            Self::DecisionAccepted { .. } => "decision.accepted",
            Self::DecisionRejected { .. } => "decision.rejected",
            Self::DecisionStale { .. } => "decision.stale",
            Self::MemoryCreated { .. } => "memory.created",
            Self::MemorySuperseded { .. } => "memory.superseded",
            Self::MemoryInvalidated { .. } => "memory.invalidated",
            Self::MemoryArchived { .. } => "memory.archived",
            Self::MemoryPromoted { .. } => "memory.promoted",
            Self::CapabilityRequested { .. } => "capability.requested",
            Self::CapabilityStarted { .. } => "capability.started",
            Self::CapabilitySucceeded { .. } => "capability.succeeded",
            Self::CapabilityFailed { .. } => "capability.failed",
            Self::CapabilityCancelled { .. } => "capability.cancelled",
            Self::CapabilityRejected { .. } => "capability.rejected",
            Self::SchedulerStateChanged { state, detail, .. }
                if *state == SchedulerState::Queued
                    && detail
                        .as_deref()
                        .is_some_and(|value| value.starts_with("retry after")) =>
            {
                "runtime.retry_scheduled"
            }
            Self::SchedulerStateChanged { state, detail, .. }
                if *state == SchedulerState::Runnable && detail.as_deref() == Some("resumed") =>
            {
                "runtime.resumed"
            }
            Self::SchedulerStateChanged { state, .. } => match state {
                SchedulerState::Queued => "runtime.queued",
                SchedulerState::Runnable => "runtime.runnable",
                SchedulerState::Running => "runtime.started",
                SchedulerState::Waiting => "runtime.waiting",
                SchedulerState::Completed => "runtime.completed",
                SchedulerState::Failed => "runtime.failed",
                SchedulerState::Cancelled => "runtime.cancelled",
                SchedulerState::TimedOut => "runtime.timed_out",
                SchedulerState::Blocked => "runtime.blocked",
                SchedulerState::Interrupted => "runtime.interrupted",
            },
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
            | Self::DecisionRequested { mission_id, .. }
            | Self::DecisionCompleted { mission_id, .. }
            | Self::DecisionFailed { mission_id, .. }
            | Self::DecisionAccepted { mission_id, .. }
            | Self::DecisionRejected { mission_id, .. }
            | Self::DecisionStale { mission_id, .. }
            | Self::MemoryCreated { mission_id, .. }
            | Self::MemorySuperseded { mission_id, .. }
            | Self::MemoryInvalidated { mission_id, .. }
            | Self::MemoryArchived { mission_id, .. }
            | Self::MemoryPromoted { mission_id, .. } => *mission_id,
            Self::CapabilityRequested { mission_id, .. }
            | Self::CapabilityStarted { mission_id, .. }
            | Self::CapabilitySucceeded { mission_id, .. }
            | Self::CapabilityFailed { mission_id, .. }
            | Self::CapabilityCancelled { mission_id, .. }
            | Self::CapabilityRejected { mission_id, .. } => *mission_id,
            Self::SchedulerStateChanged { mission_id, .. } => *mission_id,
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
            Self::CapabilityRequested { parent_id, .. }
            | Self::CapabilityStarted { parent_id, .. }
            | Self::CapabilitySucceeded { parent_id, .. }
            | Self::CapabilityFailed { parent_id, .. }
            | Self::CapabilityCancelled { parent_id, .. }
            | Self::CapabilityRejected { parent_id, .. } => *parent_id,
            Self::SchedulerStateChanged { parent_id, .. } => *parent_id,
            Self::ContextUpdated { parent_id, .. }
            | Self::DecisionRequested { parent_id, .. }
            | Self::DecisionCompleted { parent_id, .. }
            | Self::DecisionFailed { parent_id, .. } => *parent_id,
            Self::DecisionAccepted { parent_id, .. }
            | Self::DecisionRejected { parent_id, .. }
            | Self::DecisionStale { parent_id, .. } => *parent_id,
            Self::MemoryCreated { .. }
            | Self::MemorySuperseded { .. }
            | Self::MemoryInvalidated { .. }
            | Self::MemoryArchived { .. }
            | Self::MemoryPromoted { .. } => None,
        }
    }
}
