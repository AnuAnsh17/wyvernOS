use crate::behavior::BehaviorError;
use crate::decision::DecisionError;
use crate::domain::{AgentId, GrantId, MemoryId, MissionId, MissionState, RuntimeId, RuntimeState};
use crate::scheduler::SchedulerState;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, WyvernError>;

#[derive(Debug, Error)]
pub enum WyvernError {
    #[error("invalid runtime state transition from {from} to {to}")]
    InvalidStateTransition {
        from: RuntimeState,
        to: RuntimeState,
    },
    #[error("invalid mission state transition from {from} to {to}")]
    InvalidMissionStateTransition {
        from: MissionState,
        to: MissionState,
    },
    #[error("invalid scheduler state transition from {from:?} to {to:?}")]
    InvalidSchedulerTransition {
        from: SchedulerState,
        to: SchedulerState,
    },
    #[error("parent not found: {0}")]
    ParentNotFound(RuntimeId),
    #[error("cross-mission hierarchy error: parent {parent_id} belongs to {parent_mission_id} but runtime targets {child_mission_id}")]
    CrossMissionHierarchyError {
        parent_id: RuntimeId,
        parent_mission_id: MissionId,
        child_mission_id: MissionId,
    },
    #[error("runtime not found: {0}")]
    RuntimeNotFound(RuntimeId),
    #[error("mission not found: {0}")]
    MissionNotFound(MissionId),
    #[error("agent not found: {0}")]
    AgentNotFound(AgentId),
    #[error("agent already exists: {0}")]
    DuplicateAgent(AgentId),
    #[error("persisted agent is invalid: {0}")]
    InvalidPersistedAgent(String),
    #[error("runtime {runtime_id} references missing agent {agent_id}")]
    RuntimeAgentNotFound {
        runtime_id: RuntimeId,
        agent_id: AgentId,
    },
    #[error("persisted event is invalid: {0}")]
    InvalidPersistedEvent(String),
    #[error("authorization grant not found: {0}")]
    AuthorizationGrantNotFound(GrantId),
    #[error("duplicate authorization grant: {0}")]
    DuplicateAuthorizationGrant(GrantId),
    #[error("invalid authorization grant: {0}")]
    InvalidAuthorizationGrant(String),
    #[error("persisted authorization grant is invalid: {0}")]
    InvalidPersistedAuthorization(String),
    #[error("memory not found: {0}")]
    MemoryNotFound(MemoryId),
    #[error("persistence error: {0}")]
    PersistenceError(#[from] sqlx::Error),
    #[error("event error: {0}")]
    EventError(String),
    #[error("execution error: {0}")]
    ExecutionError(String),
    #[error("decision error: {0}")]
    Decision(#[from] DecisionError),
    #[error("runtime behavior error: {0}")]
    Behavior(#[from] BehaviorError),
    #[error("cancellation error: {0}")]
    CancellationError(String),
    #[error("scheduler error: {0}")]
    Scheduler(String),
    #[error("cli error: {0}")]
    Cli(String),
}
