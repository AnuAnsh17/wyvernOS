use crate::domain::{MemoryId, MissionId, MissionState, RuntimeId, RuntimeState};
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
    #[error("memory not found: {0}")]
    MemoryNotFound(MemoryId),
    #[error("persistence error: {0}")]
    PersistenceError(#[from] sqlx::Error),
    #[error("event error: {0}")]
    EventError(String),
    #[error("execution error: {0}")]
    ExecutionError(String),
    #[error("cancellation error: {0}")]
    CancellationError(String),
    #[error("cli error: {0}")]
    Cli(String),
}
