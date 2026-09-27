use crate::domain::{MissionId, MissionState, RuntimeId, RuntimeState};
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
    #[error("runtime not found: {0}")]
    RuntimeNotFound(RuntimeId),
    #[error("mission not found: {0}")]
    MissionNotFound(MissionId),
    #[error("persistence error: {0}")]
    Persistence(#[from] sqlx::Error),
    #[error("event error: {0}")]
    Event(String),
    #[error("execution error: {0}")]
    Execution(String),
    #[error("cli error: {0}")]
    Cli(String),
}
