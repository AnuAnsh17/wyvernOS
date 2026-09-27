//! Wyvern core library surface.

pub mod agent;
pub mod cli;
pub mod domain;
pub mod error;
pub mod events;
pub mod kernel;
pub mod persistence;
pub mod result;

pub use agent::{AgentDefinition, AgentId, AgentKind};
pub use domain::{Mission, MissionId, MissionState, RuntimeId, RuntimeState, Timestamp};
pub use error::{Result, WyvernError};
pub use kernel::{Kernel, Runtime, RuntimeExecutor, RuntimeHandle, RuntimeHooks, RuntimeRegistry};
pub use result::{RuntimeOutcome, RuntimeResult};
