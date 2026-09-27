//! Wyvern core library surface.

pub mod agent;
pub mod cli;
pub mod context;
pub mod domain;
pub mod error;
pub mod events;
pub mod kernel;
pub mod memory;
pub mod persistence;
pub mod result;

pub use agent::{AgentDefinition, AgentId, AgentKind};
pub use context::{
    ContextCompiler, ContextItem, ContextSnapshot, ContextStore, ContextValue, RuntimeContext,
    StructuredChildOutput,
};
pub use domain::{MemoryId, Mission, MissionId, MissionState, RuntimeId, RuntimeState, Timestamp};
pub use error::{Result, WyvernError};
pub use kernel::{Kernel, Runtime, RuntimeExecutor, RuntimeHandle, RuntimeHooks, RuntimeRegistry};
pub use memory::{
    MemoryDraft, MemoryItem, MemoryMetadata, MemoryMetadataValue, MemoryProvenance, MemoryQuery,
    MemoryScope, MemoryStatus, MemoryStore, MemoryType,
};
pub use result::{RuntimeOutcome, RuntimeResult};
