//! Wyvern core library surface.

pub mod agent;
pub mod capability;
pub mod cli;
pub mod context;
pub mod decision;
pub mod domain;
pub mod error;
pub mod events;
pub mod kernel;
pub mod memory;
pub mod persistence;
pub mod result;
pub mod scheduler;

pub use agent::{AgentDefinition, AgentId, AgentKind};
pub use capability::{
    CapabilityCancellation, CapabilityDescriptor, CapabilityError, CapabilityExecutor,
    CapabilityKind, CapabilityPermission, CapabilityPermissionScope, CapabilityRegistry,
    CapabilityRequest, CapabilityRequestStatus, CapabilityResult, RiskLevel, ToolDescriptor,
};
pub use context::{
    ContextCompiler, ContextItem, ContextSnapshot, ContextStore, ContextValue, RuntimeContext,
    StructuredChildOutput,
};
pub use decision::{
    AppliedDecision, DecisionApplication, DecisionConstraint, DecisionConstraintKind,
    DecisionContext, DecisionContextFact, DecisionEngine, DecisionEngineProvenance, DecisionError,
    DecisionMemory, DecisionOption, DecisionOptionId, DecisionRequest, DecisionResult,
    DecisionRuntimeState, DecisionType, MockDecisionEngine,
};
pub use domain::{
    CapabilityId, CapabilityRequestId, DecisionRequestId, MemoryId, Mission, MissionId,
    MissionState, RuntimeId, RuntimeState, Timestamp, ToolId,
};
pub use error::{Result, WyvernError};
pub use kernel::{Kernel, Runtime, RuntimeExecutor, RuntimeHandle, RuntimeHooks, RuntimeRegistry};
pub use memory::{
    MemoryDraft, MemoryItem, MemoryMetadata, MemoryMetadataValue, MemoryProvenance, MemoryQuery,
    MemoryScope, MemoryStatus, MemoryStore, MemoryType,
};
pub use result::{RuntimeOutcome, RuntimeResult};
pub use scheduler::{
    RuntimeExecutionError, RuntimePriority, ScheduleOptions, ScheduledRuntime,
    ScheduledRuntimeExecutor, Scheduler, SchedulerConfig, SchedulerSnapshot, SchedulerState,
};
