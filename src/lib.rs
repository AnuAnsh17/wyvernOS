//! Wyvern core library surface.

pub mod agent;
pub mod authorization;
pub mod behavior;
pub mod capability;
pub mod cli;
pub mod context;
pub mod decision;
pub mod domain;
pub mod error;
pub mod events;
pub mod kernel;
pub mod memory;
pub mod models;
pub mod persistence;
pub mod result;
pub mod scheduler;
pub mod skills;

pub use agent::{AgentDefinition, AgentId, AgentKind};
pub use authorization::{
    AuthorizationDecision, AuthorizationDenialReason, AuthorizationGrant, AuthorizationGrantDraft,
    AuthorizationIssuer, AuthorizationScope,
};
pub use behavior::demo::{PauseGate, RuntimeHooks};
pub use behavior::{
    BehaviorError, BehaviorRuntimeHandle, RuntimeBehavior, RuntimeBehaviorRegistry,
    RuntimeExecutionContext,
};
pub use capability::{
    filesystem_read_definition, CapabilityCancellation, CapabilityDescriptor, CapabilityError,
    CapabilityExecutor, CapabilityKind, CapabilityRegistry, CapabilityRequest,
    CapabilityRequestStatus, CapabilityResult, RiskLevel, ToolDescriptor,
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
    AuthorizationId, CapabilityId, CapabilityRequestId, DecisionRequestId, EventId, GrantId,
    MemoryId, Mission, MissionId, MissionState, RuntimeId, RuntimeState, SkillId, Timestamp,
    ToolId,
};
pub use error::{Result, WyvernError};
pub use kernel::{Kernel, Runtime, RuntimeExecutor, RuntimeHandle, RuntimeRegistry};
pub use memory::{
    MemoryDraft, MemoryItem, MemoryMetadata, MemoryMetadataValue, MemoryProvenance, MemoryQuery,
    MemoryScope, MemoryStatus, MemoryStore, MemoryType,
};
pub use models::{
    InferenceRequest, InferenceResponse, Model, ModelCapability, ModelError, ModelPolicy,
    ModelPrivacy, ModelProvider, ModelRouter, ModelUsage,
};
pub use persistence::StoredEvent;
pub use result::{RuntimeOutcome, RuntimeResult};
pub use scheduler::{
    RuntimeExecutionError, RuntimePriority, ScheduleOptions, ScheduledRuntime,
    ScheduledRuntimeExecutor, Scheduler, SchedulerConfig, SchedulerSnapshot, SchedulerState,
};
pub use skills::{
    SkillAdapter, SkillCapabilityBinding, SkillCapabilityDeclaration, SkillError,
    SkillHealthReport, SkillHealthStatus, SkillManifest, SkillRegistry,
};
