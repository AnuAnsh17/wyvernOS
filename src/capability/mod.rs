//! Typed capability requests and the boundary used to dispatch them.

use crate::context::RuntimeContext;
use crate::domain::{
    now_millis, CapabilityId, CapabilityRequestId, MissionId, RuntimeId, Timestamp, ToolId,
};
use crate::events::{EventBus, RuntimeEvent};
use crate::memory::{MemoryDraft, MemoryMetadataValue, MemoryProvenance, MemoryScope, MemoryType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::{watch, RwLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityKind {
    Filesystem,
    Terminal,
    Browser,
    Application,
    Network,
    Ide,
    ExternalAgent,
    Model,
    Custom,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityDescriptor {
    pub id: CapabilityId,
    pub name: String,
    pub version: String,
    pub provider: String,
    pub kind: CapabilityKind,
}

impl CapabilityDescriptor {
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        provider: impl Into<String>,
        kind: CapabilityKind,
    ) -> Self {
        Self {
            id: CapabilityId::new(),
            name: name.into(),
            version: version.into(),
            provider: provider.into(),
            kind,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Moderate,
    High,
    Critical,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolDescriptor {
    pub id: ToolId,
    pub name: String,
    pub description: String,
    pub capability_id: CapabilityId,
    /// JSON Schema representation supplied by the capability implementation.
    pub input_schema: Value,
    pub output_schema: Value,
    pub risk_level: RiskLevel,
}

impl ToolDescriptor {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        capability_id: CapabilityId,
        input_schema: Value,
        output_schema: Value,
        risk_level: RiskLevel,
    ) -> Self {
        Self {
            id: ToolId::new(),
            name: name.into(),
            description: description.into(),
            capability_id,
            input_schema,
            output_schema,
            risk_level,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityRequest {
    pub request_id: CapabilityRequestId,
    pub runtime_id: RuntimeId,
    pub mission_id: MissionId,
    #[serde(default)]
    pub parent_runtime_id: Option<RuntimeId>,
    pub capability_id: CapabilityId,
    pub tool_id: ToolId,
    pub input: Value,
    pub requested_at: Timestamp,
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

impl CapabilityRequest {
    pub fn new(
        runtime_id: RuntimeId,
        mission_id: MissionId,
        capability_id: CapabilityId,
        tool_id: ToolId,
        input: Value,
    ) -> Self {
        Self {
            request_id: CapabilityRequestId::new(),
            runtime_id,
            mission_id,
            parent_runtime_id: None,
            capability_id,
            tool_id,
            input,
            requested_at: now_millis(),
            timeout_ms: None,
            metadata: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityRequestStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
    TimedOut,
    Rejected,
}

#[derive(Clone, Debug, PartialEq, Eq, Error, Serialize, Deserialize)]
#[serde(tag = "code", content = "detail", rename_all = "snake_case")]
pub enum CapabilityError {
    #[error("capability not found: {0}")]
    CapabilityNotFound(CapabilityId),
    #[error("tool not found: {0}")]
    ToolNotFound(ToolId),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("execution failed: {0}")]
    ExecutionFailed(String),
    #[error("execution timed out")]
    ExecutionTimeout,
    #[error("execution cancelled")]
    Cancelled,
    #[error("registry error: {0}")]
    RegistryError(String),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityResult {
    pub request_id: CapabilityRequestId,
    pub status: CapabilityRequestStatus,
    pub output: Option<Value>,
    pub error: Option<CapabilityError>,
    pub started_at: Option<Timestamp>,
    pub completed_at: Timestamp,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

impl CapabilityResult {
    /// Explicitly constructs a memory draft from a successful result. The caller
    /// still chooses whether to persist it and where it should be visible.
    pub fn to_memory_draft(
        &self,
        request: &CapabilityRequest,
        scope: MemoryScope,
        memory_type: MemoryType,
        provenance: MemoryProvenance,
    ) -> std::result::Result<MemoryDraft, CapabilityError> {
        if self.request_id != request.request_id {
            return Err(CapabilityError::InvalidInput(
                "result does not belong to the supplied request".into(),
            ));
        }
        if self.status != CapabilityRequestStatus::Succeeded {
            return Err(CapabilityError::InvalidInput(
                "only a successful result can become memory".into(),
            ));
        }
        let content = serde_json::to_string(self.output.as_ref().ok_or_else(|| {
            CapabilityError::InvalidInput("successful result has no output".into())
        })?)
        .map_err(|error| CapabilityError::InvalidInput(error.to_string()))?;
        let mut draft =
            MemoryDraft::new(scope, request.mission_id, memory_type, content, provenance);
        match scope {
            MemoryScope::Mission => {}
            MemoryScope::Runtime => draft.runtime_id = Some(request.runtime_id),
            MemoryScope::Branch => draft.branch_id = Some(request.runtime_id),
            MemoryScope::Agent => {
                return Err(CapabilityError::InvalidInput(
                    "agent-scoped memory requires an explicit agent owner".into(),
                ));
            }
        }
        draft.metadata.insert(
            "capability_result_request_id".into(),
            MemoryMetadataValue::Text(request.request_id.to_string()),
        );
        Ok(draft)
    }

    /// Copies a result into a context only when explicitly selected by the caller.
    pub fn include_in_context(&self, context: &mut RuntimeContext) {
        context.capability_results.push(self.clone());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", content = "id", rename_all = "snake_case")]
pub enum CapabilityPermissionScope {
    Mission(MissionId),
    Runtime(RuntimeId),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CapabilityPermission {
    pub capability_id: CapabilityId,
    pub allowed_tools: Vec<ToolId>,
    pub scope: CapabilityPermissionScope,
    #[serde(default)]
    pub constraints: BTreeMap<String, Value>,
}

impl CapabilityPermission {
    pub fn allows(&self, request: &CapabilityRequest) -> bool {
        self.capability_id == request.capability_id
            && self.allowed_tools.contains(&request.tool_id)
            && match self.scope {
                CapabilityPermissionScope::Mission(id) => id == request.mission_id,
                CapabilityPermissionScope::Runtime(id) => id == request.runtime_id,
            }
    }
}

#[derive(Clone)]
pub struct CapabilityCancellation {
    sender: watch::Sender<bool>,
}

impl Default for CapabilityCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl CapabilityCancellation {
    pub fn new() -> Self {
        let (sender, _) = watch::channel(false);
        Self { sender }
    }

    pub fn cancel(&self) {
        let _ = self.sender.send(true);
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.sender.subscribe()
    }
}

#[async_trait::async_trait]
pub trait CapabilityExecutor: Send + Sync {
    async fn execute(
        &self,
        request: CapabilityRequest,
    ) -> std::result::Result<Value, CapabilityError>;
}

struct RegisteredCapability {
    descriptor: CapabilityDescriptor,
    tools: Vec<ToolDescriptor>,
    executor: Arc<dyn CapabilityExecutor>,
}

#[derive(Default)]
struct RegistryState {
    capabilities: HashMap<CapabilityId, RegisteredCapability>,
}

#[derive(Clone)]
pub struct CapabilityRegistry {
    state: Arc<RwLock<RegistryState>>,
    events: EventBus,
}

impl CapabilityRegistry {
    pub fn new(events: EventBus) -> Self {
        Self {
            state: Arc::new(RwLock::new(RegistryState::default())),
            events,
        }
    }

    pub async fn register(
        &self,
        descriptor: CapabilityDescriptor,
        tools: Vec<ToolDescriptor>,
        executor: Arc<dyn CapabilityExecutor>,
    ) -> std::result::Result<(), CapabilityError> {
        let mut state = self.state.write().await;
        if state.capabilities.contains_key(&descriptor.id) {
            return Err(CapabilityError::RegistryError(format!(
                "duplicate capability id: {}",
                descriptor.id
            )));
        }
        let mut seen = std::collections::HashSet::new();
        for tool in &tools {
            if tool.capability_id != descriptor.id {
                return Err(CapabilityError::RegistryError(format!(
                    "tool {} belongs to a different capability",
                    tool.id
                )));
            }
            if !seen.insert(tool.id)
                || state.capabilities.values().any(|existing| {
                    existing
                        .tools
                        .iter()
                        .any(|registered| registered.id == tool.id)
                })
            {
                return Err(CapabilityError::RegistryError(format!(
                    "duplicate tool id: {}",
                    tool.id
                )));
            }
        }
        state.capabilities.insert(
            descriptor.id,
            RegisteredCapability {
                descriptor,
                tools,
                executor,
            },
        );
        Ok(())
    }

    pub async fn unregister(
        &self,
        capability_id: CapabilityId,
    ) -> std::result::Result<CapabilityDescriptor, CapabilityError> {
        self.state
            .write()
            .await
            .capabilities
            .remove(&capability_id)
            .map(|registered| registered.descriptor)
            .ok_or(CapabilityError::CapabilityNotFound(capability_id))
    }

    pub async fn get(&self, capability_id: CapabilityId) -> Option<CapabilityDescriptor> {
        self.state
            .read()
            .await
            .capabilities
            .get(&capability_id)
            .map(|entry| entry.descriptor.clone())
    }

    pub async fn list(&self) -> Vec<CapabilityDescriptor> {
        let mut descriptors: Vec<_> = self
            .state
            .read()
            .await
            .capabilities
            .values()
            .map(|entry| entry.descriptor.clone())
            .collect();
        descriptors.sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
        descriptors
    }

    pub async fn list_tools(&self) -> Vec<ToolDescriptor> {
        let mut tools: Vec<_> = self
            .state
            .read()
            .await
            .capabilities
            .values()
            .flat_map(|entry| entry.tools.clone())
            .collect();
        tools.sort_by(|a, b| a.name.cmp(&b.name).then(a.id.cmp(&b.id)));
        tools
    }

    pub async fn resolve_tool(
        &self,
        tool_id: ToolId,
    ) -> std::result::Result<(CapabilityDescriptor, ToolDescriptor), CapabilityError> {
        self.state
            .read()
            .await
            .capabilities
            .values()
            .find_map(|entry| {
                entry
                    .tools
                    .iter()
                    .find(|tool| tool.id == tool_id)
                    .map(|tool| (entry.descriptor.clone(), tool.clone()))
            })
            .ok_or(CapabilityError::ToolNotFound(tool_id))
    }

    pub async fn execute(
        &self,
        mut request: CapabilityRequest,
        permission: &CapabilityPermission,
        cancellation: &CapabilityCancellation,
    ) -> CapabilityResult {
        let requested_at = now_millis();
        self.publish(
            &request,
            RuntimeEvent::CapabilityRequested {
                mission_id: request.mission_id,
                runtime_id: request.runtime_id,
                parent_id: request.parent_runtime_id,
                request_id: request.request_id,
                capability_id: request.capability_id,
                tool_id: request.tool_id,
                timestamp: requested_at,
            },
        );

        if !permission.allows(&request) {
            return self.finish(
                &request,
                CapabilityRequestStatus::Rejected,
                None,
                Some(CapabilityError::PermissionDenied(
                    "permission does not grant this capability, tool, and scope".into(),
                )),
                None,
            );
        }

        let registered = {
            let state = self.state.read().await;
            state
                .capabilities
                .get(&request.capability_id)
                .and_then(|entry| {
                    entry
                        .tools
                        .iter()
                        .any(|tool| tool.id == request.tool_id)
                        .then(|| (entry.descriptor.clone(), entry.executor.clone()))
                })
        };
        let Some((descriptor, executor)) = registered else {
            let error = if self.get(request.capability_id).await.is_none() {
                CapabilityError::CapabilityNotFound(request.capability_id)
            } else {
                CapabilityError::ToolNotFound(request.tool_id)
            };
            return self.finish(
                &request,
                CapabilityRequestStatus::Rejected,
                None,
                Some(error),
                None,
            );
        };
        if descriptor.id != request.capability_id {
            return self.finish(
                &request,
                CapabilityRequestStatus::Rejected,
                None,
                Some(CapabilityError::CapabilityNotFound(request.capability_id)),
                None,
            );
        }

        let start = now_millis();
        self.publish(
            &request,
            RuntimeEvent::CapabilityStarted {
                mission_id: request.mission_id,
                runtime_id: request.runtime_id,
                parent_id: request.parent_runtime_id,
                request_id: request.request_id,
                capability_id: request.capability_id,
                tool_id: request.tool_id,
                timestamp: start,
            },
        );
        let timeout = Duration::from_millis(request.timeout_ms.unwrap_or(30_000).max(1));
        let mut cancelled = cancellation.subscribe();
        let execution = executor.execute(request.clone());
        let outcome = if *cancelled.borrow() {
            Err(CapabilityError::Cancelled)
        } else {
            tokio::select! {
                result = tokio::time::timeout(timeout, execution) => match result {
                    Ok(Ok(output)) => Ok(output),
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(CapabilityError::ExecutionTimeout),
                },
                changed = cancelled.changed() => {
                    let _ = changed;
                    Err(CapabilityError::Cancelled)
                }
            }
        };
        request.requested_at = requested_at;
        match outcome {
            Ok(output) => self.finish(
                &request,
                CapabilityRequestStatus::Succeeded,
                Some(output),
                None,
                Some(start),
            ),
            Err(error @ CapabilityError::Cancelled) => self.finish(
                &request,
                CapabilityRequestStatus::Cancelled,
                None,
                Some(error),
                Some(start),
            ),
            Err(error @ CapabilityError::ExecutionTimeout) => self.finish(
                &request,
                CapabilityRequestStatus::TimedOut,
                None,
                Some(error),
                Some(start),
            ),
            Err(error) => self.finish(
                &request,
                CapabilityRequestStatus::Failed,
                None,
                Some(error),
                Some(start),
            ),
        }
    }

    fn finish(
        &self,
        request: &CapabilityRequest,
        status: CapabilityRequestStatus,
        output: Option<Value>,
        error: Option<CapabilityError>,
        started_at: Option<Timestamp>,
    ) -> CapabilityResult {
        let completed_at = now_millis();
        let event = match status {
            CapabilityRequestStatus::Succeeded => RuntimeEvent::CapabilitySucceeded {
                mission_id: request.mission_id,
                runtime_id: request.runtime_id,
                parent_id: request.parent_runtime_id,
                request_id: request.request_id,
                capability_id: request.capability_id,
                tool_id: request.tool_id,
                timestamp: completed_at,
            },
            CapabilityRequestStatus::Cancelled | CapabilityRequestStatus::TimedOut => {
                RuntimeEvent::CapabilityCancelled {
                    mission_id: request.mission_id,
                    runtime_id: request.runtime_id,
                    parent_id: request.parent_runtime_id,
                    request_id: request.request_id,
                    capability_id: request.capability_id,
                    tool_id: request.tool_id,
                    timed_out: status == CapabilityRequestStatus::TimedOut,
                    timestamp: completed_at,
                }
            }
            CapabilityRequestStatus::Rejected => RuntimeEvent::CapabilityRejected {
                mission_id: request.mission_id,
                runtime_id: request.runtime_id,
                parent_id: request.parent_runtime_id,
                request_id: request.request_id,
                capability_id: request.capability_id,
                tool_id: request.tool_id,
                error: error.as_ref().map(ToString::to_string).unwrap_or_default(),
                timestamp: completed_at,
            },
            _ => RuntimeEvent::CapabilityFailed {
                mission_id: request.mission_id,
                runtime_id: request.runtime_id,
                parent_id: request.parent_runtime_id,
                request_id: request.request_id,
                capability_id: request.capability_id,
                tool_id: request.tool_id,
                error: error.as_ref().map(ToString::to_string).unwrap_or_default(),
                timestamp: completed_at,
            },
        };
        self.publish(request, event);
        CapabilityResult {
            request_id: request.request_id,
            status,
            output,
            error,
            started_at,
            completed_at,
            metadata: BTreeMap::new(),
        }
    }

    fn publish(&self, _request: &CapabilityRequest, event: RuntimeEvent) {
        let _ = self.events.publish(event);
    }
}

/// Deterministic in-process test executor. It performs no external I/O.
pub struct MockEchoExecutor;

#[async_trait::async_trait]
impl CapabilityExecutor for MockEchoExecutor {
    async fn execute(
        &self,
        request: CapabilityRequest,
    ) -> std::result::Result<Value, CapabilityError> {
        let message = request
            .input
            .get("message")
            .and_then(Value::as_str)
            .ok_or_else(|| CapabilityError::InvalidInput("message must be a string".into()))?;
        Ok(serde_json::json!({"message": message}))
    }
}

pub fn mock_echo_definition() -> (
    CapabilityDescriptor,
    ToolDescriptor,
    Arc<dyn CapabilityExecutor>,
) {
    let capability =
        CapabilityDescriptor::new("mock.echo", "1", "wyvern.tests", CapabilityKind::Custom);
    let tool = ToolDescriptor::new(
        "echo",
        "Return the provided message unchanged.",
        capability.id,
        serde_json::json!({"type":"object","required":["message"],"properties":{"message":{"type":"string"}}}),
        serde_json::json!({"type":"object","properties":{"message":{"type":"string"}}}),
        RiskLevel::Low,
    );
    (capability, tool, Arc::new(MockEchoExecutor))
}
