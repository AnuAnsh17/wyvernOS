//! Typed capability requests and the boundary used to dispatch them.

use crate::authorization::AuthorizationDecision;
use crate::context::RuntimeContext;
use crate::domain::{
    now_millis, CapabilityId, CapabilityRequestId, GrantId, MissionId, RuntimeId, Timestamp, ToolId,
};
use crate::events::{EventBus, RuntimeEvent};
use crate::memory::{MemoryDraft, MemoryMetadataValue, MemoryProvenance, MemoryScope, MemoryType};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::path::{Component, Path, PathBuf};
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
    Medium,
    /// Legacy name retained for the risk level introduced before Stage 7A.
    Moderate,
    High,
    Critical,
}

impl RiskLevel {
    pub(crate) fn is_at_most(self, maximum: Self) -> bool {
        fn rank(value: RiskLevel) -> u8 {
            match value {
                RiskLevel::Low => 0,
                RiskLevel::Medium | RiskLevel::Moderate => 1,
                RiskLevel::High => 2,
                RiskLevel::Critical => 3,
            }
        }
        rank(self) <= rank(maximum)
    }
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
    /// Optional grant selector. This is only a reference to a kernel-issued
    /// grant; it does not itself authorize the request.
    #[serde(default)]
    pub authorization_grant_id: Option<GrantId>,
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
            authorization_grant_id: None,
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
    #[error("event persistence failed: {0}")]
    EventPersistence(String),
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
        // Retain cancellation even when authorization or request setup has not
        // subscribed yet; `send` would drop the value when there are no receivers.
        self.sender.send_replace(true);
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

    pub(crate) async fn execute_authorized(
        &self,
        mut request: CapabilityRequest,
        authorization: AuthorizationDecision,
        agent_id: crate::domain::AgentId,
        cancellation: &CapabilityCancellation,
    ) -> CapabilityResult {
        let requested_at = now_millis();
        if let Err(error) = self
            .publish(
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
            )
            .await
        {
            return self.event_failure(&request, error, None);
        }

        let decision_matches = authorization.matches_request(&request, agent_id);
        let allowed = authorization.allowed() && decision_matches;
        let denial_reason = if decision_matches {
            authorization
                .denial_reason()
                .map(|reason| reason.to_string())
        } else {
            Some("authorization decision is not bound to this request".into())
        };
        let checked_at = now_millis();
        if let Err(error) = self
            .publish(
                &request,
                RuntimeEvent::AuthorizationChecked {
                    mission_id: request.mission_id,
                    runtime_id: request.runtime_id,
                    parent_id: request.parent_runtime_id,
                    authorization_id: authorization.id(),
                    request_id: request.request_id,
                    grant_id: authorization.grant_id(),
                    agent_id: authorization.agent_id(),
                    capability_id: request.capability_id,
                    tool_id: request.tool_id,
                    allowed,
                    reason: denial_reason.clone(),
                    timestamp: checked_at,
                },
            )
            .await
        {
            return self.event_failure(&request, error, None);
        }

        if !allowed {
            if let Err(error) = self
                .publish(
                    &request,
                    RuntimeEvent::AuthorizationDenied {
                        mission_id: request.mission_id,
                        runtime_id: request.runtime_id,
                        parent_id: request.parent_runtime_id,
                        authorization_id: authorization.id(),
                        request_id: request.request_id,
                        grant_id: authorization.grant_id(),
                        reason: denial_reason.clone().unwrap_or_else(|| "denied".into()),
                        timestamp: checked_at,
                    },
                )
                .await
            {
                return self.event_failure(&request, error, None);
            }
            return self
                .finish(
                    &request,
                    CapabilityRequestStatus::Rejected,
                    None,
                    Some(CapabilityError::PermissionDenied(
                        denial_reason.unwrap_or_else(|| "authorization denied".into()),
                    )),
                    None,
                )
                .await;
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
            return self
                .finish(
                    &request,
                    CapabilityRequestStatus::Rejected,
                    None,
                    Some(error),
                    None,
                )
                .await;
        };
        if descriptor.id != request.capability_id {
            return self
                .finish(
                    &request,
                    CapabilityRequestStatus::Rejected,
                    None,
                    Some(CapabilityError::CapabilityNotFound(request.capability_id)),
                    None,
                )
                .await;
        }

        let start = now_millis();
        if let Err(error) = self
            .publish(
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
            )
            .await
        {
            return self.event_failure(&request, error, Some(start));
        }
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
            Ok(output) => {
                self.finish(
                    &request,
                    CapabilityRequestStatus::Succeeded,
                    Some(output),
                    None,
                    Some(start),
                )
                .await
            }
            Err(error @ CapabilityError::Cancelled) => {
                self.finish(
                    &request,
                    CapabilityRequestStatus::Cancelled,
                    None,
                    Some(error),
                    Some(start),
                )
                .await
            }
            Err(error @ CapabilityError::ExecutionTimeout) => {
                self.finish(
                    &request,
                    CapabilityRequestStatus::TimedOut,
                    None,
                    Some(error),
                    Some(start),
                )
                .await
            }
            Err(error) => {
                self.finish(
                    &request,
                    CapabilityRequestStatus::Failed,
                    None,
                    Some(error),
                    Some(start),
                )
                .await
            }
        }
    }

    pub(crate) async fn reject(
        &self,
        request: CapabilityRequest,
        error: CapabilityError,
    ) -> CapabilityResult {
        let requested_at = now_millis();
        if let Err(event_error) = self
            .publish(
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
            )
            .await
        {
            return self.event_failure(&request, event_error, None);
        }
        self.finish(
            &request,
            CapabilityRequestStatus::Rejected,
            None,
            Some(error),
            None,
        )
        .await
    }

    async fn finish(
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
        if let Err(error) = self.publish(request, event).await {
            return self.event_failure(request, error, started_at);
        }
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

    fn event_failure(
        &self,
        request: &CapabilityRequest,
        error: crate::error::WyvernError,
        started_at: Option<Timestamp>,
    ) -> CapabilityResult {
        CapabilityResult {
            request_id: request.request_id,
            status: CapabilityRequestStatus::Failed,
            output: None,
            error: Some(CapabilityError::EventPersistence(error.to_string())),
            started_at,
            completed_at: now_millis(),
            metadata: BTreeMap::new(),
        }
    }

    async fn publish(
        &self,
        _request: &CapabilityRequest,
        event: RuntimeEvent,
    ) -> crate::error::Result<()> {
        self.events.publish(event).await
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

struct FilesystemReadExecutor {
    root: PathBuf,
    max_bytes: u64,
}

#[async_trait::async_trait]
impl CapabilityExecutor for FilesystemReadExecutor {
    async fn execute(
        &self,
        request: CapabilityRequest,
    ) -> std::result::Result<Value, CapabilityError> {
        let relative_path = request
            .input
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| CapabilityError::InvalidInput("path must be a string".into()))?;
        let relative_path = Path::new(relative_path);
        if relative_path.as_os_str().is_empty()
            || relative_path.is_absolute()
            || relative_path
                .components()
                .any(|component| component == Component::ParentDir)
        {
            return Err(CapabilityError::InvalidInput(
                "path must be a non-empty relative path without parent traversal".into(),
            ));
        }

        let root = self.root.clone();
        let max_bytes = self.max_bytes;
        let requested_path = relative_path.to_owned();
        tokio::task::spawn_blocking(move || {
            let root = std::fs::canonicalize(&root).map_err(|error| {
                CapabilityError::ExecutionFailed(format!("filesystem root is unavailable: {error}"))
            })?;
            let path = root.join(&requested_path);
            let canonical_path = std::fs::canonicalize(&path).map_err(|error| {
                CapabilityError::ExecutionFailed(format!("cannot read requested path: {error}"))
            })?;
            if !canonical_path.starts_with(&root) {
                return Err(CapabilityError::PermissionDenied(
                    "requested path escapes the filesystem workspace".into(),
                ));
            }
            let metadata = std::fs::metadata(&canonical_path).map_err(|error| {
                CapabilityError::ExecutionFailed(format!("cannot inspect requested path: {error}"))
            })?;
            if !metadata.is_file() {
                return Err(CapabilityError::InvalidInput(
                    "filesystem.read requires a regular file".into(),
                ));
            }
            if metadata.len() > max_bytes {
                return Err(CapabilityError::InvalidInput(format!(
                    "file exceeds the {} byte limit",
                    max_bytes
                )));
            }
            let bytes = std::fs::read(&canonical_path).map_err(|error| {
                CapabilityError::ExecutionFailed(format!("cannot read requested path: {error}"))
            })?;
            match String::from_utf8(bytes.clone()) {
                Ok(content) => Ok(serde_json::json!({
                    "path": requested_path.to_string_lossy(),
                    "bytes": bytes.len(),
                    "encoding": "utf-8",
                    "content": content,
                })),
                Err(_) => Ok(serde_json::json!({
                    "path": requested_path.to_string_lossy(),
                    "bytes": bytes.len(),
                    "encoding": "binary",
                    "content_bytes": bytes,
                })),
            }
        })
        .await
        .map_err(|error| CapabilityError::ExecutionFailed(error.to_string()))?
    }
}

pub fn filesystem_read_definition(
    root: impl Into<PathBuf>,
    max_bytes: u64,
) -> (
    CapabilityDescriptor,
    ToolDescriptor,
    Arc<dyn CapabilityExecutor>,
) {
    let capability = CapabilityDescriptor::new(
        "filesystem.read",
        "1",
        "wyvern.core",
        CapabilityKind::Filesystem,
    );
    let tool = ToolDescriptor::new(
        "read",
        "Read a regular file within the configured workspace boundary.",
        capability.id,
        serde_json::json!({
            "type": "object",
            "required": ["path"],
            "properties": {"path": {"type": "string"}}
        }),
        serde_json::json!({
            "type": "object",
            "required": ["path", "bytes", "encoding"]
        }),
        RiskLevel::Low,
    );
    (
        capability,
        tool,
        Arc::new(FilesystemReadExecutor {
            root: root.into(),
            max_bytes: max_bytes.max(1),
        }),
    )
}
