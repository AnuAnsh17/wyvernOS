//! Kernel-owned durable authorization grants for capability invocation.

use crate::capability::{CapabilityRequest, RiskLevel};
use crate::domain::{
    now_millis, AgentId, AuthorizationId, CapabilityId, GrantId, MissionId, RuntimeId, Timestamp,
    ToolId,
};
use crate::error::{Result, WyvernError};
use crate::persistence::{PersistedAuthorizationGrant, RuntimeStore};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "identity", rename_all = "snake_case")]
pub enum AuthorizationIssuer {
    Kernel,
    Runtime(RuntimeId),
    Human(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", content = "id", rename_all = "snake_case")]
pub enum AuthorizationScope {
    Mission(MissionId),
    Runtime(RuntimeId),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationGrantDraft {
    pub issuer: AuthorizationIssuer,
    pub subject_agent_id: AgentId,
    pub capability_id: CapabilityId,
    pub allowed_tools: Vec<ToolId>,
    pub scope: AuthorizationScope,
    pub maximum_risk: RiskLevel,
    pub expires_at: Option<Timestamp>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthorizationGrant {
    pub id: GrantId,
    pub issuer: AuthorizationIssuer,
    pub subject_agent_id: AgentId,
    pub capability_id: CapabilityId,
    pub allowed_tools: Vec<ToolId>,
    pub scope: AuthorizationScope,
    pub maximum_risk: RiskLevel,
    pub issued_at: Timestamp,
    pub expires_at: Option<Timestamp>,
    pub revoked_at: Option<Timestamp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationDenialReason {
    NoGrant,
    UnknownGrant,
    WrongAgent,
    WrongCapability,
    WrongTool,
    WrongScope,
    ExcessiveRisk,
    Expired,
    Revoked,
}

impl std::fmt::Display for AuthorizationDenialReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::NoGrant => "no matching authorization grant",
            Self::UnknownGrant => "the requested grant does not exist",
            Self::WrongAgent => "grant subject does not match the runtime agent",
            Self::WrongCapability => "grant does not cover this capability",
            Self::WrongTool => "grant does not cover this tool",
            Self::WrongScope => "grant does not cover this mission or runtime",
            Self::ExcessiveRisk => "tool risk exceeds the grant limit",
            Self::Expired => "authorization grant has expired",
            Self::Revoked => "authorization grant has been revoked",
        };
        f.write_str(value)
    }
}

/// A single kernel-generated authorization result, bound to one request and identity.
/// It intentionally has no public constructor or deserializer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct AuthorizationDecision {
    id: AuthorizationId,
    request_id: crate::domain::CapabilityRequestId,
    runtime_id: RuntimeId,
    mission_id: MissionId,
    agent_id: AgentId,
    capability_id: CapabilityId,
    tool_id: ToolId,
    grant_id: Option<GrantId>,
    allowed: bool,
    denial_reason: Option<AuthorizationDenialReason>,
}

impl AuthorizationDecision {
    pub(crate) fn id(&self) -> AuthorizationId {
        self.id
    }

    pub(crate) fn grant_id(&self) -> Option<GrantId> {
        self.grant_id
    }

    pub(crate) fn allowed(&self) -> bool {
        self.allowed
    }

    pub(crate) fn denial_reason(&self) -> Option<AuthorizationDenialReason> {
        self.denial_reason
    }

    pub(crate) fn agent_id(&self) -> AgentId {
        self.agent_id
    }

    pub(crate) fn matches_request(&self, request: &CapabilityRequest, agent_id: AgentId) -> bool {
        self.request_id == request.request_id
            && self.runtime_id == request.runtime_id
            && self.mission_id == request.mission_id
            && self.agent_id == agent_id
            && self.capability_id == request.capability_id
            && self.tool_id == request.tool_id
    }
}

#[derive(Clone)]
pub(crate) struct AuthorizationManager {
    store: Arc<dyn RuntimeStore>,
}

impl AuthorizationManager {
    pub(crate) fn new(store: Arc<dyn RuntimeStore>) -> Self {
        Self { store }
    }

    pub(crate) async fn issue(&self, draft: AuthorizationGrantDraft) -> Result<AuthorizationGrant> {
        if !matches!(draft.issuer, AuthorizationIssuer::Runtime(_)) {
            return Err(WyvernError::InvalidAuthorizationGrant(
                "runtime-mediated issuance requires a runtime issuer".into(),
            ));
        }
        if draft.allowed_tools.is_empty() {
            return Err(WyvernError::InvalidAuthorizationGrant(
                "a grant must include at least one tool".into(),
            ));
        }
        if matches!(&draft.issuer, AuthorizationIssuer::Human(name) if name.trim().is_empty()) {
            return Err(WyvernError::InvalidAuthorizationGrant(
                "human issuer identity cannot be empty".into(),
            ));
        }
        let issued_at = now_millis();
        if draft
            .expires_at
            .is_some_and(|expires_at| expires_at <= issued_at)
        {
            return Err(WyvernError::InvalidAuthorizationGrant(
                "expiration must be in the future".into(),
            ));
        }
        let mut allowed_tools = draft.allowed_tools;
        allowed_tools.sort_unstable();
        allowed_tools.dedup();
        let grant = AuthorizationGrant {
            id: GrantId::new(),
            issuer: draft.issuer,
            subject_agent_id: draft.subject_agent_id,
            capability_id: draft.capability_id,
            allowed_tools,
            scope: draft.scope,
            maximum_risk: draft.maximum_risk,
            issued_at,
            expires_at: draft.expires_at,
            revoked_at: None,
        };
        self.store
            .insert_authorization_grant(&PersistedAuthorizationGrant::from(&grant))
            .await?;
        Ok(grant)
    }

    pub(crate) async fn get(&self, id: GrantId) -> Result<Option<AuthorizationGrant>> {
        self.store
            .get_authorization_grant(id)
            .await?
            .map(AuthorizationGrant::try_from)
            .transpose()
    }

    pub(crate) async fn revoke(&self, id: GrantId) -> Result<AuthorizationGrant> {
        let grant = self
            .get(id)
            .await?
            .ok_or(WyvernError::AuthorizationGrantNotFound(id))?;
        if grant.revoked_at.is_none() {
            let revoked_at = now_millis();
            if !self
                .store
                .revoke_authorization_grant(id, revoked_at)
                .await?
            {
                return Err(WyvernError::AuthorizationGrantNotFound(id));
            }
            self.get(id)
                .await?
                .ok_or(WyvernError::AuthorizationGrantNotFound(id))
        } else {
            Ok(grant)
        }
    }

    pub(crate) async fn authorize(
        &self,
        request: &CapabilityRequest,
        agent_id: AgentId,
        risk: RiskLevel,
    ) -> Result<AuthorizationDecision> {
        let candidates = if let Some(grant_id) = request.authorization_grant_id {
            match self.get(grant_id).await? {
                Some(grant) => vec![grant],
                None => {
                    return Ok(Self::decision(
                        request,
                        agent_id,
                        None,
                        Some(AuthorizationDenialReason::UnknownGrant),
                    ));
                }
            }
        } else {
            self.store
                .list_authorization_grants()
                .await?
                .into_iter()
                .map(AuthorizationGrant::try_from)
                .collect::<Result<Vec<_>>>()?
        };

        if candidates.is_empty() {
            return Ok(Self::decision(
                request,
                agent_id,
                None,
                Some(AuthorizationDenialReason::NoGrant),
            ));
        }

        let mut first_denial = None;
        for grant in candidates {
            let denial = Self::denial_for(&grant, request, agent_id, risk);
            if denial.is_none() {
                return Ok(Self::decision(request, agent_id, Some(grant.id), None));
            }
            first_denial.get_or_insert((grant.id, denial));
        }
        let (grant_id, denial) = first_denial.expect("non-empty candidate list");
        Ok(Self::decision(request, agent_id, Some(grant_id), denial))
    }

    fn denial_for(
        grant: &AuthorizationGrant,
        request: &CapabilityRequest,
        agent_id: AgentId,
        risk: RiskLevel,
    ) -> Option<AuthorizationDenialReason> {
        if grant.revoked_at.is_some() {
            return Some(AuthorizationDenialReason::Revoked);
        }
        if grant
            .expires_at
            .is_some_and(|expires_at| expires_at <= now_millis())
        {
            return Some(AuthorizationDenialReason::Expired);
        }
        if grant.subject_agent_id != agent_id {
            return Some(AuthorizationDenialReason::WrongAgent);
        }
        if grant.capability_id != request.capability_id {
            return Some(AuthorizationDenialReason::WrongCapability);
        }
        if !grant.allowed_tools.contains(&request.tool_id) {
            return Some(AuthorizationDenialReason::WrongTool);
        }
        let scope_matches = match grant.scope {
            AuthorizationScope::Mission(id) => id == request.mission_id,
            AuthorizationScope::Runtime(id) => id == request.runtime_id,
        };
        if !scope_matches {
            return Some(AuthorizationDenialReason::WrongScope);
        }
        if !risk.is_at_most(grant.maximum_risk) {
            return Some(AuthorizationDenialReason::ExcessiveRisk);
        }
        None
    }

    fn decision(
        request: &CapabilityRequest,
        agent_id: AgentId,
        grant_id: Option<GrantId>,
        denial_reason: Option<AuthorizationDenialReason>,
    ) -> AuthorizationDecision {
        AuthorizationDecision {
            id: AuthorizationId::new(),
            request_id: request.request_id,
            runtime_id: request.runtime_id,
            mission_id: request.mission_id,
            agent_id,
            capability_id: request.capability_id,
            tool_id: request.tool_id,
            grant_id,
            allowed: denial_reason.is_none(),
            denial_reason,
        }
    }
}

impl From<&AuthorizationGrant> for PersistedAuthorizationGrant {
    fn from(grant: &AuthorizationGrant) -> Self {
        let (scope_type, scope_id) = match grant.scope {
            AuthorizationScope::Mission(id) => ("mission", id.to_string()),
            AuthorizationScope::Runtime(id) => ("runtime", id.to_string()),
        };
        Self {
            grant_id: grant.id.to_string(),
            issuer_json: serde_json::to_string(&grant.issuer).expect("issuer serializes"),
            subject_agent_id: grant.subject_agent_id.to_string(),
            capability_id: grant.capability_id.to_string(),
            allowed_tools_json: serde_json::to_string(&grant.allowed_tools)
                .expect("tool IDs serialize"),
            scope_type: scope_type.into(),
            scope_id,
            maximum_risk_json: serde_json::to_string(&grant.maximum_risk)
                .expect("risk level serializes"),
            issued_at: grant.issued_at,
            expires_at: grant.expires_at,
            revoked_at: grant.revoked_at,
        }
    }
}

impl TryFrom<PersistedAuthorizationGrant> for AuthorizationGrant {
    type Error = WyvernError;

    fn try_from(persisted: PersistedAuthorizationGrant) -> Result<Self> {
        let parse = |error: serde_json::Error| {
            WyvernError::InvalidPersistedAuthorization(error.to_string())
        };
        let scope_id = match persisted.scope_type.as_str() {
            "mission" => {
                AuthorizationScope::Mission(MissionId::from_str(&persisted.scope_id).map_err(
                    |error| WyvernError::InvalidPersistedAuthorization(error.to_string()),
                )?)
            }
            "runtime" => {
                AuthorizationScope::Runtime(RuntimeId::from_str(&persisted.scope_id).map_err(
                    |error| WyvernError::InvalidPersistedAuthorization(error.to_string()),
                )?)
            }
            other => {
                return Err(WyvernError::InvalidPersistedAuthorization(format!(
                    "unknown scope type {other:?}"
                )))
            }
        };
        Ok(Self {
            id: GrantId::from_str(&persisted.grant_id)
                .map_err(|error| WyvernError::InvalidPersistedAuthorization(error.to_string()))?,
            issuer: serde_json::from_str(&persisted.issuer_json).map_err(parse)?,
            subject_agent_id: AgentId::from_str(&persisted.subject_agent_id)
                .map_err(|error| WyvernError::InvalidPersistedAuthorization(error.to_string()))?,
            capability_id: CapabilityId::from_str(&persisted.capability_id)
                .map_err(|error| WyvernError::InvalidPersistedAuthorization(error.to_string()))?,
            allowed_tools: serde_json::from_str(&persisted.allowed_tools_json).map_err(parse)?,
            scope: scope_id,
            maximum_risk: serde_json::from_str(&persisted.maximum_risk_json).map_err(parse)?,
            issued_at: persisted.issued_at,
            expires_at: persisted.expires_at,
            revoked_at: persisted.revoked_at,
        })
    }
}
