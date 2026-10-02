use crate::error::{Result, WyvernError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuntimeId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MissionId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AgentId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MemoryId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CapabilityId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ToolId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SkillId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CapabilityRequestId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AuthorizationId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GrantId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DecisionRequestId(Uuid);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventId(Uuid);

macro_rules! impl_id {
    ($name:ident, $prefix:literal) => {
        impl $name {
            pub fn new() -> Self {
                Self(Uuid::now_v7())
            }

            pub fn as_uuid(self) -> Uuid {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!($prefix, "{}"), self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.to_string())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::from_str(&value).map_err(serde::de::Error::custom)
            }
        }

        impl FromStr for $name {
            type Err = WyvernError;

            fn from_str(value: &str) -> Result<Self> {
                let Some(raw) = value.strip_prefix(concat!($prefix)) else {
                    return Err(WyvernError::Cli(format!("invalid id: {value}")));
                };
                let parsed = Uuid::parse_str(raw)
                    .map_err(|_| WyvernError::Cli(format!("invalid id: {value}")))?;
                Ok(Self(parsed))
            }
        }
    };
}

impl_id!(RuntimeId, "rt_");
impl_id!(MissionId, "mission_");
impl_id!(AgentId, "agent_");
impl_id!(MemoryId, "mem_");
impl_id!(CapabilityId, "cap_");
impl_id!(ToolId, "tool_");
impl_id!(SkillId, "skill_");
impl_id!(CapabilityRequestId, "creq_");
impl_id!(AuthorizationId, "authz_");
impl_id!(GrantId, "grant_");
impl_id!(DecisionRequestId, "dreq_");
impl_id!(EventId, "event_");

impl Default for EventId {
    fn default() -> Self {
        Self::new()
    }
}

pub type Timestamp = i64;

pub fn now_millis() -> Timestamp {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().try_into().unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuntimeState {
    Created,
    Ready,
    Running,
    Waiting,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

impl RuntimeState {
    pub fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Created => matches!(next, Self::Ready | Self::Cancelled | Self::Failed),
            Self::Ready => matches!(next, Self::Running | Self::Cancelled | Self::Failed),
            Self::Running => matches!(
                next,
                Self::Waiting
                    | Self::Completed
                    | Self::Failed
                    | Self::Cancelled
                    | Self::TimedOut
                    | Self::Interrupted
            ),
            Self::Waiting => matches!(
                next,
                Self::Running
                    | Self::Completed
                    | Self::Failed
                    | Self::Cancelled
                    | Self::TimedOut
                    | Self::Interrupted
            ),
            Self::Completed
            | Self::Failed
            | Self::Cancelled
            | Self::TimedOut
            | Self::Interrupted => false,
        }
    }
}

impl FromStr for RuntimeState {
    type Err = WyvernError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "Created" => Ok(Self::Created),
            "Ready" => Ok(Self::Ready),
            "Running" => Ok(Self::Running),
            "Waiting" => Ok(Self::Waiting),
            "Completed" => Ok(Self::Completed),
            "Failed" => Ok(Self::Failed),
            "Cancelled" => Ok(Self::Cancelled),
            "TimedOut" => Ok(Self::TimedOut),
            "Interrupted" => Ok(Self::Interrupted),
            _ => Err(WyvernError::Cli(format!("invalid runtime state: {value}"))),
        }
    }
}

impl fmt::Display for RuntimeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Created => "Created",
            Self::Ready => "Ready",
            Self::Running => "Running",
            Self::Waiting => "Waiting",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
            Self::TimedOut => "TimedOut",
            Self::Interrupted => "Interrupted",
        };
        f.write_str(label)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MissionState {
    Created,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl MissionState {
    pub fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Created => matches!(next, Self::Running | Self::Cancelled | Self::Failed),
            Self::Running => matches!(next, Self::Completed | Self::Failed | Self::Cancelled),
            Self::Completed | Self::Failed | Self::Cancelled => false,
        }
    }
}

impl FromStr for MissionState {
    type Err = WyvernError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "Created" => Ok(Self::Created),
            "Running" => Ok(Self::Running),
            "Completed" => Ok(Self::Completed),
            "Failed" => Ok(Self::Failed),
            "Cancelled" => Ok(Self::Cancelled),
            _ => Err(WyvernError::Cli(format!("invalid mission state: {value}"))),
        }
    }
}

impl fmt::Display for MissionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::Created => "Created",
            Self::Running => "Running",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
            Self::Cancelled => "Cancelled",
        };
        f.write_str(label)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Mission {
    pub id: MissionId,
    pub objective: String,
    pub root_runtime_id: Option<RuntimeId>,
    pub state: MissionState,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

impl Mission {
    pub fn new(objective: impl Into<String>) -> Self {
        let now = now_millis();
        Self {
            id: MissionId::new(),
            objective: objective.into(),
            root_runtime_id: None,
            state: MissionState::Created,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn transition_to(&mut self, next: MissionState) -> Result<()> {
        if !self.state.can_transition_to(next) {
            return Err(WyvernError::InvalidMissionStateTransition {
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        self.updated_at = now_millis();
        Ok(())
    }

    pub fn assign_root_runtime(&mut self, runtime_id: RuntimeId) {
        self.root_runtime_id = Some(runtime_id);
        self.updated_at = now_millis();
    }
}

impl fmt::Display for Mission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [{}]", self.id, self.state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_ids_are_unique() {
        let a = RuntimeId::new();
        let b = RuntimeId::new();
        assert_ne!(a, b);
        assert!(a.to_string().starts_with("rt_"));
    }

    #[test]
    fn runtime_state_machine_rejects_invalid_transitions() {
        let mut runtime_state = RuntimeState::Created;
        assert!(runtime_state.can_transition_to(RuntimeState::Ready));
        runtime_state = RuntimeState::Ready;
        assert!(runtime_state.can_transition_to(RuntimeState::Running));
        runtime_state = RuntimeState::Running;
        assert!(runtime_state.can_transition_to(RuntimeState::Waiting));
        runtime_state = RuntimeState::Waiting;
        assert!(runtime_state.can_transition_to(RuntimeState::Completed));
        assert!(!RuntimeState::Completed.can_transition_to(RuntimeState::Running));
    }

    #[test]
    fn mission_state_machine_rejects_invalid_transitions() {
        assert!(MissionState::Created.can_transition_to(MissionState::Running));
        assert!(!MissionState::Completed.can_transition_to(MissionState::Running));
    }
}
