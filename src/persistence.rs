use crate::agent::AgentDefinition;
use crate::domain::{
    AgentId, EventId, GrantId, Mission, MissionId, MissionState, RuntimeId, RuntimeState,
};
use crate::error::{Result, WyvernError};
use crate::events::{EventEnvelope, RuntimeEvent};
use crate::result::RuntimeResult;
use crate::Runtime;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{FromRow, SqlitePool};
use std::path::Path;
use std::str::FromStr;

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct PersistedMission {
    pub mission_id: String,
    pub objective: String,
    pub root_runtime_id: Option<String>,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct PersistedAgent {
    pub agent_id: String,
    pub name: String,
    pub kind: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct PersistedEvent {
    pub sequence_id: i64,
    pub event_id: String,
    pub mission_id: String,
    pub runtime_id: String,
    pub parent_id: Option<String>,
    pub event_type: String,
    pub timestamp: i64,
    pub payload_json: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct PersistedAuthorizationGrant {
    pub grant_id: String,
    pub issuer_json: String,
    pub subject_agent_id: String,
    pub capability_id: String,
    pub allowed_tools_json: String,
    pub scope_type: String,
    pub scope_id: String,
    pub maximum_risk_json: String,
    pub issued_at: i64,
    pub expires_at: Option<i64>,
    pub revoked_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredEvent {
    pub sequence_id: i64,
    pub envelope: EventEnvelope,
}

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
pub struct PersistedRuntime {
    pub runtime_id: String,
    pub mission_id: String,
    pub agent_id: String,
    pub agent_name: String,
    pub agent_kind: String,
    pub parent_id: Option<String>,
    pub objective: String,
    pub state: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub result_json: Option<String>,
}

#[async_trait]
pub trait RuntimeStore: Send + Sync {
    /// Inserts an Agent identity. Existing IDs must be rejected rather than replaced.
    async fn insert_agent(&self, agent: &AgentDefinition) -> Result<()>;
    async fn list_agents(&self) -> Result<Vec<PersistedAgent>>;
    async fn get_agent(&self, agent_id: AgentId) -> Result<Option<PersistedAgent>>;
    async fn upsert_mission(&self, mission: &Mission) -> Result<()>;
    async fn upsert_runtime(&self, runtime: &Runtime, agent: &AgentDefinition) -> Result<()>;
    async fn list_missions(&self) -> Result<Vec<PersistedMission>>;
    async fn list_runtimes(&self) -> Result<Vec<PersistedRuntime>>;
    async fn get_mission(&self, mission_id: MissionId) -> Result<Option<PersistedMission>>;
    async fn get_runtime(&self, runtime_id: RuntimeId) -> Result<Option<PersistedRuntime>>;
    /// Appends one immutable history record and returns its database sequence.
    async fn append_event(&self, event: &EventEnvelope) -> Result<i64>;
    async fn list_events(&self) -> Result<Vec<PersistedEvent>>;
    async fn events_for_runtime(&self, runtime_id: RuntimeId) -> Result<Vec<PersistedEvent>>;
    async fn events_for_mission(&self, mission_id: MissionId) -> Result<Vec<PersistedEvent>>;
    async fn insert_authorization_grant(&self, grant: &PersistedAuthorizationGrant) -> Result<()>;
    async fn get_authorization_grant(
        &self,
        grant_id: GrantId,
    ) -> Result<Option<PersistedAuthorizationGrant>>;
    async fn list_authorization_grants(&self) -> Result<Vec<PersistedAuthorizationGrant>>;
    async fn revoke_authorization_grant(&self, grant_id: GrantId, revoked_at: i64) -> Result<bool>;
    async fn update_runtime_state(
        &self,
        _runtime_id: RuntimeId,
        _expected: RuntimeState,
        _next: RuntimeState,
        _updated_at: i64,
    ) -> Result<bool> {
        Err(WyvernError::Scheduler(
            "runtime store does not support atomic state recovery".into(),
        ))
    }
    async fn compare_and_transition_runtime_state(
        &self,
        runtime_id: RuntimeId,
        expected: RuntimeState,
        expected_updated_at: i64,
        next: RuntimeState,
        updated_at: i64,
    ) -> Result<bool> {
        let _ = (runtime_id, expected, expected_updated_at, next, updated_at);
        Err(WyvernError::Scheduler(
            "runtime store does not support versioned state transitions".into(),
        ))
    }
    async fn compare_and_transition_runtime_state_with_event(
        &self,
        runtime_id: RuntimeId,
        expected: RuntimeState,
        expected_updated_at: i64,
        next: RuntimeState,
        updated_at: i64,
        event: &EventEnvelope,
    ) -> Result<bool> {
        if !self
            .compare_and_transition_runtime_state(
                runtime_id,
                expected,
                expected_updated_at,
                next,
                updated_at,
            )
            .await?
        {
            return Ok(false);
        }
        self.append_event(event).await?;
        Ok(true)
    }
}

#[derive(Clone)]
pub struct SqliteRuntimeStore {
    pool: SqlitePool,
}

impl SqliteRuntimeStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        let store = Self { pool };
        store.initialize().await?;
        Ok(store)
    }

    async fn initialize(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS missions (
                mission_id TEXT PRIMARY KEY,
                objective TEXT NOT NULL,
                root_runtime_id TEXT NULL,
                state TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS runtimes (
                runtime_id TEXT PRIMARY KEY,
                mission_id TEXT NOT NULL,
                agent_id TEXT NOT NULL,
                agent_name TEXT NOT NULL,
                agent_kind TEXT NOT NULL,
                parent_id TEXT NULL,
                objective TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                result_json TEXT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS agents (
                agent_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                kind TEXT NOT NULL,
                created_at INTEGER NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS event_log (
                sequence_id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                mission_id TEXT NOT NULL,
                runtime_id TEXT NOT NULL,
                parent_id TEXT NULL,
                event_type TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_event_log_runtime_sequence ON event_log(runtime_id, sequence_id)",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_event_log_mission_sequence ON event_log(mission_id, sequence_id)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS authorization_grants (
                grant_id TEXT PRIMARY KEY,
                issuer_json TEXT NOT NULL,
                subject_agent_id TEXT NOT NULL,
                capability_id TEXT NOT NULL,
                allowed_tools_json TEXT NOT NULL,
                scope_type TEXT NOT NULL,
                scope_id TEXT NOT NULL,
                maximum_risk_json TEXT NOT NULL,
                issued_at INTEGER NOT NULL,
                expires_at INTEGER NULL,
                revoked_at INTEGER NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        sqlx::query(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_active_authorization_grant_identity
            ON authorization_grants (
                subject_agent_id, capability_id, allowed_tools_json,
                scope_type, scope_id, maximum_risk_json, IFNULL(expires_at, -1)
            ) WHERE revoked_at IS NULL
            "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl RuntimeStore for SqliteRuntimeStore {
    async fn insert_agent(&self, agent: &AgentDefinition) -> Result<()> {
        let result = sqlx::query(
            "INSERT INTO agents (agent_id, name, kind, created_at) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(agent.id.to_string())
        .bind(&agent.name)
        .bind(agent.kind.to_string())
        .bind(agent.created_at)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
                Err(WyvernError::DuplicateAgent(agent.id))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn list_agents(&self) -> Result<Vec<PersistedAgent>> {
        Ok(sqlx::query_as::<_, PersistedAgent>(
            "SELECT agent_id, name, kind, created_at FROM agents ORDER BY agent_id ASC",
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn get_agent(&self, agent_id: AgentId) -> Result<Option<PersistedAgent>> {
        Ok(sqlx::query_as::<_, PersistedAgent>(
            "SELECT agent_id, name, kind, created_at FROM agents WHERE agent_id = ?1",
        )
        .bind(agent_id.to_string())
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn upsert_mission(&self, mission: &Mission) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO missions (mission_id, objective, root_runtime_id, state, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(mission_id) DO UPDATE SET
                objective = excluded.objective,
                root_runtime_id = excluded.root_runtime_id,
                state = excluded.state,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(mission.id.to_string())
        .bind(&mission.objective)
        .bind(mission.root_runtime_id.map(|id| id.to_string()))
        .bind(mission.state.to_string())
        .bind(mission.created_at)
        .bind(mission.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn upsert_runtime(&self, runtime: &Runtime, agent: &AgentDefinition) -> Result<()> {
        let result_json = runtime
            .result
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|err| WyvernError::ExecutionError(err.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO runtimes (
                runtime_id, mission_id, agent_id, agent_name, agent_kind,
                parent_id, objective, state, created_at, updated_at, result_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(runtime_id) DO UPDATE SET
                mission_id = excluded.mission_id,
                agent_id = excluded.agent_id,
                agent_name = excluded.agent_name,
                agent_kind = excluded.agent_kind,
                parent_id = excluded.parent_id,
                objective = excluded.objective,
                state = excluded.state,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                result_json = excluded.result_json
            "#,
        )
        .bind(runtime.id.to_string())
        .bind(runtime.mission_id.to_string())
        .bind(runtime.agent_id.to_string())
        .bind(&agent.name)
        .bind(agent.kind.to_string())
        .bind(runtime.parent_id.map(|id| id.to_string()))
        .bind(&runtime.objective)
        .bind(runtime.state.to_string())
        .bind(runtime.created_at)
        .bind(runtime.updated_at)
        .bind(result_json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_missions(&self) -> Result<Vec<PersistedMission>> {
        let missions = sqlx::query_as::<_, PersistedMission>(
            r#"
            SELECT mission_id, objective, root_runtime_id, state, created_at, updated_at
            FROM missions
            ORDER BY mission_id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(missions)
    }

    async fn list_runtimes(&self) -> Result<Vec<PersistedRuntime>> {
        let runtimes = sqlx::query_as::<_, PersistedRuntime>(
            r#"
            SELECT runtime_id, mission_id, agent_id, agent_name, agent_kind, parent_id, objective, state, created_at, updated_at, result_json
            FROM runtimes
            ORDER BY runtime_id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(runtimes)
    }

    async fn get_mission(&self, mission_id: MissionId) -> Result<Option<PersistedMission>> {
        let mission = sqlx::query_as::<_, PersistedMission>(
            r#"
            SELECT mission_id, objective, root_runtime_id, state, created_at, updated_at
            FROM missions
            WHERE mission_id = ?1
            "#,
        )
        .bind(mission_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        Ok(mission)
    }

    async fn get_runtime(&self, runtime_id: RuntimeId) -> Result<Option<PersistedRuntime>> {
        let runtime = sqlx::query_as::<_, PersistedRuntime>(
            r#"
            SELECT runtime_id, mission_id, agent_id, agent_name, agent_kind, parent_id, objective, state, created_at, updated_at, result_json
            FROM runtimes
            WHERE runtime_id = ?1
            "#,
        )
        .bind(runtime_id.to_string())
        .fetch_optional(&self.pool)
        .await?;
        Ok(runtime)
    }

    async fn append_event(&self, event: &EventEnvelope) -> Result<i64> {
        let payload_json = serde_json::to_string(&event.payload)
            .map_err(|error| WyvernError::InvalidPersistedEvent(error.to_string()))?;
        let result = sqlx::query(
            r#"
            INSERT INTO event_log (
                event_id, mission_id, runtime_id, parent_id, event_type, timestamp, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
        )
        .bind(event.event_id.to_string())
        .bind(event.mission_id.to_string())
        .bind(event.runtime_id.to_string())
        .bind(event.parent_runtime_id.map(|id| id.to_string()))
        .bind(&event.event_type)
        .bind(event.timestamp)
        .bind(payload_json)
        .execute(&self.pool)
        .await?;
        Ok(result.last_insert_rowid())
    }

    async fn list_events(&self) -> Result<Vec<PersistedEvent>> {
        Ok(sqlx::query_as::<_, PersistedEvent>(
            r#"SELECT sequence_id, event_id, mission_id, runtime_id, parent_id,
                      event_type, timestamp, payload_json
               FROM event_log ORDER BY sequence_id ASC"#,
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn events_for_runtime(&self, runtime_id: RuntimeId) -> Result<Vec<PersistedEvent>> {
        Ok(sqlx::query_as::<_, PersistedEvent>(
            r#"SELECT sequence_id, event_id, mission_id, runtime_id, parent_id,
                      event_type, timestamp, payload_json
               FROM event_log WHERE runtime_id = ?1 ORDER BY sequence_id ASC"#,
        )
        .bind(runtime_id.to_string())
        .fetch_all(&self.pool)
        .await?)
    }

    async fn events_for_mission(&self, mission_id: MissionId) -> Result<Vec<PersistedEvent>> {
        Ok(sqlx::query_as::<_, PersistedEvent>(
            r#"SELECT sequence_id, event_id, mission_id, runtime_id, parent_id,
                      event_type, timestamp, payload_json
               FROM event_log WHERE mission_id = ?1 ORDER BY sequence_id ASC"#,
        )
        .bind(mission_id.to_string())
        .fetch_all(&self.pool)
        .await?)
    }

    async fn insert_authorization_grant(&self, grant: &PersistedAuthorizationGrant) -> Result<()> {
        let result = sqlx::query(
            r#"
            INSERT INTO authorization_grants (
                grant_id, issuer_json, subject_agent_id, capability_id, allowed_tools_json,
                scope_type, scope_id, maximum_risk_json, issued_at, expires_at, revoked_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
        )
        .bind(&grant.grant_id)
        .bind(&grant.issuer_json)
        .bind(&grant.subject_agent_id)
        .bind(&grant.capability_id)
        .bind(&grant.allowed_tools_json)
        .bind(&grant.scope_type)
        .bind(&grant.scope_id)
        .bind(&grant.maximum_risk_json)
        .bind(grant.issued_at)
        .bind(grant.expires_at)
        .bind(grant.revoked_at)
        .execute(&self.pool)
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(sqlx::Error::Database(error)) if error.is_unique_violation() => {
                let id = GrantId::from_str(&grant.grant_id).map_err(|_| {
                    WyvernError::InvalidAuthorizationGrant("invalid generated grant ID".into())
                })?;
                Err(WyvernError::DuplicateAuthorizationGrant(id))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn get_authorization_grant(
        &self,
        grant_id: GrantId,
    ) -> Result<Option<PersistedAuthorizationGrant>> {
        Ok(sqlx::query_as::<_, PersistedAuthorizationGrant>(
            r#"SELECT grant_id, issuer_json, subject_agent_id, capability_id,
                      allowed_tools_json, scope_type, scope_id, maximum_risk_json,
                      issued_at, expires_at, revoked_at
               FROM authorization_grants WHERE grant_id = ?1"#,
        )
        .bind(grant_id.to_string())
        .fetch_optional(&self.pool)
        .await?)
    }

    async fn list_authorization_grants(&self) -> Result<Vec<PersistedAuthorizationGrant>> {
        Ok(sqlx::query_as::<_, PersistedAuthorizationGrant>(
            r#"SELECT grant_id, issuer_json, subject_agent_id, capability_id,
                      allowed_tools_json, scope_type, scope_id, maximum_risk_json,
                      issued_at, expires_at, revoked_at
               FROM authorization_grants ORDER BY grant_id ASC"#,
        )
        .fetch_all(&self.pool)
        .await?)
    }

    async fn revoke_authorization_grant(&self, grant_id: GrantId, revoked_at: i64) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE authorization_grants SET revoked_at = ?2 WHERE grant_id = ?1 AND revoked_at IS NULL",
        )
        .bind(grant_id.to_string())
        .bind(revoked_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn update_runtime_state(
        &self,
        runtime_id: RuntimeId,
        expected: RuntimeState,
        next: RuntimeState,
        updated_at: i64,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE runtimes SET state = ?3, updated_at = ?4 WHERE runtime_id = ?1 AND state = ?2",
        )
        .bind(runtime_id.to_string())
        .bind(expected.to_string())
        .bind(next.to_string())
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn compare_and_transition_runtime_state(
        &self,
        runtime_id: RuntimeId,
        expected: RuntimeState,
        expected_updated_at: i64,
        next: RuntimeState,
        updated_at: i64,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE runtimes SET state = ?4, updated_at = ?5 WHERE runtime_id = ?1 AND state = ?2 AND updated_at = ?3",
        )
        .bind(runtime_id.to_string())
        .bind(expected.to_string())
        .bind(expected_updated_at)
        .bind(next.to_string())
        .bind(updated_at)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    async fn compare_and_transition_runtime_state_with_event(
        &self,
        runtime_id: RuntimeId,
        expected: RuntimeState,
        expected_updated_at: i64,
        next: RuntimeState,
        updated_at: i64,
        event: &EventEnvelope,
    ) -> Result<bool> {
        let payload_json = serde_json::to_string(&event.payload)
            .map_err(|error| WyvernError::InvalidPersistedEvent(error.to_string()))?;
        let mut transaction = self.pool.begin().await?;
        let state_update = sqlx::query(
            "UPDATE runtimes SET state = ?4, updated_at = ?5 WHERE runtime_id = ?1 AND state = ?2 AND updated_at = ?3",
        )
        .bind(runtime_id.to_string())
        .bind(expected.to_string())
        .bind(expected_updated_at)
        .bind(next.to_string())
        .bind(updated_at)
        .execute(&mut *transaction)
        .await?;
        if state_update.rows_affected() != 1 {
            transaction.rollback().await?;
            return Ok(false);
        }
        sqlx::query(
            r#"
            INSERT INTO event_log (
                event_id, mission_id, runtime_id, parent_id, event_type, timestamp, payload_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
        )
        .bind(event.event_id.to_string())
        .bind(event.mission_id.to_string())
        .bind(event.runtime_id.to_string())
        .bind(event.parent_runtime_id.map(|id| id.to_string()))
        .bind(&event.event_type)
        .bind(event.timestamp)
        .bind(payload_json)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(true)
    }
}

impl PersistedRuntime {
    pub fn result(&self) -> Option<RuntimeResult> {
        self.result_json
            .as_ref()
            .and_then(|json| serde_json::from_str(json).ok())
    }

    pub fn into_runtime(&self) -> Result<Runtime> {
        Ok(Runtime {
            id: RuntimeId::from_str(&self.runtime_id)?,
            mission_id: MissionId::from_str(&self.mission_id)?,
            agent_id: AgentId::from_str(&self.agent_id)
                .map_err(|error| WyvernError::InvalidPersistedAgent(error.to_string()))?,
            parent_id: self
                .parent_id
                .as_ref()
                .map(|value| RuntimeId::from_str(value))
                .transpose()?,
            objective: self.objective.clone(),
            state: RuntimeState::from_str(&self.state)?,
            created_at: self.created_at,
            updated_at: self.updated_at,
            children: Vec::new(),
            result: self.result(),
        })
    }
}

impl PersistedAgent {
    pub fn into_agent(&self) -> Result<AgentDefinition> {
        let id = AgentId::from_str(&self.agent_id)
            .map_err(|error| WyvernError::InvalidPersistedAgent(error.to_string()))?;
        let kind = match self.kind.as_str() {
            "Commander" => crate::agent::AgentKind::Commander,
            "WorkerA" => crate::agent::AgentKind::WorkerA,
            "WorkerB" => crate::agent::AgentKind::WorkerB,
            "WorkerA1" => crate::agent::AgentKind::WorkerA1,
            "WorkerA2" => crate::agent::AgentKind::WorkerA2,
            _ => {
                return Err(WyvernError::InvalidPersistedAgent(format!(
                    "unknown agent kind {:?}",
                    self.kind
                )))
            }
        };
        Ok(AgentDefinition {
            id,
            name: self.name.clone(),
            kind,
            created_at: self.created_at,
        })
    }
}

impl PersistedEvent {
    pub fn into_stored_event(&self) -> Result<StoredEvent> {
        let event_id = EventId::from_str(&self.event_id)
            .map_err(|error| WyvernError::InvalidPersistedEvent(error.to_string()))?;
        let mission_id = MissionId::from_str(&self.mission_id)
            .map_err(|error| WyvernError::InvalidPersistedEvent(error.to_string()))?;
        let runtime_id = RuntimeId::from_str(&self.runtime_id)
            .map_err(|error| WyvernError::InvalidPersistedEvent(error.to_string()))?;
        let parent_runtime_id = self
            .parent_id
            .as_ref()
            .map(|value| {
                RuntimeId::from_str(value)
                    .map_err(|error| WyvernError::InvalidPersistedEvent(error.to_string()))
            })
            .transpose()?;
        let payload: RuntimeEvent = serde_json::from_str(&self.payload_json)
            .map_err(|error| WyvernError::InvalidPersistedEvent(error.to_string()))?;
        if payload.event_type() != self.event_type
            || payload.timestamp() != self.timestamp
            || payload.mission_id() != mission_id
            || payload.runtime_id() != runtime_id
            || payload.parent_id() != parent_runtime_id
        {
            return Err(WyvernError::InvalidPersistedEvent(format!(
                "event {} metadata does not match its payload",
                self.event_id
            )));
        }
        Ok(StoredEvent {
            sequence_id: self.sequence_id,
            envelope: EventEnvelope {
                event_id,
                event_type: self.event_type.clone(),
                timestamp: self.timestamp,
                mission_id,
                runtime_id,
                parent_runtime_id,
                payload,
            },
        })
    }
}

impl PersistedMission {
    pub fn into_mission(&self) -> Result<Mission> {
        Ok(Mission {
            id: MissionId::from_str(&self.mission_id)?,
            objective: self.objective.clone(),
            root_runtime_id: self
                .root_runtime_id
                .as_ref()
                .map(|value| RuntimeId::from_str(value))
                .transpose()?,
            state: MissionState::from_str(&self.state)?,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}
