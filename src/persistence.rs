use crate::agent::AgentDefinition;
use crate::domain::{AgentId, Mission, MissionId, MissionState, RuntimeId, RuntimeState};
use crate::error::{Result, WyvernError};
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
    async fn upsert_mission(&self, mission: &Mission) -> Result<()>;
    async fn upsert_runtime(&self, runtime: &Runtime, agent: &AgentDefinition) -> Result<()>;
    async fn list_missions(&self) -> Result<Vec<PersistedMission>>;
    async fn list_runtimes(&self) -> Result<Vec<PersistedRuntime>>;
    async fn get_mission(&self, mission_id: MissionId) -> Result<Option<PersistedMission>>;
    async fn get_runtime(&self, runtime_id: RuntimeId) -> Result<Option<PersistedRuntime>>;
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

        Ok(())
    }
}

#[async_trait]
impl RuntimeStore for SqliteRuntimeStore {
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
            agent_id: AgentId::from_str(&self.agent_id)?,
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
