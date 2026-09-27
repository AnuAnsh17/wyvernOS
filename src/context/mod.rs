//! Branch-local runtime context, deterministic compilation, and durable snapshots.

use crate::domain::{now_millis, MissionId, RuntimeId, RuntimeState, Timestamp};
use crate::error::{Result, WyvernError};
use crate::events::EventEnvelope;
use crate::result::{RuntimeOutcome, RuntimeResult};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedMutexGuard};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RuntimeStateView {
    pub runtime_id: RuntimeId,
    pub mission_id: MissionId,
    pub agent_id: crate::agent::AgentId,
    pub parent_id: Option<RuntimeId>,
    pub lifecycle_state: RuntimeState,
    pub objective: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub result: Option<RuntimeResult>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextSourceType {
    RuntimeState,
    ParentContext,
    LocalContext,
    Event,
    ChildResult,
    SystemMetadata,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextProvenance {
    pub source_type: ContextSourceType,
    pub source_runtime_id: Option<RuntimeId>,
    pub source_event_id: Option<String>,
    pub created_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ContextValue {
    Text(String),
    Boolean(bool),
    Number(f64),
    Structured(Value),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextItem {
    pub key: String,
    pub value: ContextValue,
    pub provenance: ContextProvenance,
    pub priority: Option<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RuntimeContext {
    pub runtime: Option<RuntimeStateView>,
    pub objective: String,
    pub inherited_context: Vec<ContextItem>,
    pub local_facts: Vec<ContextItem>,
    pub relevant_events: Vec<EventEnvelope>,
    pub child_outputs: Vec<StructuredChildOutput>,
    pub constraints: Vec<String>,
    pub metadata: Vec<ContextItem>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StructuredChildOutput {
    pub runtime_id: RuntimeId,
    pub mission_id: MissionId,
    pub status: RuntimeOutcome,
    pub result_type: String,
    pub data: Value,
    pub created_at: Timestamp,
}

impl StructuredChildOutput {
    pub fn from_result(
        mission_id: MissionId,
        result: &RuntimeResult,
        result_type: impl Into<String>,
    ) -> Self {
        Self {
            runtime_id: result.runtime_id,
            mission_id,
            status: result.status,
            result_type: result_type.into(),
            data: result.data.clone(),
            created_at: result.timestamp,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ContextSnapshot {
    pub runtime_id: RuntimeId,
    pub mission_id: MissionId,
    pub version: u64,
    pub created_at: Timestamp,
    pub context: RuntimeContext,
    pub parent_snapshot: Option<(RuntimeId, u64)>,
}

#[derive(Clone, Debug, Default)]
pub struct ContextCompileInput {
    pub runtime: Option<RuntimeStateView>,
    pub objective: String,
    /// Explicitly selected items from the parent's context. The compiler never
    /// fetches parent/global state itself.
    pub inherited_context: Vec<ContextItem>,
    pub local_facts: Vec<ContextItem>,
    pub relevant_events: Vec<EventEnvelope>,
    pub child_outputs: Vec<StructuredChildOutput>,
    pub constraints: Vec<String>,
    pub metadata: Vec<ContextItem>,
}

pub struct ContextCompiler;

impl ContextCompiler {
    pub fn compile(input: ContextCompileInput) -> RuntimeContext {
        RuntimeContext {
            runtime: input.runtime,
            objective: input.objective,
            inherited_context: input.inherited_context,
            local_facts: input.local_facts,
            relevant_events: input.relevant_events,
            child_outputs: input.child_outputs,
            constraints: input.constraints,
            metadata: input.metadata,
        }
    }
}

#[derive(Clone)]
pub struct ContextStore {
    pool: SqlitePool,
    runtime_locks: Arc<Mutex<HashMap<RuntimeId, Arc<Mutex<()>>>>>,
}

impl ContextStore {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        Self::from_pool(pool).await
    }

    pub async fn in_memory() -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        Self::from_pool(pool).await
    }

    async fn from_pool(pool: SqlitePool) -> Result<Self> {
        sqlx::query("CREATE TABLE IF NOT EXISTS runtime_contexts (runtime_id TEXT PRIMARY KEY, mission_id TEXT NOT NULL, context_json TEXT NOT NULL, version INTEGER NOT NULL)")
            .execute(&pool).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS context_snapshots (runtime_id TEXT NOT NULL, mission_id TEXT NOT NULL, version INTEGER NOT NULL, created_at INTEGER NOT NULL, snapshot_json TEXT NOT NULL, PRIMARY KEY(runtime_id, version))")
            .execute(&pool).await?;
        Ok(Self {
            pool,
            runtime_locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    async fn lock_runtime(&self, runtime_id: RuntimeId) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.runtime_locks.lock().await;
            locks
                .entry(runtime_id)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    pub async fn create(
        &self,
        runtime_id: RuntimeId,
        mission_id: MissionId,
        context: RuntimeContext,
    ) -> Result<RuntimeContext> {
        let _guard = self.lock_runtime(runtime_id).await;
        let json = encode(&context)?;
        sqlx::query("INSERT INTO runtime_contexts (runtime_id, mission_id, context_json, version) VALUES (?1, ?2, ?3, 0) ON CONFLICT(runtime_id) DO NOTHING")
            .bind(runtime_id.to_string()).bind(mission_id.to_string()).bind(json).execute(&self.pool).await?;
        self.get(runtime_id)
            .await?
            .ok_or(WyvernError::RuntimeNotFound(runtime_id))
    }

    pub async fn get(&self, runtime_id: RuntimeId) -> Result<Option<RuntimeContext>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT context_json FROM runtime_contexts WHERE runtime_id = ?1")
                .bind(runtime_id.to_string())
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(json,)| decode(&json)).transpose()
    }

    /// Applies the caller's update atomically to this runtime's branch only.
    pub async fn update(
        &self,
        runtime_id: RuntimeId,
        update: impl Fn(&mut RuntimeContext),
    ) -> Result<RuntimeContext> {
        let _guard = self.lock_runtime(runtime_id).await;
        loop {
            let row: Option<(String, i64)> = sqlx::query_as(
                "SELECT context_json, version FROM runtime_contexts WHERE runtime_id = ?1",
            )
            .bind(runtime_id.to_string())
            .fetch_optional(&self.pool)
            .await?;
            let Some((json, version)) = row else {
                return Err(WyvernError::RuntimeNotFound(runtime_id));
            };
            let mut context: RuntimeContext = decode(&json)?;
            update(&mut context);
            let changed = sqlx::query(
                "UPDATE runtime_contexts SET context_json = ?2, version = version + 1 WHERE runtime_id = ?1 AND version = ?3",
            )
            .bind(runtime_id.to_string())
            .bind(encode(&context)?)
            .bind(version)
            .execute(&self.pool)
            .await?
            .rows_affected();
            if changed == 1 {
                return Ok(context);
            }
            // Another write won between the read and update. Reapply this
            // deterministic mutation to the newly stored context.
        }
    }

    pub async fn create_snapshot(
        &self,
        runtime_id: RuntimeId,
        parent_snapshot: Option<(RuntimeId, u64)>,
    ) -> Result<ContextSnapshot> {
        let _guard = self.lock_runtime(runtime_id).await;
        let mut tx = self.pool.begin().await?;
        let row: Option<(String, String, i64)> = sqlx::query_as(
            "SELECT mission_id, context_json, version FROM runtime_contexts WHERE runtime_id = ?1",
        )
        .bind(runtime_id.to_string())
        .fetch_optional(&mut *tx)
        .await?;
        let Some((mission, json, _context_version)) = row else {
            return Err(WyvernError::RuntimeNotFound(runtime_id));
        };
        let (version,): (i64,) = sqlx::query_as(
            "SELECT COALESCE(MAX(version), -1) + 1 FROM context_snapshots WHERE runtime_id = ?1",
        )
        .bind(runtime_id.to_string())
        .fetch_one(&mut *tx)
        .await?;
        let snapshot = ContextSnapshot {
            runtime_id,
            mission_id: MissionId::from_str(&mission)?,
            version: version as u64,
            created_at: now_millis(),
            context: decode(&json)?,
            parent_snapshot,
        };
        sqlx::query("INSERT INTO context_snapshots (runtime_id, mission_id, version, created_at, snapshot_json) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(runtime_id, version) DO NOTHING")
            .bind(runtime_id.to_string()).bind(mission).bind(version).bind(snapshot.created_at).bind(encode(&snapshot)?).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    pub async fn latest_snapshot(&self, runtime_id: RuntimeId) -> Result<Option<ContextSnapshot>> {
        let row: Option<(String,)> = sqlx::query_as("SELECT snapshot_json FROM context_snapshots WHERE runtime_id = ?1 ORDER BY version DESC LIMIT 1")
            .bind(runtime_id.to_string()).fetch_optional(&self.pool).await?;
        row.map(|(json,)| decode(&json)).transpose()
    }

    pub async fn snapshot_history(&self, runtime_id: RuntimeId) -> Result<Vec<ContextSnapshot>> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT snapshot_json FROM context_snapshots WHERE runtime_id = ?1 ORDER BY version ASC")
            .bind(runtime_id.to_string()).fetch_all(&self.pool).await?;
        rows.into_iter().map(|(json,)| decode(&json)).collect()
    }
}

fn encode<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|e| WyvernError::ExecutionError(e.to_string()))
}
fn decode<T: for<'de> Deserialize<'de>>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(|e| WyvernError::ExecutionError(e.to_string()))
}
