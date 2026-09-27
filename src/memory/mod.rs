//! Durable, typed memory with explicit scope and immutable content history.

use crate::domain::{now_millis, AgentId, MemoryId, MissionId, RuntimeId, Timestamp};
use crate::error::{Result, WyvernError};
use crate::events::{EventBus, RuntimeEvent};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{QueryBuilder, Sqlite, SqlitePool};
use std::collections::BTreeMap;
use std::path::Path;
use std::str::FromStr;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    Mission,
    Runtime,
    Branch,
    Agent,
}

impl MemoryScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Mission => "mission",
            Self::Runtime => "runtime",
            Self::Branch => "branch",
            Self::Agent => "agent",
        }
    }
}

impl FromStr for MemoryScope {
    type Err = WyvernError;

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "mission" => Ok(Self::Mission),
            "runtime" => Ok(Self::Runtime),
            "branch" => Ok(Self::Branch),
            "agent" => Ok(Self::Agent),
            _ => Err(WyvernError::Cli(format!("invalid memory scope: {value}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryType {
    Fact,
    Finding,
    Decision,
    Observation,
    Constraint,
    Failure,
    Result,
    Note,
}

impl MemoryType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fact => "fact",
            Self::Finding => "finding",
            Self::Decision => "decision",
            Self::Observation => "observation",
            Self::Constraint => "constraint",
            Self::Failure => "failure",
            Self::Result => "result",
            Self::Note => "note",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryStatus {
    Active,
    Superseded,
    Invalidated,
    Archived,
}

impl MemoryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Superseded => "superseded",
            Self::Invalidated => "invalidated",
            Self::Archived => "archived",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum MemoryMetadataValue {
    Text(String),
    Boolean(bool),
    Number(f64),
}

pub type MemoryMetadata = BTreeMap<String, MemoryMetadataValue>;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryProvenance {
    pub source_runtime_id: Option<RuntimeId>,
    pub source_agent_id: Option<AgentId>,
    pub source_event_id: Option<String>,
    pub created_at: Timestamp,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryItem {
    pub memory_id: MemoryId,
    pub scope: MemoryScope,
    pub mission_id: MissionId,
    pub runtime_id: Option<RuntimeId>,
    pub branch_id: Option<RuntimeId>,
    pub agent_id: Option<AgentId>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub memory_type: MemoryType,
    pub content: String,
    pub provenance: MemoryProvenance,
    pub status: MemoryStatus,
    pub metadata: MemoryMetadata,
    pub version: u64,
    pub supersedes_memory_id: Option<MemoryId>,
    pub superseded_by_memory_id: Option<MemoryId>,
    #[serde(default)]
    pub invalidated_by_memory_id: Option<MemoryId>,
    pub promoted_from_memory_id: Option<MemoryId>,
    pub dedupe_key: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MemoryDraft {
    pub scope: MemoryScope,
    pub mission_id: MissionId,
    pub runtime_id: Option<RuntimeId>,
    pub branch_id: Option<RuntimeId>,
    pub agent_id: Option<AgentId>,
    pub memory_type: MemoryType,
    pub content: String,
    pub provenance: MemoryProvenance,
    pub metadata: MemoryMetadata,
    pub dedupe_key: Option<String>,
}

impl MemoryDraft {
    pub fn new(
        scope: MemoryScope,
        mission_id: MissionId,
        memory_type: MemoryType,
        content: impl Into<String>,
        provenance: MemoryProvenance,
    ) -> Self {
        Self {
            scope,
            mission_id,
            runtime_id: None,
            branch_id: None,
            agent_id: None,
            memory_type,
            content: content.into(),
            provenance,
            metadata: MemoryMetadata::new(),
            dedupe_key: None,
        }
    }

    fn validate(&self) -> Result<()> {
        let owners_valid = match self.scope {
            MemoryScope::Mission => {
                self.runtime_id.is_none() && self.branch_id.is_none() && self.agent_id.is_none()
            }
            MemoryScope::Runtime => {
                self.runtime_id.is_some() && self.branch_id.is_none() && self.agent_id.is_none()
            }
            MemoryScope::Branch => {
                self.runtime_id.is_none() && self.branch_id.is_some() && self.agent_id.is_none()
            }
            MemoryScope::Agent => {
                self.runtime_id.is_none() && self.branch_id.is_none() && self.agent_id.is_some()
            }
        };
        if !owners_valid {
            return Err(WyvernError::ExecutionError(
                "memory scope must have exactly its corresponding owner".into(),
            ));
        }
        if self.content.trim().is_empty() {
            return Err(WyvernError::ExecutionError(
                "memory content cannot be empty".into(),
            ));
        }
        if self.provenance.source_runtime_id.is_none() {
            return Err(WyvernError::ExecutionError(
                "memory provenance requires a source runtime for event routing".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct MemoryQuery {
    pub mission_id: Option<MissionId>,
    pub scope: Option<MemoryScope>,
    pub runtime_id: Option<RuntimeId>,
    pub branch_id: Option<RuntimeId>,
    pub agent_id: Option<AgentId>,
    pub memory_types: Vec<MemoryType>,
    pub statuses: Vec<MemoryStatus>,
    pub limit: Option<u32>,
}

#[derive(Clone)]
pub struct MemoryStore {
    pool: SqlitePool,
    events: EventBus,
}

impl MemoryStore {
    pub async fn open(path: impl AsRef<Path>, events: EventBus) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .busy_timeout(std::time::Duration::from_secs(5))
            .journal_mode(SqliteJournalMode::Wal);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        Self::from_pool(pool, events).await
    }

    pub async fn in_memory(events: EventBus) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await?;
        Self::from_pool(pool, events).await
    }

    async fn from_pool(pool: SqlitePool, events: EventBus) -> Result<Self> {
        sqlx::query("CREATE TABLE IF NOT EXISTS memories (memory_id TEXT PRIMARY KEY, mission_id TEXT NOT NULL, scope TEXT NOT NULL, runtime_id TEXT NULL, branch_id TEXT NULL, agent_id TEXT NULL, memory_type TEXT NOT NULL, status TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, version INTEGER NOT NULL, supersedes_memory_id TEXT NULL, superseded_by_memory_id TEXT NULL, invalidated_by_memory_id TEXT NULL, promoted_from_memory_id TEXT NULL, dedupe_key TEXT NULL, item_json TEXT NOT NULL)")
            .execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_memories_mission_scope_status_created ON memories(mission_id, scope, status, created_at)")
            .execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_memories_mission_status_created ON memories(mission_id, status, created_at)")
            .execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_memories_mission_type_status_created ON memories(mission_id, memory_type, status, created_at)")
            .execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_memories_runtime ON memories(runtime_id, status, created_at)")
            .execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_memories_branch ON memories(branch_id, status, created_at)")
            .execute(&pool).await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_memories_agent ON memories(agent_id, status, created_at)")
            .execute(&pool).await?;
        Ok(Self { pool, events })
    }

    pub async fn create(&self, draft: MemoryDraft) -> Result<MemoryItem> {
        draft.validate()?;
        let item = self.item_from_draft(draft, 1, None, None);
        self.insert_item(&item).await?;
        self.publish(
            item.mission_id,
            item.provenance
                .source_runtime_id
                .expect("validated provenance"),
            RuntimeEvent::MemoryCreated {
                mission_id: item.mission_id,
                runtime_id: item
                    .provenance
                    .source_runtime_id
                    .expect("validated provenance"),
                memory_id: item.memory_id,
                timestamp: item.created_at,
            },
        )?;
        Ok(item)
    }

    pub async fn get(&self, memory_id: MemoryId) -> Result<Option<MemoryItem>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT item_json FROM memories WHERE memory_id = ?1")
                .bind(memory_id.to_string())
                .fetch_optional(&self.pool)
                .await?;
        row.map(|(json,)| decode(&json)).transpose()
    }

    pub async fn query(&self, query: MemoryQuery) -> Result<Vec<MemoryItem>> {
        let mut builder = QueryBuilder::<Sqlite>::new("SELECT item_json FROM memories WHERE 1 = 1");
        if let Some(id) = query.mission_id {
            builder.push(" AND mission_id = ").push_bind(id.to_string());
        }
        if let Some(scope) = query.scope {
            builder.push(" AND scope = ").push_bind(scope.as_str());
        }
        if let Some(id) = query.runtime_id {
            builder.push(" AND runtime_id = ").push_bind(id.to_string());
        }
        if let Some(id) = query.branch_id {
            builder.push(" AND branch_id = ").push_bind(id.to_string());
        }
        if let Some(id) = query.agent_id {
            builder.push(" AND agent_id = ").push_bind(id.to_string());
        }
        if !query.memory_types.is_empty() {
            builder.push(" AND memory_type IN (");
            let mut separated = builder.separated(", ");
            for memory_type in query.memory_types {
                separated.push_bind(memory_type.as_str());
            }
            separated.push_unseparated(")");
        }
        if !query.statuses.is_empty() {
            builder.push(" AND status IN (");
            let mut separated = builder.separated(", ");
            for status in query.statuses {
                separated.push_bind(status.as_str());
            }
            separated.push_unseparated(")");
        }
        builder.push(" ORDER BY created_at ASC, memory_id ASC");
        if let Some(limit) = query.limit {
            builder.push(" LIMIT ").push_bind(i64::from(limit));
        }
        let rows: Vec<(String,)> = builder.build_query_as().fetch_all(&self.pool).await?;
        rows.into_iter().map(|(json,)| decode(&json)).collect()
    }

    pub async fn list_scope(
        &self,
        mission_id: MissionId,
        scope: MemoryScope,
        owner_id: Option<&str>,
    ) -> Result<Vec<MemoryItem>> {
        let mut query = MemoryQuery {
            mission_id: Some(mission_id),
            scope: Some(scope),
            ..MemoryQuery::default()
        };
        match scope {
            MemoryScope::Mission => {}
            MemoryScope::Runtime => query.runtime_id = owner_id.map(str::parse).transpose()?,
            MemoryScope::Branch => query.branch_id = owner_id.map(str::parse).transpose()?,
            MemoryScope::Agent => query.agent_id = owner_id.map(str::parse).transpose()?,
        }
        if !matches!(scope, MemoryScope::Mission) && owner_id.is_none() {
            return Err(WyvernError::ExecutionError(
                "an owner id is required for this memory scope".into(),
            ));
        }
        self.query(query).await
    }

    pub async fn history(&self, memory_id: MemoryId) -> Result<Vec<MemoryItem>> {
        let item = self
            .get(memory_id)
            .await?
            .ok_or(WyvernError::MemoryNotFound(memory_id))?;
        let items = self
            .query(MemoryQuery {
                mission_id: Some(item.mission_id),
                ..MemoryQuery::default()
            })
            .await?;
        let mut ids = vec![memory_id];
        loop {
            let before = ids.len();
            for candidate in &items {
                if candidate
                    .supersedes_memory_id
                    .is_some_and(|id| ids.contains(&id))
                    || candidate
                        .superseded_by_memory_id
                        .is_some_and(|id| ids.contains(&id))
                    || candidate
                        .invalidated_by_memory_id
                        .is_some_and(|id| ids.contains(&id))
                    || candidate
                        .promoted_from_memory_id
                        .is_some_and(|id| ids.contains(&id))
                {
                    if !ids.contains(&candidate.memory_id) {
                        ids.push(candidate.memory_id);
                    }
                }
                if ids.contains(&candidate.memory_id) {
                    for related in [
                        candidate.supersedes_memory_id,
                        candidate.superseded_by_memory_id,
                        candidate.invalidated_by_memory_id,
                        candidate.promoted_from_memory_id,
                    ]
                    .into_iter()
                    .flatten()
                    {
                        if !ids.contains(&related) {
                            ids.push(related);
                        }
                    }
                }
            }
            if ids.len() == before {
                break;
            }
        }
        Ok(items
            .into_iter()
            .filter(|candidate| ids.contains(&candidate.memory_id))
            .collect())
    }

    pub async fn supersede(
        &self,
        memory_id: MemoryId,
        replacement: MemoryDraft,
    ) -> Result<MemoryItem> {
        replacement.validate()?;
        let previous = self
            .get(memory_id)
            .await?
            .ok_or(WyvernError::MemoryNotFound(memory_id))?;
        if !same_scope(&previous, &replacement) {
            return Err(WyvernError::ExecutionError(
                "replacement memory must remain in the same mission and scope".into(),
            ));
        }
        let new_id = MemoryId::new();
        let mut superseded_item = previous.clone();
        superseded_item.status = MemoryStatus::Superseded;
        superseded_item.updated_at = now_millis();
        superseded_item.superseded_by_memory_id = Some(new_id);
        let replacement_item = self.item_from_draft(
            replacement.clone(),
            previous.version + 1,
            Some(memory_id),
            None,
        );
        let mut tx = self.pool.begin().await?;
        let changed = sqlx::query("UPDATE memories SET status = 'superseded', updated_at = ?2, superseded_by_memory_id = ?3, item_json = ?4 WHERE memory_id = ?1 AND status = 'active'")
            .bind(memory_id.to_string()).bind(superseded_item.updated_at).bind(new_id.to_string()).bind(encode(&superseded_item)?).execute(&mut *tx).await?.rows_affected();
        if changed != 1 {
            return Err(WyvernError::ExecutionError(
                "only active memory can be superseded".into(),
            ));
        }
        let mut replacement_item = replacement_item;
        replacement_item.memory_id = new_id;
        self.insert_item_tx(&mut tx, &replacement_item).await?;
        tx.commit().await?;
        self.publish(
            previous.mission_id,
            replacement
                .provenance
                .source_runtime_id
                .expect("validated provenance"),
            RuntimeEvent::MemorySuperseded {
                mission_id: previous.mission_id,
                runtime_id: replacement
                    .provenance
                    .source_runtime_id
                    .expect("validated provenance"),
                memory_id,
                replacement_memory_id: new_id,
                timestamp: replacement_item.created_at,
            },
        )?;
        self.publish(
            replacement_item.mission_id,
            replacement_item
                .provenance
                .source_runtime_id
                .expect("validated provenance"),
            RuntimeEvent::MemoryCreated {
                mission_id: replacement_item.mission_id,
                runtime_id: replacement_item
                    .provenance
                    .source_runtime_id
                    .expect("validated provenance"),
                memory_id: replacement_item.memory_id,
                timestamp: replacement_item.created_at,
            },
        )?;
        Ok(replacement_item)
    }

    pub async fn invalidate(&self, memory_id: MemoryId) -> Result<MemoryItem> {
        self.set_status(memory_id, MemoryStatus::Invalidated, None)
            .await
    }

    pub async fn invalidate_by(
        &self,
        memory_id: MemoryId,
        invalidating_memory_id: MemoryId,
    ) -> Result<MemoryItem> {
        let invalidator = self
            .get(invalidating_memory_id)
            .await?
            .ok_or(WyvernError::MemoryNotFound(invalidating_memory_id))?;
        let target = self
            .get(memory_id)
            .await?
            .ok_or(WyvernError::MemoryNotFound(memory_id))?;
        if target.mission_id != invalidator.mission_id || memory_id == invalidating_memory_id {
            return Err(WyvernError::ExecutionError(
                "invalidation relationship requires distinct memories in the same mission".into(),
            ));
        }
        self.set_status(
            memory_id,
            MemoryStatus::Invalidated,
            Some(invalidating_memory_id),
        )
        .await
    }

    pub async fn archive(&self, memory_id: MemoryId) -> Result<MemoryItem> {
        self.set_status(memory_id, MemoryStatus::Archived, None)
            .await
    }

    async fn set_status(
        &self,
        memory_id: MemoryId,
        status: MemoryStatus,
        invalidated_by_memory_id: Option<MemoryId>,
    ) -> Result<MemoryItem> {
        let mut item = self
            .get(memory_id)
            .await?
            .ok_or(WyvernError::MemoryNotFound(memory_id))?;
        if item.status != MemoryStatus::Active {
            return Err(WyvernError::ExecutionError(
                "only active memory can change lifecycle status".into(),
            ));
        }
        item.status = status;
        item.updated_at = now_millis();
        item.invalidated_by_memory_id = invalidated_by_memory_id;
        let changed = sqlx::query("UPDATE memories SET status = ?2, updated_at = ?3, invalidated_by_memory_id = ?4, item_json = ?5 WHERE memory_id = ?1 AND status = 'active'")
            .bind(memory_id.to_string()).bind(status.as_str()).bind(item.updated_at)
            .bind(item.invalidated_by_memory_id.map(|id| id.to_string())).bind(encode(&item)?)
            .execute(&self.pool).await?.rows_affected();
        if changed != 1 {
            return Err(WyvernError::ExecutionError(
                "memory lifecycle changed concurrently".into(),
            ));
        }
        let event = match status {
            MemoryStatus::Invalidated => RuntimeEvent::MemoryInvalidated {
                mission_id: item.mission_id,
                runtime_id: item
                    .provenance
                    .source_runtime_id
                    .expect("validated provenance"),
                memory_id,
                timestamp: item.updated_at,
            },
            MemoryStatus::Archived => RuntimeEvent::MemoryArchived {
                mission_id: item.mission_id,
                runtime_id: item
                    .provenance
                    .source_runtime_id
                    .expect("validated provenance"),
                memory_id,
                timestamp: item.updated_at,
            },
            MemoryStatus::Active | MemoryStatus::Superseded => {
                return Err(WyvernError::ExecutionError(format!(
                    "unsupported memory status event: {}",
                    status.as_str()
                )))
            }
        };
        self.publish(
            item.mission_id,
            item.provenance
                .source_runtime_id
                .expect("validated provenance"),
            event,
        )?;
        Ok(item)
    }

    pub async fn promote(
        &self,
        memory_id: MemoryId,
        target_runtime: RuntimeId,
    ) -> Result<MemoryItem> {
        let source = self
            .get(memory_id)
            .await?
            .ok_or(WyvernError::MemoryNotFound(memory_id))?;
        if source.status != MemoryStatus::Active {
            return Err(WyvernError::ExecutionError(
                "only active memory can be promoted".into(),
            ));
        }
        let mut draft = MemoryDraft::new(
            MemoryScope::Runtime,
            source.mission_id,
            source.memory_type,
            source.content.clone(),
            source.provenance.clone(),
        );
        draft.runtime_id = Some(target_runtime);
        draft.metadata = source.metadata.clone();
        draft.dedupe_key = source.dedupe_key.clone();
        let promoted = self.item_from_draft(draft, source.version, None, Some(memory_id));
        self.insert_item(&promoted).await?;
        self.publish(
            source.mission_id,
            target_runtime,
            RuntimeEvent::MemoryPromoted {
                mission_id: source.mission_id,
                runtime_id: target_runtime,
                memory_id: promoted.memory_id,
                source_memory_id: memory_id,
                timestamp: promoted.created_at,
            },
        )?;
        self.publish(
            promoted.mission_id,
            promoted
                .provenance
                .source_runtime_id
                .expect("validated provenance"),
            RuntimeEvent::MemoryCreated {
                mission_id: promoted.mission_id,
                runtime_id: promoted
                    .provenance
                    .source_runtime_id
                    .expect("validated provenance"),
                memory_id: promoted.memory_id,
                timestamp: promoted.created_at,
            },
        )?;
        Ok(promoted)
    }

    pub async fn visible_to_runtime(
        &self,
        mission_id: MissionId,
        runtime_id: RuntimeId,
        agent_id: AgentId,
        branch_ancestors: &[RuntimeId],
    ) -> Result<Vec<MemoryItem>> {
        let items = self
            .query(MemoryQuery {
                mission_id: Some(mission_id),
                statuses: vec![MemoryStatus::Active],
                ..MemoryQuery::default()
            })
            .await?;
        Ok(items
            .into_iter()
            .filter(|item| match item.scope {
                MemoryScope::Mission => true,
                MemoryScope::Runtime => item.runtime_id == Some(runtime_id),
                MemoryScope::Branch => item
                    .branch_id
                    .is_some_and(|id| branch_ancestors.contains(&id)),
                MemoryScope::Agent => item.agent_id == Some(agent_id),
            })
            .collect())
    }

    fn item_from_draft(
        &self,
        draft: MemoryDraft,
        version: u64,
        supersedes_memory_id: Option<MemoryId>,
        promoted_from_memory_id: Option<MemoryId>,
    ) -> MemoryItem {
        let now = now_millis();
        MemoryItem {
            memory_id: MemoryId::new(),
            scope: draft.scope,
            mission_id: draft.mission_id,
            runtime_id: draft.runtime_id,
            branch_id: draft.branch_id,
            agent_id: draft.agent_id,
            created_at: now,
            updated_at: now,
            memory_type: draft.memory_type,
            content: draft.content,
            provenance: draft.provenance,
            status: MemoryStatus::Active,
            metadata: draft.metadata,
            version,
            supersedes_memory_id,
            superseded_by_memory_id: None,
            invalidated_by_memory_id: None,
            promoted_from_memory_id,
            dedupe_key: draft.dedupe_key,
        }
    }

    async fn insert_item(&self, item: &MemoryItem) -> Result<()> {
        sqlx::query("INSERT INTO memories (memory_id, mission_id, scope, runtime_id, branch_id, agent_id, memory_type, status, created_at, updated_at, version, supersedes_memory_id, superseded_by_memory_id, invalidated_by_memory_id, promoted_from_memory_id, dedupe_key, item_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)")
            .bind(item.memory_id.to_string()).bind(item.mission_id.to_string()).bind(item.scope.as_str())
            .bind(item.runtime_id.map(|id| id.to_string())).bind(item.branch_id.map(|id| id.to_string()))
            .bind(item.agent_id.map(|id| id.to_string())).bind(item.memory_type.as_str()).bind(item.status.as_str())
            .bind(item.created_at).bind(item.updated_at).bind(item.version as i64)
            .bind(item.supersedes_memory_id.map(|id| id.to_string())).bind(item.superseded_by_memory_id.map(|id| id.to_string()))
            .bind(item.invalidated_by_memory_id.map(|id| id.to_string()))
            .bind(item.promoted_from_memory_id.map(|id| id.to_string())).bind(&item.dedupe_key).bind(encode(item)?)
            .execute(&self.pool).await?;
        Ok(())
    }

    async fn insert_item_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Sqlite>,
        item: &MemoryItem,
    ) -> Result<()> {
        sqlx::query("INSERT INTO memories (memory_id, mission_id, scope, runtime_id, branch_id, agent_id, memory_type, status, created_at, updated_at, version, supersedes_memory_id, superseded_by_memory_id, invalidated_by_memory_id, promoted_from_memory_id, dedupe_key, item_json) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)")
            .bind(item.memory_id.to_string()).bind(item.mission_id.to_string()).bind(item.scope.as_str())
            .bind(item.runtime_id.map(|id| id.to_string())).bind(item.branch_id.map(|id| id.to_string()))
            .bind(item.agent_id.map(|id| id.to_string())).bind(item.memory_type.as_str()).bind(item.status.as_str())
            .bind(item.created_at).bind(item.updated_at).bind(item.version as i64)
            .bind(item.supersedes_memory_id.map(|id| id.to_string())).bind(item.superseded_by_memory_id.map(|id| id.to_string()))
            .bind(item.invalidated_by_memory_id.map(|id| id.to_string()))
            .bind(item.promoted_from_memory_id.map(|id| id.to_string())).bind(&item.dedupe_key).bind(encode(item)?)
            .execute(&mut **tx).await?;
        Ok(())
    }

    fn publish(
        &self,
        _mission_id: MissionId,
        _runtime_id: RuntimeId,
        event: RuntimeEvent,
    ) -> Result<()> {
        self.events.publish(event)
    }
}

fn same_scope(previous: &MemoryItem, draft: &MemoryDraft) -> bool {
    previous.mission_id == draft.mission_id
        && previous.scope == draft.scope
        && previous.runtime_id == draft.runtime_id
        && previous.branch_id == draft.branch_id
        && previous.agent_id == draft.agent_id
}

fn encode<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|error| WyvernError::ExecutionError(error.to_string()))
}

fn decode<T: for<'de> Deserialize<'de>>(json: &str) -> Result<T> {
    serde_json::from_str(json).map_err(|error| WyvernError::ExecutionError(error.to_string()))
}
