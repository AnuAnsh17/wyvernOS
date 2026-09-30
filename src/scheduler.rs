//! Local, bounded runtime execution coordination.

use crate::domain::{now_millis, MissionId, RuntimeId, RuntimeState, Timestamp};
use crate::error::{Result, WyvernError};
use crate::events::RuntimeEvent;
use crate::kernel::{Kernel, Runtime};
use crate::result::{RuntimeOutcome, RuntimeResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{watch, Mutex, Notify, RwLock};
use tokio::task::JoinHandle;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerState {
    Queued,
    Runnable,
    Running,
    Waiting,
    Completed,
    Failed,
    Cancelled,
    TimedOut,
    Blocked,
    Interrupted,
}

impl SchedulerState {
    fn can_transition_to(self, next: Self) -> bool {
        match self {
            Self::Queued => matches!(
                next,
                Self::Runnable | Self::Blocked | Self::Cancelled | Self::Interrupted
            ),
            Self::Runnable => matches!(
                next,
                Self::Running | Self::Waiting | Self::Cancelled | Self::Blocked
            ),
            Self::Running => matches!(
                next,
                Self::Waiting
                    | Self::Completed
                    | Self::Failed
                    | Self::Cancelled
                    | Self::TimedOut
                    | Self::Queued
                    | Self::Interrupted
            ),
            Self::Waiting => matches!(
                next,
                Self::Runnable | Self::Running | Self::Cancelled | Self::Interrupted
            ),
            Self::Blocked => matches!(next, Self::Cancelled),
            Self::Completed
            | Self::Failed
            | Self::Cancelled
            | Self::TimedOut
            | Self::Interrupted => false,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Runnable => "runnable",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::TimedOut => "timed_out",
            Self::Blocked => "blocked",
            Self::Interrupted => "interrupted",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "runnable" => Ok(Self::Runnable),
            "running" => Ok(Self::Running),
            "waiting" => Ok(Self::Waiting),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "timed_out" => Ok(Self::TimedOut),
            "blocked" => Ok(Self::Blocked),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(WyvernError::Scheduler(format!(
                "invalid scheduler state: {value}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimePriority {
    Background,
    Low,
    Normal,
    High,
    Critical,
}

impl RuntimePriority {
    fn score(self) -> i64 {
        match self {
            Self::Background => 0,
            Self::Low => 1,
            Self::Normal => 2,
            Self::High => 3,
            Self::Critical => 4,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    pub max_concurrent_runtimes: usize,
    pub default_timeout: Duration,
    pub priority_aging_interval: Duration,
    pub max_retry_backoff: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_concurrent_runtimes: 4,
            default_timeout: Duration::from_secs(300),
            priority_aging_interval: Duration::from_secs(30),
            max_retry_backoff: Duration::from_secs(300),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduleOptions {
    pub priority: RuntimePriority,
    pub dependencies: Vec<RuntimeId>,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub timeout: Option<Duration>,
}

impl Default for ScheduleOptions {
    fn default() -> Self {
        Self {
            priority: RuntimePriority::Normal,
            dependencies: Vec::new(),
            max_retries: 0,
            retry_delay: Duration::from_millis(100),
            timeout: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScheduledRuntime {
    pub runtime_id: RuntimeId,
    pub mission_id: MissionId,
    pub parent_id: Option<RuntimeId>,
    pub state: SchedulerState,
    pub priority: RuntimePriority,
    pub dependencies: Vec<RuntimeId>,
    pub attempt: u32,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    pub timeout_ms: u64,
    pub queued_at: Timestamp,
    pub started_at: Option<Timestamp>,
    pub updated_at: Timestamp,
    pub not_before: Option<Timestamp>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SchedulerSnapshot {
    pub total: usize,
    pub queued: usize,
    pub runnable: usize,
    pub running: usize,
    pub waiting: usize,
    pub completed: usize,
    pub failed: usize,
    pub cancelled: usize,
    pub timed_out: usize,
    pub blocked: usize,
    pub interrupted: usize,
    pub runtimes: Vec<ScheduledRuntime>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeExecutionError {
    pub message: String,
    pub retryable: bool,
}

impl RuntimeExecutionError {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: true,
        }
    }
    pub fn permanent(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }
}

#[async_trait]
pub trait ScheduledRuntimeExecutor: Send + Sync + 'static {
    async fn execute(
        &self,
        runtime: Runtime,
        cancellation: watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError>;
}

#[derive(Clone)]
struct SchedulerStore {
    pool: SqlitePool,
}

impl SchedulerStore {
    async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path.as_ref())
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS scheduler_runtime (runtime_id TEXT PRIMARY KEY, mission_id TEXT NOT NULL, parent_id TEXT NULL, state TEXT NOT NULL, priority INTEGER NOT NULL, dependencies_json TEXT NOT NULL, attempt INTEGER NOT NULL, max_retries INTEGER NOT NULL, retry_delay_ms INTEGER NOT NULL DEFAULT 100, timeout_ms INTEGER NOT NULL, queued_at INTEGER NOT NULL, started_at INTEGER NULL, updated_at INTEGER NOT NULL, not_before INTEGER NULL, error TEXT NULL)")
            .execute(&pool).await?;
        Ok(Self { pool })
    }

    async fn insert(&self, entry: &ScheduledRuntime) -> Result<()> {
        sqlx::query("INSERT INTO scheduler_runtime (runtime_id, mission_id, parent_id, state, priority, dependencies_json, attempt, max_retries, retry_delay_ms, timeout_ms, queued_at, started_at, updated_at, not_before, error) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)")
            .bind(entry.runtime_id.to_string()).bind(entry.mission_id.to_string())
            .bind(entry.parent_id.map(|id| id.to_string())).bind(entry.state.as_str())
            .bind(entry.priority.score()).bind(encode(&entry.dependencies)?).bind(entry.attempt as i64)
            .bind(entry.max_retries as i64).bind(entry.retry_delay_ms as i64).bind(entry.timeout_ms as i64).bind(entry.queued_at)
            .bind(entry.started_at).bind(entry.updated_at).bind(entry.not_before).bind(&entry.error)
            .execute(&self.pool).await.map_err(|error| if is_unique(&error) {
                WyvernError::Scheduler(format!("runtime is already scheduled: {}", entry.runtime_id))
            } else { WyvernError::PersistenceError(error) })?;
        Ok(())
    }

    async fn get(&self, id: RuntimeId) -> Result<Option<ScheduledRuntime>> {
        let row = sqlx::query_as::<_, ScheduleRow>("SELECT runtime_id, mission_id, parent_id, state, priority, dependencies_json, attempt, max_retries, retry_delay_ms, timeout_ms, queued_at, started_at, updated_at, not_before, error FROM scheduler_runtime WHERE runtime_id = ?1")
            .bind(id.to_string()).fetch_optional(&self.pool).await?;
        row.map(ScheduleRow::into_entry).transpose()
    }

    async fn list(&self) -> Result<Vec<ScheduledRuntime>> {
        let rows = sqlx::query_as::<_, ScheduleRow>("SELECT runtime_id, mission_id, parent_id, state, priority, dependencies_json, attempt, max_retries, retry_delay_ms, timeout_ms, queued_at, started_at, updated_at, not_before, error FROM scheduler_runtime ORDER BY queued_at, runtime_id")
            .fetch_all(&self.pool).await?;
        rows.into_iter().map(ScheduleRow::into_entry).collect()
    }

    async fn set_state(
        &self,
        id: RuntimeId,
        expected: &[SchedulerState],
        next: SchedulerState,
        error: Option<String>,
        not_before: Option<Timestamp>,
    ) -> Result<bool> {
        if expected.is_empty() {
            return Ok(false);
        }
        for from in expected {
            if !from.can_transition_to(next) {
                return Err(WyvernError::InvalidSchedulerTransition {
                    from: *from,
                    to: next,
                });
            }
        }
        let current = self.get(id).await?;
        let Some(current) = current else {
            return Ok(false);
        };
        if !expected.contains(&current.state) {
            return Ok(false);
        }
        let result = sqlx::query("UPDATE scheduler_runtime SET state = ?2, updated_at = ?3, error = ?4, not_before = ?5 WHERE runtime_id = ?1 AND state = ?6")
            .bind(id.to_string()).bind(next.as_str()).bind(now_millis()).bind(error)
            .bind(not_before).bind(current.state.as_str()).execute(&self.pool).await?;
        Ok(result.rows_affected() == 1)
    }

    async fn claim_next(&self, now: Timestamp, aging_ms: i64) -> Result<Option<ScheduledRuntime>> {
        let mut tx = self.pool.begin().await?;
        let rows = sqlx::query_as::<_, ScheduleRow>("SELECT runtime_id, mission_id, parent_id, state, priority, dependencies_json, attempt, max_retries, retry_delay_ms, timeout_ms, queued_at, started_at, updated_at, not_before, error FROM scheduler_runtime WHERE state IN ('queued','runnable') AND (not_before IS NULL OR not_before <= ?1) ORDER BY (priority + CASE WHEN ?2 > 0 THEN CAST((?1 - queued_at) / ?2 AS INTEGER) ELSE 0 END) DESC, queued_at ASC, runtime_id ASC")
            .bind(now).bind(aging_ms).fetch_all(&mut *tx).await?;
        for row in rows {
            let entry = row.into_entry()?;
            let mut waiting = false;
            let mut failed = false;
            for dependency in &entry.dependencies {
                match sqlx::query_as::<_, (String,)>(
                    "SELECT state FROM scheduler_runtime WHERE runtime_id = ?1",
                )
                .bind(dependency.to_string())
                .fetch_optional(&mut *tx)
                .await?
                {
                    Some((state,)) if state == SchedulerState::Completed.as_str() => {}
                    Some((state,))
                        if matches!(
                            SchedulerState::parse(&state)?,
                            SchedulerState::Failed
                                | SchedulerState::Cancelled
                                | SchedulerState::TimedOut
                                | SchedulerState::Blocked
                                | SchedulerState::Interrupted
                        ) =>
                    {
                        failed = true
                    }
                    _ => waiting = true,
                }
            }
            if failed {
                sqlx::query("UPDATE scheduler_runtime SET state='blocked', error='dependency did not complete successfully', updated_at=?2 WHERE runtime_id=?1 AND state IN ('queued','runnable')")
                    .bind(entry.runtime_id.to_string()).bind(now).execute(&mut *tx).await?;
                continue;
            }
            if waiting {
                continue;
            }
            let changed = sqlx::query("UPDATE scheduler_runtime SET state='running', attempt=attempt+1, started_at=?2, updated_at=?2, not_before=NULL WHERE runtime_id=?1 AND state IN ('queued','runnable')")
                .bind(entry.runtime_id.to_string()).bind(now).execute(&mut *tx).await?.rows_affected();
            if changed == 1 {
                tx.commit().await?;
                return self.get(entry.runtime_id).await;
            }
        }
        tx.commit().await?;
        Ok(None)
    }

    async fn next_ready_at(&self) -> Result<Option<Timestamp>> {
        let row = sqlx::query_as::<_, (Option<i64>,)>("SELECT MIN(not_before) FROM scheduler_runtime WHERE state='queued' AND not_before IS NOT NULL")
            .fetch_one(&self.pool).await?;
        Ok(row.0)
    }
}

#[derive(sqlx::FromRow)]
struct ScheduleRow {
    runtime_id: String,
    mission_id: String,
    parent_id: Option<String>,
    state: String,
    priority: i64,
    dependencies_json: String,
    attempt: i64,
    max_retries: i64,
    retry_delay_ms: i64,
    timeout_ms: i64,
    queued_at: i64,
    started_at: Option<i64>,
    updated_at: i64,
    not_before: Option<i64>,
    error: Option<String>,
}

impl ScheduleRow {
    fn into_entry(self) -> Result<ScheduledRuntime> {
        let priority = match self.priority {
            0 => RuntimePriority::Background,
            1 => RuntimePriority::Low,
            2 => RuntimePriority::Normal,
            3 => RuntimePriority::High,
            _ => RuntimePriority::Critical,
        };
        let state = SchedulerState::parse(&self.state)?;
        Ok(ScheduledRuntime {
            runtime_id: RuntimeId::from_str(&self.runtime_id)?,
            mission_id: MissionId::from_str(&self.mission_id)?,
            parent_id: self
                .parent_id
                .as_deref()
                .map(RuntimeId::from_str)
                .transpose()?,
            state,
            priority,
            dependencies: serde_json::from_str(&self.dependencies_json)
                .map_err(|error| WyvernError::Scheduler(error.to_string()))?,
            attempt: self.attempt as u32,
            max_retries: self.max_retries as u32,
            retry_delay_ms: self.retry_delay_ms as u64,
            timeout_ms: self.timeout_ms as u64,
            queued_at: self.queued_at,
            started_at: self.started_at,
            updated_at: self.updated_at,
            not_before: self.not_before,
            error: self.error,
        })
    }
}

fn encode<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(|error| WyvernError::Scheduler(error.to_string()))
}

fn is_unique(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| code == "1555" || code == "2067")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WorkerSignal {
    None,
    Wait,
    Cancel,
}

struct SchedulerShared {
    kernel: Kernel,
    executor: Arc<dyn ScheduledRuntimeExecutor>,
    store: SchedulerStore,
    config: SchedulerConfig,
    notify: Notify,
    changed: Notify,
    active: RwLock<std::collections::HashMap<RuntimeId, watch::Sender<WorkerSignal>>>,
}

pub struct Scheduler {
    shared: Arc<SchedulerShared>,
    shutdown: watch::Sender<bool>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl Scheduler {
    pub async fn inspect_database(path: impl AsRef<Path>) -> Result<SchedulerSnapshot> {
        snapshot(SchedulerStore::open(path).await?.list().await?)
    }

    pub async fn open(
        kernel: Kernel,
        database_path: impl AsRef<Path>,
        executor: Arc<dyn ScheduledRuntimeExecutor>,
        config: SchedulerConfig,
    ) -> Result<Self> {
        if config.max_concurrent_runtimes == 0 {
            return Err(WyvernError::Scheduler(
                "max_concurrent_runtimes must be positive".into(),
            ));
        }
        let store = SchedulerStore::open(database_path).await?;
        let shared = Arc::new(SchedulerShared {
            kernel,
            executor,
            store,
            config,
            notify: Notify::new(),
            changed: Notify::new(),
            active: RwLock::new(std::collections::HashMap::new()),
        });
        recover_running(&shared).await?;
        let (shutdown, receiver) = watch::channel(false);
        let mut workers = Vec::new();
        for index in 0..shared.config.max_concurrent_runtimes {
            workers.push(tokio::spawn(worker_loop(
                shared.clone(),
                receiver.clone(),
                index,
            )));
        }
        Ok(Self {
            shared,
            shutdown,
            workers: Mutex::new(workers),
        })
    }

    pub fn concurrency_limit(&self) -> usize {
        self.shared.config.max_concurrent_runtimes
    }

    pub async fn submit(
        &self,
        runtime_id: RuntimeId,
        options: ScheduleOptions,
    ) -> Result<ScheduledRuntime> {
        let runtime = self.shared.kernel.get_runtime(runtime_id).await?;
        if runtime.state != RuntimeState::Ready {
            return Err(WyvernError::Scheduler(format!(
                "runtime must be Ready to submit (found {})",
                runtime.state
            )));
        }
        for dependency in &options.dependencies {
            if *dependency == runtime_id {
                return Err(WyvernError::Scheduler(
                    "runtime cannot depend on itself".into(),
                ));
            }
            let dependency_mission =
                if let Some(predecessor) = self.shared.store.get(*dependency).await? {
                    predecessor.mission_id
                } else {
                    let predecessor = self.shared.kernel.get_runtime(*dependency).await?;
                    if predecessor.state != RuntimeState::Completed {
                        return Err(WyvernError::Scheduler(format!(
                            "dependency is not scheduled: {dependency}"
                        )));
                    }
                    predecessor.mission_id
                };
            if dependency_mission != runtime.mission_id {
                return Err(WyvernError::Scheduler(
                    "dependencies must belong to the same mission".into(),
                ));
            }
        }
        let now = now_millis();
        let state = if options.dependencies.is_empty() {
            SchedulerState::Runnable
        } else {
            SchedulerState::Queued
        };
        let entry = ScheduledRuntime {
            runtime_id,
            mission_id: runtime.mission_id,
            parent_id: runtime.parent_id,
            state,
            priority: options.priority,
            dependencies: options.dependencies,
            attempt: 0,
            max_retries: options.max_retries,
            retry_delay_ms: options.retry_delay.as_millis().min(u64::MAX as u128) as u64,
            timeout_ms: options
                .timeout
                .map(|d| d.as_millis() as u64)
                .unwrap_or(self.shared.config.default_timeout.as_millis() as u64)
                .max(1),
            queued_at: now,
            started_at: None,
            updated_at: now,
            not_before: None,
            error: None,
        };
        self.shared.store.insert(&entry).await?;
        publish_state(&self.shared.kernel, &entry, None).await?;
        refresh_dependencies(&self.shared).await?;
        self.shared.notify.notify_one();
        Ok(self.shared.store.get(runtime_id).await?.unwrap_or(entry))
    }

    pub async fn get(&self, runtime_id: RuntimeId) -> Result<ScheduledRuntime> {
        self.shared
            .store
            .get(runtime_id)
            .await?
            .ok_or(WyvernError::RuntimeNotFound(runtime_id))
    }

    pub async fn inspect(&self) -> Result<SchedulerSnapshot> {
        snapshot(self.shared.store.list().await?)
    }

    pub async fn inspect_mission(&self, mission_id: MissionId) -> Result<SchedulerSnapshot> {
        snapshot(
            self.shared
                .store
                .list()
                .await?
                .into_iter()
                .filter(|entry| entry.mission_id == mission_id)
                .collect(),
        )
    }

    pub async fn wait(&self, runtime_id: RuntimeId) -> Result<()> {
        let entry = self.get(runtime_id).await?;
        match entry.state {
            SchedulerState::Running => {
                signal_active(&self.shared, runtime_id, WorkerSignal::Wait).await?;
            }
            SchedulerState::Runnable => {
                if self
                    .shared
                    .store
                    .set_state(
                        runtime_id,
                        &[entry.state],
                        SchedulerState::Waiting,
                        None,
                        None,
                    )
                    .await?
                {
                    let updated = self.get(runtime_id).await?;
                    publish_state(&self.shared.kernel, &updated, None).await?;
                } else if self.get(runtime_id).await?.state == SchedulerState::Running {
                    signal_active(&self.shared, runtime_id, WorkerSignal::Wait).await?;
                } else {
                    return Err(WyvernError::Scheduler(format!(
                        "runtime changed before wait: {runtime_id}"
                    )));
                }
            }
            _ => {
                return Err(WyvernError::InvalidSchedulerTransition {
                    from: entry.state,
                    to: SchedulerState::Waiting,
                })
            }
        }
        self.shared.changed.notify_one();
        Ok(())
    }

    pub async fn resume(&self, runtime_id: RuntimeId) -> Result<()> {
        let current = self.get(runtime_id).await?;
        if current.state != SchedulerState::Waiting {
            return Err(WyvernError::InvalidSchedulerTransition {
                from: current.state,
                to: SchedulerState::Runnable,
            });
        }
        if !self
            .shared
            .store
            .set_state(
                runtime_id,
                &[SchedulerState::Waiting],
                SchedulerState::Runnable,
                None,
                None,
            )
            .await?
        {
            return Err(WyvernError::Scheduler(format!(
                "runtime is not waiting: {runtime_id}"
            )));
        }
        let entry = self.get(runtime_id).await?;
        publish_state(&self.shared.kernel, &entry, Some("resumed".into())).await?;
        self.shared.notify.notify_one();
        Ok(())
    }

    pub async fn cancel(&self, runtime_id: RuntimeId) -> Result<()> {
        let entry = self.get(runtime_id).await?;
        if entry.state == SchedulerState::Running {
            signal_active(&self.shared, runtime_id, WorkerSignal::Cancel).await?;
            return Ok(());
        }
        if matches!(
            entry.state,
            SchedulerState::Queued
                | SchedulerState::Runnable
                | SchedulerState::Waiting
                | SchedulerState::Blocked
        ) {
            if self
                .shared
                .store
                .set_state(
                    runtime_id,
                    &[entry.state],
                    SchedulerState::Cancelled,
                    None,
                    None,
                )
                .await?
            {
                cancel_kernel_runtime(&self.shared.kernel, runtime_id).await?;
                let updated = self.get(runtime_id).await?;
                publish_state(&self.shared.kernel, &updated, None).await?;
                self.shared.changed.notify_one();
            }
        }
        Ok(())
    }

    pub async fn cancel_subtree(&self, root_id: RuntimeId) -> Result<()> {
        let mut pending = vec![root_id];
        while let Some(id) = pending.pop() {
            let runtime = self.shared.kernel.get_runtime(id).await?;
            pending.extend(runtime.children);
            if self.shared.store.get(id).await?.is_some() {
                self.cancel(id).await?;
            } else {
                let _ = cancel_kernel_runtime(&self.shared.kernel, id).await;
            }
        }
        Ok(())
    }

    pub async fn cancel_mission(&self, mission_id: MissionId) -> Result<()> {
        for runtime in self
            .shared
            .kernel
            .list_runtimes()
            .await
            .into_iter()
            .filter(|runtime| runtime.mission_id == mission_id)
        {
            if self.shared.store.get(runtime.id).await?.is_some() {
                self.cancel(runtime.id).await?;
            } else if matches!(
                runtime.state,
                RuntimeState::Ready | RuntimeState::Running | RuntimeState::Waiting
            ) {
                let _ = cancel_kernel_runtime(&self.shared.kernel, runtime.id).await;
            }
        }
        Ok(())
    }

    /// Waits without holding a scheduler worker; parent work is restarted when children settle.
    pub async fn wait_for_children(&self, parent_id: RuntimeId) -> Result<()> {
        if self.get(parent_id).await?.state == SchedulerState::Running {
            self.wait(parent_id).await?;
        }
        loop {
            let changed = self.shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if children_terminal(&self.shared, parent_id).await? {
                break;
            }
            changed.await;
        }
        if self.get(parent_id).await?.state == SchedulerState::Waiting {
            self.resume(parent_id).await?;
        }
        Ok(())
    }

    pub async fn shutdown(&self) {
        let _ = self.shutdown.send(true);
        self.shared.notify.notify_waiters();
        for sender in self.shared.active.read().await.values() {
            let _ = sender.send(WorkerSignal::Cancel);
        }
        let workers = std::mem::take(&mut *self.workers.lock().await);
        for worker in workers {
            let _ = worker.await;
        }
    }
}

async fn signal_active(
    shared: &SchedulerShared,
    runtime_id: RuntimeId,
    signal: WorkerSignal,
) -> Result<()> {
    for _ in 0..1000 {
        if let Some(sender) = shared.active.read().await.get(&runtime_id) {
            let _ = sender.send(signal);
            return Ok(());
        }
        if shared
            .store
            .get(runtime_id)
            .await?
            .is_none_or(|entry| entry.state != SchedulerState::Running)
        {
            return Err(WyvernError::Scheduler(format!(
                "runtime stopped running before signal: {runtime_id}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    Err(WyvernError::Scheduler(format!(
        "runtime worker did not become active: {runtime_id}"
    )))
}

fn is_terminal(state: SchedulerState) -> bool {
    matches!(
        state,
        SchedulerState::Completed
            | SchedulerState::Failed
            | SchedulerState::Cancelled
            | SchedulerState::TimedOut
            | SchedulerState::Blocked
            | SchedulerState::Interrupted
    )
}

fn snapshot(runtimes: Vec<ScheduledRuntime>) -> Result<SchedulerSnapshot> {
    let mut status = SchedulerSnapshot {
        total: runtimes.len(),
        runtimes,
        ..SchedulerSnapshot::default()
    };
    for runtime in &status.runtimes {
        match runtime.state {
            SchedulerState::Queued => status.queued += 1,
            SchedulerState::Runnable => status.runnable += 1,
            SchedulerState::Running => status.running += 1,
            SchedulerState::Waiting => status.waiting += 1,
            SchedulerState::Completed => status.completed += 1,
            SchedulerState::Failed => status.failed += 1,
            SchedulerState::Cancelled => status.cancelled += 1,
            SchedulerState::TimedOut => status.timed_out += 1,
            SchedulerState::Blocked => status.blocked += 1,
            SchedulerState::Interrupted => status.interrupted += 1,
        }
    }
    Ok(status)
}

async fn worker_loop(
    shared: Arc<SchedulerShared>,
    mut shutdown: watch::Receiver<bool>,
    _index: usize,
) {
    loop {
        if *shutdown.borrow() {
            return;
        }
        let notified = shared.notify.notified();
        tokio::pin!(notified);
        let aging_ms = shared
            .config
            .priority_aging_interval
            .as_millis()
            .min(i64::MAX as u128) as i64;
        match shared.store.claim_next(now_millis(), aging_ms).await {
            Ok(Some(entry)) => {
                // Keep waking idle workers while runnable work remains. Tokio Notify
                // coalesces notify_one calls, so submission-time notifications alone
                // can otherwise leave the configured worker pool underutilized.
                shared.notify.notify_one();
                run_claimed(shared.clone(), entry, shutdown.clone()).await;
                continue;
            }
            Ok(None) => {}
            Err(error) => tracing::error!(%error, "scheduler claim failed"),
        }
        let wake = shared.store.next_ready_at().await.ok().flatten();
        tokio::select! {
            _ = &mut notified => {},
            _ = async {
                if let Some(at) = wake {
                    tokio::time::sleep(Duration::from_millis(at.saturating_sub(now_millis()) as u64)).await
                } else { std::future::pending::<()>().await }
            } => {},
            changed = shutdown.changed() => if changed.is_err() || *shutdown.borrow() { return; },
        }
    }
}

async fn run_claimed(
    shared: Arc<SchedulerShared>,
    entry: ScheduledRuntime,
    mut shutdown: watch::Receiver<bool>,
) {
    if let Err(error) = publish_state(&shared.kernel, &entry, None).await {
        tracing::error!(%error, runtime_id = %entry.runtime_id, "scheduler event append failed");
    }
    let runtime = match shared.kernel.get_runtime(entry.runtime_id).await {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(%error, "claimed runtime missing");
            return;
        }
    };
    if !matches!(runtime.state, RuntimeState::Ready | RuntimeState::Waiting) {
        let _ = shared
            .store
            .set_state(
                entry.runtime_id,
                &[SchedulerState::Running],
                SchedulerState::Blocked,
                Some(format!("runtime state is {}", runtime.state)),
                None,
            )
            .await;
        return;
    }
    if let Err(error) = shared
        .kernel
        .transition_runtime(entry.runtime_id, RuntimeState::Running)
        .await
    {
        let _ = shared
            .store
            .set_state(
                entry.runtime_id,
                &[SchedulerState::Running],
                SchedulerState::Failed,
                Some(error.to_string()),
                None,
            )
            .await;
        return;
    }
    let (signal_tx, mut signal_rx) = watch::channel(WorkerSignal::None);
    shared
        .active
        .write()
        .await
        .insert(entry.runtime_id, signal_tx);
    let mut cancellation = match shared.kernel.cancellation_receiver(entry.runtime_id).await {
        Ok(receiver) => receiver,
        Err(error) => {
            tracing::error!(%error, "missing runtime cancellation receiver");
            return;
        }
    };
    let execution = shared
        .executor
        .execute(runtime.clone(), cancellation.clone());
    let timeout = Duration::from_millis(entry.timeout_ms.max(1));
    let outcome = tokio::select! {
        result = tokio::time::timeout(timeout, execution) => match result {
            Ok(result) => WorkerOutcome::Execution(result),
            Err(_) => WorkerOutcome::Timeout,
        },
        changed = signal_rx.changed() => match *signal_rx.borrow() {
            WorkerSignal::Wait => WorkerOutcome::Wait,
            WorkerSignal::Cancel | WorkerSignal::None => { let _ = changed; WorkerOutcome::Cancelled },
        },
        changed = cancellation.changed() => { let _ = changed; WorkerOutcome::Cancelled }
        changed = shutdown.changed() => { let _ = changed; WorkerOutcome::Cancelled }
    };
    shared.active.write().await.remove(&entry.runtime_id);
    match outcome {
        WorkerOutcome::Execution(Ok(result))
            if result.runtime_id == entry.runtime_id
                && result.status == RuntimeOutcome::Completed =>
        {
            match shared
                .kernel
                .complete_runtime(entry.runtime_id, result)
                .await
            {
                Ok(_) => set_terminal(&shared, &entry, SchedulerState::Completed, None).await,
                Err(error) => tracing::error!(%error, "runtime completion persistence failed"),
            }
        }
        WorkerOutcome::Execution(Ok(result)) if result.status == RuntimeOutcome::Cancelled => {
            let _ = cancel_kernel_runtime(&shared.kernel, entry.runtime_id).await;
            set_terminal(
                &shared,
                &entry,
                SchedulerState::Cancelled,
                Some(result.summary),
            )
            .await;
        }
        WorkerOutcome::Execution(Ok(result)) if result.status == RuntimeOutcome::Failed => {
            fail_or_retry(shared.clone(), &entry, result.summary, false).await;
        }
        WorkerOutcome::Execution(Ok(_)) => {
            fail_or_retry(
                shared.clone(),
                &entry,
                "executor returned a result for another runtime".into(),
                false,
            )
            .await;
        }
        WorkerOutcome::Execution(Err(error)) => {
            fail_or_retry(shared.clone(), &entry, error.message, error.retryable).await
        }
        WorkerOutcome::Timeout => {
            let _ = timeout_kernel_runtime(&shared.kernel, entry.runtime_id).await;
            set_terminal(
                &shared,
                &entry,
                SchedulerState::TimedOut,
                Some("runtime execution timed out".into()),
            )
            .await;
        }
        WorkerOutcome::Cancelled => {
            let _ = cancel_kernel_runtime(&shared.kernel, entry.runtime_id).await;
            set_terminal(&shared, &entry, SchedulerState::Cancelled, None).await;
        }
        WorkerOutcome::Wait => {
            let _ = shared
                .kernel
                .transition_runtime(entry.runtime_id, RuntimeState::Waiting)
                .await;
            let _ = shared
                .store
                .set_state(
                    entry.runtime_id,
                    &[SchedulerState::Running],
                    SchedulerState::Waiting,
                    None,
                    None,
                )
                .await;
            if let Ok(Some(waiting)) = shared.store.get(entry.runtime_id).await {
                if let Err(error) = publish_state(&shared.kernel, &waiting, None).await {
                    tracing::error!(%error, runtime_id = %waiting.runtime_id, "scheduler event append failed");
                }
            }
        }
    }
    let _ = refresh_dependencies(&shared).await;
    wake_parent(&shared, entry.parent_id).await;
    shared.notify.notify_one();
    shared.changed.notify_one();
}

enum WorkerOutcome {
    Execution(std::result::Result<RuntimeResult, RuntimeExecutionError>),
    Timeout,
    Cancelled,
    Wait,
}

async fn set_terminal(
    shared: &SchedulerShared,
    old: &ScheduledRuntime,
    state: SchedulerState,
    error: Option<String>,
) {
    let _ = shared
        .store
        .set_state(
            old.runtime_id,
            &[SchedulerState::Running],
            state,
            error.clone(),
            None,
        )
        .await;
    if let Ok(Some(entry)) = shared.store.get(old.runtime_id).await {
        if let Err(error) = publish_state(&shared.kernel, &entry, error).await {
            tracing::error!(%error, runtime_id = %entry.runtime_id, "scheduler event append failed");
        }
    }
}

async fn fail_or_retry(
    shared: Arc<SchedulerShared>,
    entry: &ScheduledRuntime,
    message: String,
    retryable: bool,
) {
    if retryable && entry.attempt <= entry.max_retries {
        let exponent = entry.attempt.saturating_sub(1).min(20);
        let multiplier = 1u32 << exponent;
        let delay = Duration::from_millis(entry.retry_delay_ms)
            .saturating_mul(multiplier)
            .min(shared.config.max_retry_backoff);
        let not_before =
            now_millis().saturating_add(delay.as_millis().min(i64::MAX as u128) as i64);
        let _ = shared
            .kernel
            .transition_runtime(entry.runtime_id, RuntimeState::Waiting)
            .await;
        if shared
            .store
            .set_state(
                entry.runtime_id,
                &[SchedulerState::Running],
                SchedulerState::Queued,
                Some(message.clone()),
                Some(not_before),
            )
            .await
            .unwrap_or(false)
        {
            if let Ok(Some(updated)) = shared.store.get(entry.runtime_id).await {
                if let Err(error) = publish_state(
                    &shared.kernel,
                    &updated,
                    Some(format!("retry after {delay:?}: {message}")),
                )
                .await
                {
                    tracing::error!(%error, runtime_id = %updated.runtime_id, "scheduler event append failed");
                }
            }
        }
    } else {
        let _ = shared
            .kernel
            .fail_runtime(entry.runtime_id, message.clone())
            .await;
        set_terminal(
            shared.as_ref(),
            entry,
            SchedulerState::Failed,
            Some(message),
        )
        .await;
    }
}

async fn refresh_dependencies(shared: &SchedulerShared) -> Result<()> {
    for entry in shared.store.list().await? {
        if entry.state != SchedulerState::Queued
            || entry.not_before.is_some_and(|time| time > now_millis())
        {
            continue;
        }
        let mut complete = true;
        let mut failed = None;
        for dependency in &entry.dependencies {
            match shared.store.get(*dependency).await? {
                Some(dep) if dep.state == SchedulerState::Completed => {}
                Some(dep)
                    if matches!(
                        dep.state,
                        SchedulerState::Failed
                            | SchedulerState::Cancelled
                            | SchedulerState::TimedOut
                            | SchedulerState::Blocked
                            | SchedulerState::Interrupted
                    ) =>
                {
                    failed = Some(format!("dependency {dependency} ended in {:?}", dep.state))
                }
                None if matches!(
                    shared.kernel.get_runtime(*dependency).await?.state,
                    RuntimeState::Completed
                ) => {}
                None if matches!(
                    shared.kernel.get_runtime(*dependency).await?.state,
                    RuntimeState::Failed
                        | RuntimeState::Cancelled
                        | RuntimeState::TimedOut
                        | RuntimeState::Interrupted
                ) =>
                {
                    failed = Some(format!("dependency {dependency} ended unsuccessfully"));
                }
                _ => complete = false,
            }
        }
        if let Some(error) = failed {
            if shared
                .store
                .set_state(
                    entry.runtime_id,
                    &[SchedulerState::Queued],
                    SchedulerState::Blocked,
                    Some(error.clone()),
                    None,
                )
                .await?
            {
                if let Ok(Some(updated)) = shared.store.get(entry.runtime_id).await {
                    publish_state(&shared.kernel, &updated, Some(error)).await?;
                }
            }
        } else if complete
            && shared
                .store
                .set_state(
                    entry.runtime_id,
                    &[SchedulerState::Queued],
                    SchedulerState::Runnable,
                    None,
                    None,
                )
                .await?
        {
            if let Ok(Some(updated)) = shared.store.get(entry.runtime_id).await {
                publish_state(&shared.kernel, &updated, None).await?;
            }
        }
    }
    Ok(())
}

async fn wake_parent(shared: &SchedulerShared, parent: Option<RuntimeId>) {
    if let Some(parent_id) = parent {
        if !children_terminal(shared, parent_id).await.unwrap_or(false) {
            return;
        }
        if let Ok(Some(parent_entry)) = shared.store.get(parent_id).await {
            if parent_entry.state == SchedulerState::Waiting {
                let _ = shared
                    .store
                    .set_state(
                        parent_id,
                        &[SchedulerState::Waiting],
                        SchedulerState::Runnable,
                        None,
                        None,
                    )
                    .await;
                if let Ok(Some(updated)) = shared.store.get(parent_id).await {
                    if let Err(error) = publish_state(&shared.kernel, &updated, None).await {
                        tracing::error!(%error, runtime_id = %updated.runtime_id, "scheduler event append failed");
                    }
                }
            }
        }
    }
}

async fn children_terminal(shared: &SchedulerShared, parent_id: RuntimeId) -> Result<bool> {
    let children = shared.kernel.get_runtime(parent_id).await?.children;
    for id in children {
        if let Some(entry) = shared.store.get(id).await? {
            if !is_terminal(entry.state) {
                return Ok(false);
            }
        } else if !matches!(
            shared.kernel.get_runtime(id).await?.state,
            RuntimeState::Completed
                | RuntimeState::Failed
                | RuntimeState::Cancelled
                | RuntimeState::TimedOut
                | RuntimeState::Interrupted
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn cancel_kernel_runtime(kernel: &Kernel, runtime_id: RuntimeId) -> Result<()> {
    let runtime = kernel.get_runtime(runtime_id).await?;
    if runtime.state == RuntimeState::Cancelled {
        return Ok(());
    }
    kernel.cancel_runtime(runtime_id).await?;
    Ok(())
}

async fn timeout_kernel_runtime(kernel: &Kernel, runtime_id: RuntimeId) -> Result<()> {
    kernel.timeout_runtime(runtime_id).await.map(|_| ())
}

async fn publish_state(
    kernel: &Kernel,
    entry: &ScheduledRuntime,
    detail: Option<String>,
) -> Result<()> {
    kernel
        .events()
        .publish(RuntimeEvent::SchedulerStateChanged {
            mission_id: entry.mission_id,
            runtime_id: entry.runtime_id,
            parent_id: entry.parent_id,
            state: entry.state,
            attempt: entry.attempt,
            detail,
            timestamp: now_millis(),
        })
        .await
}

async fn recover_running(shared: &SchedulerShared) -> Result<()> {
    for entry in shared.store.list().await? {
        if entry.state == SchedulerState::Running {
            shared
                .store
                .set_state(
                    entry.runtime_id,
                    &[SchedulerState::Running],
                    SchedulerState::Interrupted,
                    Some("process ended before execution was recorded complete".into()),
                    None,
                )
                .await?;
            shared
                .kernel
                .mark_runtime_interrupted(entry.runtime_id)
                .await?;
            if let Ok(Some(updated)) = shared.store.get(entry.runtime_id).await {
                publish_state(
                    &shared.kernel,
                    &updated,
                    Some("recovered as interrupted".into()),
                )
                .await?;
            }
        }
    }
    Ok(())
}
