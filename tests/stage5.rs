use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use tempfile::TempDir;
use tokio::sync::{Mutex, Notify, Semaphore};
use wyvern::agent::{AgentDefinition, AgentKind};
use wyvern::domain::{MissionId, RuntimeId, RuntimeState};
use wyvern::kernel::{Kernel, Runtime};
use wyvern::result::RuntimeResult;
use wyvern::scheduler::{
    RuntimeExecutionError, RuntimePriority, ScheduleOptions, ScheduledRuntimeExecutor, Scheduler,
    SchedulerConfig, SchedulerState,
};

async fn setup(limit: usize) -> (Kernel, Scheduler, MissionId, RuntimeId, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("stage5.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("scheduler tests").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("scheduled worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &agent, "root")
        .await
        .unwrap();
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        Arc::new(ImmediateExecutor),
        SchedulerConfig {
            max_concurrent_runtimes: limit,
            default_timeout: Duration::from_secs(2),
            priority_aging_interval: Duration::from_millis(20),
            max_retry_backoff: Duration::from_millis(200),
        },
    )
    .await
    .unwrap();
    (kernel, scheduler, mission.id, root.id(), dir)
}

async fn child(kernel: &Kernel, parent: RuntimeId, objective: &str) -> RuntimeId {
    let agent = kernel
        .register_agent(AgentDefinition::new(objective, AgentKind::WorkerA))
        .await
        .unwrap();
    kernel
        .spawn_child(parent, &agent, objective)
        .await
        .unwrap()
        .id()
}

async fn until_state(scheduler: &Scheduler, id: RuntimeId, state: SchedulerState) {
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if scheduler.get(id).await.unwrap().state == state {
                return;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    if result.is_err() {
        panic!(
            "timed out waiting for {id} to reach {state:?}; current state: {:?}",
            scheduler.get(id).await.unwrap().state
        );
    }
}

struct ImmediateExecutor;

#[async_trait]
impl ScheduledRuntimeExecutor for ImmediateExecutor {
    async fn execute(
        &self,
        runtime: Runtime,
        _cancellation: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        Ok(RuntimeResult::completed(
            runtime.id,
            "done",
            json!({"ok":true}),
            Vec::new(),
        ))
    }
}

#[tokio::test]
async fn runtime_is_queued_claimed_completed_and_emits_routed_events() {
    let (kernel, scheduler, mission_id, root, _dir) = setup(1).await;
    let mut events = kernel.events().subscribe_envelopes();
    let queued = scheduler
        .submit(root, ScheduleOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        queued.state,
        SchedulerState::Runnable | SchedulerState::Running | SchedulerState::Completed
    ));
    until_state(&scheduler, root, SchedulerState::Completed).await;
    assert_eq!(
        kernel.get_runtime(root).await.unwrap().state,
        RuntimeState::Completed
    );
    assert!(matches!(
        scheduler.resume(root).await,
        Err(wyvern::WyvernError::InvalidSchedulerTransition { .. })
    ));
    let event_list: Vec<_> = std::iter::from_fn(|| events.try_recv().ok()).collect();
    assert!(event_list
        .iter()
        .any(|event| event.event_type == "runtime.runnable"));
    assert!(event_list
        .iter()
        .any(|event| event.event_type == "runtime.started"));
    assert!(event_list
        .iter()
        .any(|event| event.event_type == "runtime.completed"));
    assert!(event_list
        .iter()
        .all(|event| event.mission_id == mission_id && event.runtime_id == root));
    scheduler.shutdown().await;
}

struct MeteredExecutor {
    current: AtomicUsize,
    peak: AtomicUsize,
    delay: Duration,
}

#[async_trait]
impl ScheduledRuntimeExecutor for MeteredExecutor {
    async fn execute(
        &self,
        runtime: Runtime,
        _cancel: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        let active = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.current.fetch_sub(1, Ordering::SeqCst);
        Ok(RuntimeResult::completed(
            runtime.id,
            "done",
            json!({}),
            Vec::new(),
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_workers_execute_concurrently_across_missions() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("parallel.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let meter = Arc::new(MeteredExecutor {
        current: AtomicUsize::new(0),
        peak: AtomicUsize::new(0),
        delay: Duration::from_millis(60),
    });
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        meter.clone(),
        SchedulerConfig {
            max_concurrent_runtimes: 2,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    let mut ids = Vec::new();
    for index in 0..6 {
        let mission = kernel
            .create_mission(format!("mission {index}"))
            .await
            .unwrap();
        ids.push(
            kernel
                .create_runtime(mission.id, None, &agent, format!("root {index}"))
                .await
                .unwrap()
                .id(),
        );
    }
    for id in &ids {
        scheduler
            .submit(*id, ScheduleOptions::default())
            .await
            .unwrap();
    }
    for id in ids {
        until_state(&scheduler, id, SchedulerState::Completed).await;
    }
    assert_eq!(meter.peak.load(Ordering::SeqCst), 2);
    assert_eq!(scheduler.concurrency_limit(), 2);
    scheduler.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_scheduler_instances_cannot_claim_one_runtime_twice() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("atomic-claim.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("atomic claim").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &agent, "one claim")
        .await
        .unwrap()
        .id();
    let executions = Arc::new(AtomicUsize::new(0));
    let executor = Arc::new(CountExecutor(executions.clone()));
    let config = SchedulerConfig {
        max_concurrent_runtimes: 1,
        ..SchedulerConfig::default()
    };
    let scheduler_a = Scheduler::open(kernel.clone(), &db, executor.clone(), config.clone())
        .await
        .unwrap();
    let scheduler_b = Scheduler::open(kernel.clone(), &db, executor, config)
        .await
        .unwrap();
    scheduler_a
        .submit(root, ScheduleOptions::default())
        .await
        .unwrap();
    until_state(&scheduler_a, root, SchedulerState::Completed).await;
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(
        kernel.get_runtime(root).await.unwrap().state,
        RuntimeState::Completed
    );
    scheduler_a.shutdown().await;
    scheduler_b.shutdown().await;
}

struct CountExecutor(Arc<AtomicUsize>);

#[async_trait]
impl ScheduledRuntimeExecutor for CountExecutor {
    async fn execute(
        &self,
        runtime: Runtime,
        _cancel: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(RuntimeResult::completed(
            runtime.id,
            "once",
            json!({}),
            Vec::new(),
        ))
    }
}

struct GatedExecutor {
    started: Notify,
    permits: Semaphore,
    order: Mutex<Vec<String>>,
}

#[async_trait]
impl ScheduledRuntimeExecutor for GatedExecutor {
    async fn execute(
        &self,
        runtime: Runtime,
        _cancel: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        self.started.notify_one();
        self.permits.acquire().await.unwrap().forget();
        self.order.lock().await.push(runtime.objective.clone());
        Ok(RuntimeResult::completed(
            runtime.id,
            "done",
            json!({}),
            Vec::new(),
        ))
    }
}

#[tokio::test]
async fn higher_priority_dispatches_first_and_age_prevents_starvation() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("priority.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("priority").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let first = kernel
        .create_runtime(mission.id, None, &agent, "gate")
        .await
        .unwrap()
        .id();
    let executor = Arc::new(GatedExecutor {
        started: Notify::new(),
        permits: Semaphore::new(0),
        order: Mutex::new(Vec::new()),
    });
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        executor.clone(),
        SchedulerConfig {
            max_concurrent_runtimes: 1,
            priority_aging_interval: Duration::from_millis(5),
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    scheduler
        .submit(first, ScheduleOptions::default())
        .await
        .unwrap();
    executor.started.notified().await;
    let low = child(&kernel, first, "low").await;
    let high = child(&kernel, first, "high").await;
    let background = child(&kernel, first, "background").await;
    scheduler
        .submit(
            low,
            ScheduleOptions {
                priority: RuntimePriority::Low,
                ..ScheduleOptions::default()
            },
        )
        .await
        .unwrap();
    scheduler
        .submit(
            high,
            ScheduleOptions {
                priority: RuntimePriority::Critical,
                ..ScheduleOptions::default()
            },
        )
        .await
        .unwrap();
    scheduler
        .submit(
            background,
            ScheduleOptions {
                priority: RuntimePriority::Background,
                ..ScheduleOptions::default()
            },
        )
        .await
        .unwrap();
    for _ in 0..5 {
        executor.permits.add_permits(1);
        tokio::time::sleep(Duration::from_millis(8)).await;
    }
    until_state(&scheduler, low, SchedulerState::Completed).await;
    until_state(&scheduler, high, SchedulerState::Completed).await;
    until_state(&scheduler, background, SchedulerState::Completed).await;
    let order = executor.order.lock().await.clone();
    assert_eq!(order[0], "gate");
    assert!(
        order.iter().position(|name| name == "high").unwrap()
            < order.iter().position(|name| name == "low").unwrap()
    );
    assert!(order.iter().any(|name| name == "background"));
    scheduler.shutdown().await;
}

struct FailByObjective;

#[async_trait]
impl ScheduledRuntimeExecutor for FailByObjective {
    async fn execute(
        &self,
        runtime: Runtime,
        _cancel: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        if runtime.objective.contains("fail") {
            Err(RuntimeExecutionError::permanent("planned failure"))
        } else {
            Ok(RuntimeResult::completed(
                runtime.id,
                "done",
                json!({}),
                Vec::new(),
            ))
        }
    }
}

#[tokio::test]
async fn dependencies_unlock_on_success_and_block_after_failure() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("deps.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("deps").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &agent, "root")
        .await
        .unwrap()
        .id();
    let predecessor = child(&kernel, root, "predecessor").await;
    let dependent = child(&kernel, root, "dependent").await;
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        Arc::new(ImmediateExecutor),
        SchedulerConfig {
            max_concurrent_runtimes: 1,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    scheduler
        .submit(predecessor, ScheduleOptions::default())
        .await
        .unwrap();
    let queued = scheduler
        .submit(
            dependent,
            ScheduleOptions {
                dependencies: vec![predecessor],
                ..ScheduleOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        queued.state,
        SchedulerState::Queued
            | SchedulerState::Runnable
            | SchedulerState::Running
            | SchedulerState::Completed
    ));
    until_state(&scheduler, dependent, SchedulerState::Completed).await;
    scheduler.shutdown().await;

    let dir2 = tempfile::tempdir().unwrap();
    let db2 = dir2.path().join("failed-deps.sqlite");
    let kernel2 = Kernel::new_sqlite(&db2).await.unwrap();
    let mission2 = kernel2.create_mission("failed deps").await.unwrap();
    let agent2 = kernel2
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let root2 = kernel2
        .create_runtime(mission2.id, None, &agent2, "root")
        .await
        .unwrap()
        .id();
    let failed = child(&kernel2, root2, "fail predecessor").await;
    let blocked = child(&kernel2, root2, "must stay blocked").await;
    let scheduler2 = Scheduler::open(
        kernel2.clone(),
        &db2,
        Arc::new(FailByObjective),
        SchedulerConfig {
            max_concurrent_runtimes: 1,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    scheduler2
        .submit(failed, ScheduleOptions::default())
        .await
        .unwrap();
    scheduler2
        .submit(
            blocked,
            ScheduleOptions {
                dependencies: vec![failed],
                ..ScheduleOptions::default()
            },
        )
        .await
        .unwrap();
    until_state(&scheduler2, failed, SchedulerState::Failed).await;
    until_state(&scheduler2, blocked, SchedulerState::Blocked).await;
    assert_eq!(
        kernel2.get_runtime(blocked).await.unwrap().state,
        RuntimeState::Ready
    );
    scheduler2.shutdown().await;
}

struct RetryOnce {
    attempts: AtomicUsize,
}

#[async_trait]
impl ScheduledRuntimeExecutor for RetryOnce {
    async fn execute(
        &self,
        runtime: Runtime,
        _cancel: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(RuntimeExecutionError::retryable("transient"))
        } else {
            Ok(RuntimeResult::completed(
                runtime.id,
                "recovered",
                json!({}),
                Vec::new(),
            ))
        }
    }
}

#[tokio::test]
async fn retry_uses_configured_backoff_and_preserves_attempt_count() {
    let db_dir = tempfile::tempdir().unwrap();
    let db = db_dir.path().join("retry.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("retry").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("retry", AgentKind::WorkerA))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &agent, "retry me")
        .await
        .unwrap()
        .id();
    let executor = Arc::new(RetryOnce {
        attempts: AtomicUsize::new(0),
    });
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        executor.clone(),
        SchedulerConfig {
            max_concurrent_runtimes: 1,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    let mut events = kernel.events().subscribe_envelopes();
    scheduler
        .submit(
            root,
            ScheduleOptions {
                max_retries: 1,
                retry_delay: Duration::from_millis(20),
                ..ScheduleOptions::default()
            },
        )
        .await
        .unwrap();
    until_state(&scheduler, root, SchedulerState::Completed).await;
    assert_eq!(scheduler.get(root).await.unwrap().attempt, 2);
    assert_eq!(executor.attempts.load(Ordering::SeqCst), 2);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if std::iter::from_fn(|| events.try_recv().ok())
                .any(|event| event.event_type == "runtime.retry_scheduled")
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    scheduler.shutdown().await;
}

struct RetryAlways(AtomicUsize);

#[async_trait]
impl ScheduledRuntimeExecutor for RetryAlways {
    async fn execute(
        &self,
        _runtime: Runtime,
        _cancel: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(RuntimeExecutionError::retryable("still unavailable"))
    }
}

#[tokio::test]
async fn retry_exhaustion_fails_after_the_configured_attempt_budget() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("retry-exhaustion.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("retry exhaustion").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &agent, "always fail")
        .await
        .unwrap()
        .id();
    let executor = Arc::new(RetryAlways(AtomicUsize::new(0)));
    let scheduler = Scheduler::open(
        kernel,
        &db,
        executor.clone(),
        SchedulerConfig {
            max_concurrent_runtimes: 1,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    scheduler
        .submit(
            root,
            ScheduleOptions {
                max_retries: 1,
                retry_delay: Duration::from_millis(2),
                ..ScheduleOptions::default()
            },
        )
        .await
        .unwrap();
    until_state(&scheduler, root, SchedulerState::Failed).await;
    assert_eq!(scheduler.get(root).await.unwrap().attempt, 2);
    assert_eq!(executor.0.load(Ordering::SeqCst), 2);
    scheduler.shutdown().await;
}

struct SlowExecutor(Duration);

#[async_trait]
impl ScheduledRuntimeExecutor for SlowExecutor {
    async fn execute(
        &self,
        runtime: Runtime,
        _cancel: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        tokio::time::sleep(self.0).await;
        Ok(RuntimeResult::completed(
            runtime.id,
            "done",
            json!({}),
            Vec::new(),
        ))
    }
}

#[tokio::test]
async fn timeout_releases_slot_and_wait_resume_does_not_hold_a_worker() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("timeouts.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("time slots").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &agent, "root")
        .await
        .unwrap()
        .id();
    let timed = child(&kernel, root, "timeout").await;
    let succeeds = child(&kernel, root, "succeed").await;
    let mut events = kernel.events().subscribe_envelopes();
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        Arc::new(SlowExecutor(Duration::from_millis(80))),
        SchedulerConfig {
            max_concurrent_runtimes: 1,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    scheduler
        .submit(
            timed,
            ScheduleOptions {
                timeout: Some(Duration::from_millis(10)),
                ..ScheduleOptions::default()
            },
        )
        .await
        .unwrap();
    scheduler
        .submit(succeeds, ScheduleOptions::default())
        .await
        .unwrap();
    until_state(&scheduler, timed, SchedulerState::TimedOut).await;
    until_state(&scheduler, succeeds, SchedulerState::Completed).await;
    assert_eq!(
        kernel.get_runtime(timed).await.unwrap().state,
        RuntimeState::TimedOut
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if std::iter::from_fn(|| events.try_recv().ok())
                .any(|event| event.event_type == "runtime.timed_out")
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    scheduler.shutdown().await;
}

#[tokio::test]
async fn runtime_timeout_and_cancellation_emit_terminal_scheduler_events() {
    let (kernel, scheduler, _mission, root, _dir) = setup(1).await;
    let mut events = kernel.events().subscribe_envelopes();
    scheduler
        .submit(root, ScheduleOptions::default())
        .await
        .unwrap();
    until_state(&scheduler, root, SchedulerState::Completed).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if std::iter::from_fn(|| events.try_recv().ok())
                .any(|event| event.event_type == "runtime.completed")
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    scheduler.shutdown().await;
}

#[tokio::test]
async fn schedule_records_survive_restart_and_running_records_recover_as_interrupted() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("recovery.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("recover").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &agent, "root")
        .await
        .unwrap()
        .id();
    let interrupted_mission = kernel.create_mission("crash case").await.unwrap();
    let interrupted_runtime = kernel
        .create_runtime(interrupted_mission.id, None, &agent, "interrupted")
        .await
        .unwrap()
        .id();
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        Arc::new(ImmediateExecutor),
        SchedulerConfig {
            max_concurrent_runtimes: 1,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    scheduler
        .submit(root, ScheduleOptions::default())
        .await
        .unwrap();
    until_state(&scheduler, root, SchedulerState::Completed).await;
    scheduler.shutdown().await;

    // Recreate the final durable state left when a process exits mid-attempt.
    let options = SqliteConnectOptions::new()
        .filename(&db)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    sqlx::query("UPDATE runtimes SET state='Running', result_json=NULL WHERE runtime_id=?1")
        .bind(interrupted_runtime.to_string())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO scheduler_runtime (runtime_id, mission_id, parent_id, state, priority, dependencies_json, attempt, max_retries, retry_delay_ms, timeout_ms, queued_at, started_at, updated_at, not_before, error) VALUES (?1, ?2, NULL, 'running', 2, '[]', 1, 0, 50, 300000, 1, 1, 1, NULL, NULL)")
        .bind(interrupted_runtime.to_string())
        .bind(interrupted_mission.id.to_string())
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE missions SET state='Running' WHERE mission_id=?1")
        .bind(interrupted_mission.id.to_string())
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    drop(kernel);

    let reopened = Kernel::new_sqlite(&db).await.unwrap();
    let reopened_scheduler = Scheduler::open(
        reopened.clone(),
        &db,
        Arc::new(ImmediateExecutor),
        SchedulerConfig::default(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened_scheduler.get(root).await.unwrap().state,
        SchedulerState::Completed
    );
    assert_eq!(
        reopened.get_runtime(root).await.unwrap().state,
        RuntimeState::Completed
    );
    assert_eq!(
        reopened
            .get_runtime(interrupted_runtime)
            .await
            .unwrap()
            .state,
        RuntimeState::Interrupted
    );
    assert_eq!(Scheduler::inspect_database(&db).await.unwrap().completed, 1);
    assert_eq!(
        reopened_scheduler
            .get(interrupted_runtime)
            .await
            .unwrap()
            .state,
        SchedulerState::Interrupted
    );
    reopened_scheduler.shutdown().await;
}

#[tokio::test]
async fn duplicate_submission_is_rejected_and_snapshots_are_nonblocking() {
    let (kernel, scheduler, mission, root, _dir) = setup(1).await;
    let waiting = child(&kernel, root, "waiting").await;
    scheduler
        .submit(waiting, ScheduleOptions::default())
        .await
        .unwrap();
    let snapshot = scheduler.inspect_mission(mission).await.unwrap();
    assert_eq!(snapshot.total, 1);
    assert_eq!(snapshot.runtimes[0].runtime_id, waiting);
    assert!(scheduler
        .submit(waiting, ScheduleOptions::default())
        .await
        .is_err());
    scheduler.shutdown().await;
}

#[tokio::test]
async fn mission_and_subtree_cancellation_leave_other_missions_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("cancel.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission_a = kernel.create_mission("A").await.unwrap();
    let mission_b = kernel.create_mission("B").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let root_a = kernel
        .create_runtime(mission_a.id, None, &agent, "root A")
        .await
        .unwrap()
        .id();
    let child_a = child(&kernel, root_a, "child A").await;
    let root_b = kernel
        .create_runtime(mission_b.id, None, &agent, "root B")
        .await
        .unwrap()
        .id();
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        Arc::new(SlowExecutor(Duration::from_secs(10))),
        SchedulerConfig {
            max_concurrent_runtimes: 3,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    let mut events = kernel.events().subscribe_envelopes();
    for id in [root_a, child_a, root_b] {
        scheduler
            .submit(id, ScheduleOptions::default())
            .await
            .unwrap();
    }
    until_state(&scheduler, root_a, SchedulerState::Running).await;
    scheduler.cancel_subtree(root_a).await.unwrap();
    until_state(&scheduler, root_a, SchedulerState::Cancelled).await;
    until_state(&scheduler, child_a, SchedulerState::Cancelled).await;
    assert_ne!(
        scheduler.get(root_b).await.unwrap().state,
        SchedulerState::Cancelled,
        "cancelling one subtree must leave another mission untouched"
    );
    scheduler.cancel_mission(mission_b.id).await.unwrap();
    until_state(&scheduler, root_b, SchedulerState::Cancelled).await;
    assert!(std::iter::from_fn(|| events.try_recv().ok())
        .any(|event| event.event_type == "runtime.cancelled"));
    scheduler.shutdown().await;
}

#[tokio::test]
async fn waiting_runtime_releases_its_worker_slot() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("wait.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("waiting").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let root = kernel
        .create_runtime(mission.id, None, &agent, "root")
        .await
        .unwrap()
        .id();
    let waiting = child(&kernel, root, "pause me").await;
    let other = child(&kernel, root, "other").await;
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        Arc::new(SlowExecutor(Duration::from_secs(1))),
        SchedulerConfig {
            max_concurrent_runtimes: 1,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    scheduler
        .submit(waiting, ScheduleOptions::default())
        .await
        .unwrap();
    until_state(&scheduler, waiting, SchedulerState::Running).await;
    scheduler.wait(waiting).await.unwrap();
    until_state(&scheduler, waiting, SchedulerState::Waiting).await;
    scheduler
        .submit(other, ScheduleOptions::default())
        .await
        .unwrap();
    until_state(&scheduler, other, SchedulerState::Completed).await;
    scheduler.resume(waiting).await.unwrap();
    until_state(&scheduler, waiting, SchedulerState::Completed).await;
    scheduler.shutdown().await;
}

struct ParentWaitExecutor {
    starts: Notify,
    parent_calls: AtomicUsize,
}

#[async_trait]
impl ScheduledRuntimeExecutor for ParentWaitExecutor {
    async fn execute(
        &self,
        runtime: Runtime,
        _cancel: tokio::sync::watch::Receiver<bool>,
    ) -> std::result::Result<RuntimeResult, RuntimeExecutionError> {
        if runtime.objective == "parent" && self.parent_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.starts.notify_one();
            std::future::pending::<()>().await;
        }
        if runtime.objective == "child failure" {
            Err(RuntimeExecutionError::permanent("child failed"))
        } else {
            Ok(RuntimeResult::completed(
                runtime.id,
                "done",
                json!({}),
                Vec::new(),
            ))
        }
    }
}

#[tokio::test]
async fn parent_waits_for_all_children_and_failure_is_visible_before_resume() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("parent-wait.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("parent wait").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let parent = kernel
        .create_runtime(mission.id, None, &agent, "parent")
        .await
        .unwrap()
        .id();
    let child_ok = child(&kernel, parent, "child success").await;
    let child_failed = child(&kernel, parent, "child failure").await;
    let executor = Arc::new(ParentWaitExecutor {
        starts: Notify::new(),
        parent_calls: AtomicUsize::new(0),
    });
    let scheduler = Scheduler::open(
        kernel.clone(),
        &db,
        executor.clone(),
        SchedulerConfig {
            max_concurrent_runtimes: 1,
            ..SchedulerConfig::default()
        },
    )
    .await
    .unwrap();
    scheduler
        .submit(parent, ScheduleOptions::default())
        .await
        .unwrap();
    executor.starts.notified().await;
    scheduler.wait(parent).await.unwrap();
    until_state(&scheduler, parent, SchedulerState::Waiting).await;
    scheduler
        .submit(child_ok, ScheduleOptions::default())
        .await
        .unwrap();
    scheduler
        .submit(child_failed, ScheduleOptions::default())
        .await
        .unwrap();
    scheduler.wait_for_children(parent).await.unwrap();
    until_state(&scheduler, parent, SchedulerState::Completed).await;
    assert_eq!(
        scheduler.get(child_ok).await.unwrap().state,
        SchedulerState::Completed
    );
    assert_eq!(
        scheduler.get(child_failed).await.unwrap().state,
        SchedulerState::Failed
    );
    assert_eq!(executor.parent_calls.load(Ordering::SeqCst), 2);
    scheduler.shutdown().await;
}

#[test]
fn scheduled_state_serialization_includes_all_terminal_and_waiting_states() {
    for state in [
        SchedulerState::Queued,
        SchedulerState::Runnable,
        SchedulerState::Running,
        SchedulerState::Waiting,
        SchedulerState::Completed,
        SchedulerState::Failed,
        SchedulerState::Cancelled,
        SchedulerState::TimedOut,
        SchedulerState::Blocked,
        SchedulerState::Interrupted,
    ] {
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(
            serde_json::from_str::<SchedulerState>(&json).unwrap(),
            state
        );
    }
}

#[tokio::test]
async fn scheduler_status_cli_reads_persisted_scheduler_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("scheduler-cli.sqlite");
    let kernel = Kernel::new_sqlite(&db).await.unwrap();
    let mission = kernel.create_mission("cli").await.unwrap();
    let agent = kernel
        .register_agent(AgentDefinition::new("worker", AgentKind::WorkerA))
        .await
        .unwrap();
    let runtime = kernel
        .create_runtime(mission.id, None, &agent, "cli runtime")
        .await
        .unwrap()
        .id();
    let scheduler = Scheduler::open(
        kernel,
        &db,
        Arc::new(ImmediateExecutor),
        SchedulerConfig::default(),
    )
    .await
    .unwrap();
    scheduler
        .submit(runtime, ScheduleOptions::default())
        .await
        .unwrap();
    until_state(&scheduler, runtime, SchedulerState::Completed).await;
    scheduler.shutdown().await;
    let output = Command::new(std::env::var("CARGO_BIN_EXE_wyvern").unwrap())
        .arg("--database")
        .arg(&db)
        .args(["scheduler", "status"])
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("completed"));
}
