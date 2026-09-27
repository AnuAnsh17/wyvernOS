use crate::domain::{MemoryId, MissionId, RuntimeId};
use crate::error::{Result, WyvernError};
use crate::kernel::Kernel;
use crate::memory::MemoryScope;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "wyvern", version, about = "Wyvern orchestration kernel")]
pub struct Cli {
    #[arg(long, global = true, default_value = "wyvern.sqlite")]
    pub database: PathBuf,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Demo {
        #[command(subcommand)]
        command: DemoCommands,
    },
    Mission {
        #[command(subcommand)]
        command: MissionCommands,
    },
    Runtime {
        #[command(subcommand)]
        command: RuntimeCommands,
    },
    Context {
        #[command(subcommand)]
        command: ContextCommands,
    },
    Memory {
        #[command(subcommand)]
        command: MemoryCommands,
    },
}

#[derive(Subcommand)]
pub enum DemoCommands {
    Recursive,
}

#[derive(Subcommand)]
pub enum MissionCommands {
    Create { objective: String },
    List,
    Status { mission_id: String },
}

#[derive(Subcommand)]
pub enum RuntimeCommands {
    List,
    Inspect { runtime_id: String },
}

#[derive(Subcommand)]
pub enum ContextCommands {
    Inspect { runtime_id: String },
    History { runtime_id: String },
    Snapshot { runtime_id: String },
}

#[derive(Subcommand)]
pub enum MemoryCommands {
    Inspect {
        memory_id: String,
    },
    List {
        mission_id: String,
        scope: String,
        owner_id: Option<String>,
    },
    History {
        memory_id: String,
    },
    Promote {
        memory_id: String,
        target_runtime: String,
    },
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let kernel = Kernel::new_sqlite(&cli.database).await?;

    match cli.command {
        Commands::Demo {
            command: DemoCommands::Recursive,
        } => {
            let result = kernel.demo_recursive().await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&result).unwrap_or_else(|_| result.summary.clone())
            );
        }
        Commands::Mission { command } => match command {
            MissionCommands::Create { objective } => {
                let mission = kernel.create_mission(objective).await?;
                println!("{} {}", mission.id, mission.objective);
            }
            MissionCommands::List => {
                for mission in kernel.persisted_missions().await? {
                    println!(
                        "{} | {} | {}",
                        mission.mission_id, mission.state, mission.objective
                    );
                }
            }
            MissionCommands::Status { mission_id } => {
                let mission_id = mission_id.parse::<MissionId>()?;
                let mission = kernel
                    .persisted_mission(mission_id)
                    .await?
                    .ok_or(WyvernError::MissionNotFound(mission_id))?;
                println!(
                    "{} | {} | {}",
                    mission.mission_id, mission.state, mission.objective
                );
            }
        },
        Commands::Runtime { command } => match command {
            RuntimeCommands::List => {
                for runtime in kernel.persisted_runtimes().await? {
                    println!(
                        "{} | {} | {} | {} | {}",
                        runtime.runtime_id,
                        runtime.state,
                        format!("mission_{}", runtime.mission_id),
                        runtime
                            .parent_id
                            .map(|id| format!("rt_{}", id))
                            .unwrap_or_else(|| "-".to_string()),
                        runtime.objective,
                    );
                }
            }
            RuntimeCommands::Inspect { runtime_id } => {
                let runtime_id = runtime_id.parse::<RuntimeId>()?;
                let runtime = kernel
                    .persisted_runtime(runtime_id)
                    .await?
                    .ok_or(WyvernError::RuntimeNotFound(runtime_id))?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&runtime)
                        .unwrap_or_else(|_| format!("rt_{}", runtime.runtime_id))
                );
            }
        },
        Commands::Context { command } => match command {
            ContextCommands::Inspect { runtime_id } => {
                let runtime_id = runtime_id.parse::<RuntimeId>()?;
                let context = kernel.context(runtime_id).await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&context).unwrap_or_default()
                );
            }
            ContextCommands::History { runtime_id } => {
                let runtime_id = runtime_id.parse::<RuntimeId>()?;
                let snapshots = kernel.contexts().snapshot_history(runtime_id).await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&snapshots).unwrap_or_default()
                );
            }
            ContextCommands::Snapshot { runtime_id } => {
                let runtime_id = runtime_id.parse::<RuntimeId>()?;
                let snapshot = kernel.contexts().create_snapshot(runtime_id, None).await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&snapshot).unwrap_or_default()
                );
            }
        },
        Commands::Memory { command } => match command {
            MemoryCommands::Inspect { memory_id } => {
                let memory_id = memory_id.parse::<MemoryId>()?;
                let memory = kernel
                    .memories()
                    .get(memory_id)
                    .await?
                    .ok_or(WyvernError::MemoryNotFound(memory_id))?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&memory).unwrap_or_default()
                );
            }
            MemoryCommands::List {
                mission_id,
                scope,
                owner_id,
            } => {
                let mission_id = mission_id.parse::<MissionId>()?;
                let scope = scope.parse::<MemoryScope>()?;
                let items = kernel
                    .memories()
                    .list_scope(mission_id, scope, owner_id.as_deref())
                    .await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&items).unwrap_or_default()
                );
            }
            MemoryCommands::History { memory_id } => {
                let memory_id = memory_id.parse::<MemoryId>()?;
                let history = kernel.memories().history(memory_id).await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&history).unwrap_or_default()
                );
            }
            MemoryCommands::Promote {
                memory_id,
                target_runtime,
            } => {
                let memory_id = memory_id.parse::<MemoryId>()?;
                let target_runtime = target_runtime.parse::<RuntimeId>()?;
                let memory = kernel.promote_memory(memory_id, target_runtime).await?;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&memory).unwrap_or_default()
                );
            }
        },
    }

    Ok(())
}

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

impl From<WyvernError> for std::process::ExitCode {
    fn from(err: WyvernError) -> Self {
        eprintln!("{err}");
        std::process::ExitCode::from(1)
    }
}
