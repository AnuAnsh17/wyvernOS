//! Wyvern core library surface.
//!
//! Stage 0 defines the architectural boundaries only. Runtime behavior will be
//! added later, starting with the orchestration kernel in Stage 1.

pub mod adapters;
pub mod context;
pub mod decision;
pub mod domain;
pub mod events;
pub mod memory;
pub mod models;
pub mod orchestration;
pub mod skills;
pub mod storage;
pub mod tools;
pub mod tui;
