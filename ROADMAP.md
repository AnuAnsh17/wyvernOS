# Wyvern Roadmap

This roadmap tracks the staged development plan for Wyvern.

## Status Legend

- Planned: not started
- In Progress: active work but incomplete
- Completed: implemented and validated

## Stage 0 - Repository / Architecture

Status: COMPLETED

Goals:

- create the repository structure
- document the architecture
- define the development philosophy
- capture the staged implementation plan

## Stage 1 - Runtime Kernel

Status: IMPLEMENTED

Goals:

- typed Mission/Runtime/Agent IDs
- persistent-safe UUID-based IDs
- runtime lifecycle state machine
- recursive parent/child hierarchy
- concurrent runtime execution
- multi-mission support
- failure handling
- cancellation
- SQLite persistence
- restart/recovery hydration
- runtime event publication
- deterministic demo executor
- CLI inspection

## Stage 1A - Runtime Kernel Hardening

Status: COMPLETED

Goals:

- validation tests for lifecycle transitions
- restart-safe ID verification
- parent/child integrity checks
- cross-mission rejection checks
- recursive tree execution checks
- concurrent mission execution checks
- failure propagation checks
- cancellation checks
- event subscriber checks
- restart recovery checks
- CLI inspection and recursive demo checks

## Stage 2 - Context, State & Event Infrastructure

Status: Implemented

Goals:

- typed branch-local runtime context
- deterministic context compiler boundary
- explicit downward context inheritance and upward child-result propagation
- context provenance and immutable versioned SQLite snapshots
- machine-readable event envelopes and context update notifications
- live event bus retained separately from durable context state

## Stage 3 - Scoped Memory

Status: Planned

Goals:

- scoped memory
- mission state
- branch state
- artifact references
- relevance filtering
- provenance
- durable structured memory beyond active runtime context

## Stage 4 - Tool System

Status: Planned

Goals:

- common tool interfaces
- filesystem
- terminal
- process execution

## Stage 5 - Model Router

Status: Planned

Goals:

- provider abstraction
- external reasoning API support
- policy-driven model selection

## Stage 6 - Laya Integration

Status: Planned

Goals:

- fast decision provider
- deterministic decision logic
- separation from reasoning

## Stage 7 - TUI

Status: Planned

Goals:

- mission view
- runtime tree
- agent view
- event stream
- system monitoring
- model activity
- command input

## Stage 8 - Browser / Application Control

Status: Planned

Goals:

- browser
- tabs
- filesystem extensions
- IDE control
- application control

## Stage 9 - Skills

Status: Planned

Goals:

- skill registry
- skill loading system

## Stage 10 - Advanced Agents

Status: Planned

Goals:

- research
- coding
- browser automation
- security
- academic work
- reporting
- validation

## Stage 11 - Reliability

Status: Planned

Goals:

- retries
- crash recovery
- runtime restoration
- cancellation
- timeouts
- dependency handling
- checkpointing
- graceful shutdown
- mission resumption

## Stage 12 - Deep OS Integration

Status: Planned

Goals:

- Linux system integration
- process management
- sandboxing
- permissions
- resource control
- isolation
- system services

## Stage 13 - Wyvern Linux Environment

Status: Planned

Goals:

- investigate a minimal Linux environment or distribution where Wyvern becomes the primary AI orchestration layer
