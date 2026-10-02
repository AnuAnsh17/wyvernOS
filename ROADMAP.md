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

Status: Implemented

Goals:

- scoped memory
- mission state
- branch state
- artifact references
- relevance filtering
- provenance
- durable structured memory beyond active runtime context

## Stage 4 - Capability / Tool Boundary

Status: Implemented

Goals:

- common tool interfaces
- typed capability and tool descriptors
- explicit scoped permission boundary
- executor contract and in-process mock
- request/result lifecycle, events, context/memory handoff

## Stage 5 - Scheduler / Execution Coordinator

Status: Implemented

Goals:

- bounded asynchronous worker pool and atomic runtime claims
- deterministic priority with age-based fairness
- explicit dependencies and blocked dependency failures
- waiting, resume, timeout, cancellation, and retry/backoff
- durable scheduler state and interrupted-runtime recovery
- scheduler snapshots and CLI inspection

## Stage 6A-D - Decision, Behavior, and Durable Identity

Status: Implemented

Goals completed:

- model-agnostic `DecisionEngine` request/result contract and deterministic mock
- bounded, kernel-applied runtime lifecycle decisions with freshness checks
- `RuntimeBehavior` registry and common direct/scheduled execution path
- durable Agent definitions and SQLite event history

## Stage 7 - Kernel Hardening

### Stage 7A - Kernel-Owned Authorization

Status: Implemented

Goals completed:

- typed durable grants bound to AgentId, capability, tools, mission/runtime scope, and maximum risk
- issue time, optional expiration, and revocation state in SQLite
- kernel authorization checks before capability dispatch
- durable grant, revoke, checked, and denied events
- tests for allow/deny, invalid/duplicate/unknown grants, identity and scope mismatch, risk, expiry, revocation, and restart recovery

Known boundary: issuer labels are recorded by the trusted local management API but are not authenticated human/operator identities. Grant mutations and event appends are not yet atomic.

### Stage 7B - Atomic State and Event Consistency

Status: In progress

Goals:

- transact versioned runtime lifecycle state changes with corresponding durable event appends in SQLite
- preserve existing compare-and-set runtime version checks
- broadcast the exact envelope committed by the transaction
- extend atomic coverage to remaining critical mutations and injected persistence-failure recovery

### Stage 7C - Scheduler Hardening

Status: In progress

Goals:

- reconcile scheduler recovery with already-terminal kernel runtimes
- address waiting-parent recovery and dependency dead ends
- harden timeout, cancellation, retry, and completion races
- preserve fairness and prevent duplicate claims
- remove unnecessary busy polling where present
- repeat full-suite runs and resolve the known intermittent Stage 1 concurrency-test stall

### Stage 7D - First Real Read-Only Capability

Status: Implemented

Goals completed:

- implement a scoped, workspace-configured `filesystem.read` capability
- enforce canonical path boundaries, traversal protection, regular-file checks, file-size limits, authorization, and timeout
- return structured UTF-8 or binary results and emit durable events
- tests cover denied paths, traversal, oversized and missing files, concurrency, restart registration, and symlink-specific behavior

## Stage 8 - Skill Integration Layer

Status: In progress

Sub-stages:

- **8A:** typed Skill identity, metadata, capability declarations, and adapter contract (Implemented)
- **8B:** metadata registry, lookup/resolution, persistence, and process-local adapter binding
- **8C:** bridge adapters through the existing capability and authorization path; use an offline mock skill
- **8D:** runtime skill orchestration demo with decisions, authorization, result propagation, and restart coverage

Skills are integrations that expose capabilities; they are not runtimes, agents, models, or kernel authorities. Executable adapter objects remain process-local.

## Stage 9 - Specialized Decision Intelligence

Status: Planned

Sub-stages:

- **9A:** formalize the decision ontology and build a validated scenario dataset while protecting the held-out Wyvern benchmark
- **9B:** add a replaceable teacher interface and structured distillation/quality filtering
- **9C:** fine-tune and evaluate a specialized Laya implementation of `DecisionEngine`
- **9D:** add conservative hybrid local/cloud decision routing with escalation and human-approval boundaries

Kaggle is the preferred GPU training environment for Stage 9 experiments. Keep authentication secrets and large model artifacts outside Git, record the actual execution environment and reproducible configurations, and compare the trained model against the protected held-out benchmark before calling the experiment successful.

## Later Work

Planned areas after the prototype sequence include a TUI, broader browser/application integration, additional reusable agents and skills, stronger recovery/checkpointing, deep Linux integration, and exploration of a Wyvern-oriented Linux environment. These items are not implemented by Stages 6A-D or 7A.
