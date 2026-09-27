# Contributing to Wyvern

Wyvern is intended to become a serious systems project. Contributions should favor clarity, determinism, and architectural discipline over speculative abstractions.

## Coding Standards

- Prefer Rust from the beginning.
- Keep code paths deterministic unless a reasoning boundary is explicitly required.
- Avoid introducing abstractions before the first real use case exists.
- Keep modules small and purpose-specific.
- Preserve provenance in memory, events, and artifacts.
- Separate orchestration, decision, reasoning, tools, persistence, and presentation.
- Use structured types for runtime, mission, event, and memory data.
- Avoid hiding control flow behind unnecessary magic.

## Architectural Principles

- The runtime kernel should not depend on an LLM for basic lifecycle management.
- Decision logic should remain separate from reasoning logic.
- Tools should be accessed through abstractions, not direct coupling.
- Context should be compiled, scoped, and relevance-filtered.
- Memory should be scoped and provenance-aware.
- The TUI should observe orchestration state rather than duplicate it.
- OS-specific behavior should live behind adapters.

## Scope Discipline

- Do not implement the runtime engine during Stage 0.
- Do not add fake components just to make the repository look complete.
- If a decision is unresolved, document it as TBD rather than guessing.
- Keep Stage 1 focused on the smallest useful kernel.

## Preferred Workflow

1. Update the relevant design document first if the architecture changes.
2. Add the smallest implementation that satisfies the current stage.
3. Validate behavior with focused tests or checks.
4. Keep changes aligned with the staged roadmap.

## Review Expectations

Reviews should prioritize:

- correctness
- determinism
- clarity of boundaries
- maintainability
- stage alignment
- security implications
