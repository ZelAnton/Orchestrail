# Architecture Decision Records

Architecture Decision Records (ADRs) capture architectural choices that are significant enough to affect how Orchestrail is built, operated, or extended. They preserve the decision, its rationale, and the consequences so that the code and its constraints remain understandable over time.

This document defines Orchestrail's ADR practice.

## Recording a decision

Create one Markdown file in this directory named `<NNNN>-<kebab-case-title>.md`. Allocate numbers sequentially from `0001` through `9999`; never reuse a number. Use the following format:

```markdown
# NNNN. Decision title

Status: Accepted · Date: YYYY-MM-DD

## Context

Describe the background, constraints, and rationale that made the decision necessary.

## Decision

State the architectural choice and its scope.

## Consequences

- Record benefits, costs, constraints, and related ADRs.
```

New records start as `Accepted` only once the decision has been made. When a later decision replaces an earlier one, retain the earlier record and change its status to `Superseded`; the replacement must link back to it. Use `Amends` when a record changes or clarifies an earlier decision without replacing its core choice. Reference related records by number and title, and cite repository paths rather than external links where code is the supporting evidence.

## Index

- [0001. ProcessKit as the sole process execution boundary](0001-processkit-sole-boundary.md)
- [0002. Typed VCS crates instead of shell CLI wrappers](0002-typed-vcs-crates.md)
- [0003. Markdown control plane with canonical ASCII states](0003-markdown-control-plane-canonical-states.md)
- [0004. Fail closed for unported integrations](0004-fail-closed-unported-integrations.md)
- [0005. Deterministic UUIDv5 event identities](0005-deterministic-uuidv5-event-identities.md)
