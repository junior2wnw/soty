# Soty Memory Plane

Soty memory is not a prompt bundle and not a single external memory product. It is a server-side control plane with one stable contract:

- append-only sanitized receipts are the source of truth;
- `soty.memctl.v1` promotes repeated proof into reusable route memory;
- `/api/agent/memory/query` returns short ranked hints to Codex;
- Codex treats memory as evidence, not as rules;
- user-device agents stay thin and only send receipts or execute capability actions.

## Why This Shape

The simplest reliable long-term memory is a durable event log plus deterministic promotion. Vector databases and products such as mem0 or mempalace can be useful as retrieval backends later, but they must not own the contract. Soty owns the contract, so backend engines can be swapped or combined without changing the installed agent.

## Runtime Flow

1. The agent performs work through capability tools.
2. The agent writes sanitized receipts: family, route, result, proof shape, duration, platform, task signature, hashes.
3. The server appends receipts to JSONL partitions.
4. MemCtl builds a bounded working view from the append-only source.
5. MemCtl scores proven routes, stop gates, route fixes, and dialog memory markers.
6. The next task asks `/memory/query` with family/platform filters.
7. Codex receives only compact hints: confidence, score, route, guidance, and evidence.

## Promotion Rules

Routes become reusable only when evidence is strong enough:

- repeated success increases confidence;
- reusable proof fields such as `reuseKey`, `successCriteria`, and `context` help promotion;
- newer failures after success mark the route as conflicted;
- repeated failures become stop gates;
- slow or low-quality routes become route-fix hints;
- memory markers are deduplicated and ranked, not blindly injected.

## Production Invariants

- Memory never blocks the user task.
- Raw commands and private identifiers are redacted or hashed before server storage.
- Append-only receipts are never destructively edited by query logic.
- Query windows are bounded for speed; the source log remains durable.
- External memory engines are adapters, not authority.

## Current Schemas

- `soty.memory-plane.v1`: public memory plane contract in the agent manifest.
- `soty.memory.receipt.v1`: sanitized event written by agents.
- `soty.memctl.v1`: deterministic promotion and scoring controller.
- `soty.memory.report.v2`: full server report for review.
- `soty.memory.query.v2`: compact ranked hints for Codex.
