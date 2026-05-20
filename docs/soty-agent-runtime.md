# Soty Agent Runtime

Soty Agent is the installed local runtime for a user's computer. The reusable
technology contract lives in `trustlink-kernel/docs/agent-runtime.md`; this file
documents the Soty adapter.

## Shape

- `computer` is the public front door.
- `soty_*` names are compatibility aliases behind that front door.
- Long or repeated work should become durable jobs with status, result, proof,
  and a reusable learning receipt.
- Quiet waits and event-based continuation should use Agent Triggers
  (`docs/soty-agent-triggers.md`) instead of keeping a chat turn open just to
  say "still waiting".
- Specific programs should be connected as adapters, not as one-off command
  strings.

## Capability Families

Soty publishes the TrustLink runtime families through `/health`,
`/operator/toolkits`, and `/agent/manifest.json`:

- console, filesystem, process, service, package
- browser, desktop, screen, keyboard, mouse, clipboard
- network, surface, app, api, job, artifact, audio
- os, transaction, device
- trigger

Mini app work enters through `surface.*` plus the app-surface install request:
build/serve the frontend, then register/open it with `computer`
operation `mini_app` or `surface`.

## Triggers

Agent Triggers are a small wake-up layer:

1. The Agent gives the user a short handoff.
2. It sets a `time`, `interval`, or `event` trigger through `computer`
   operation `trigger`.
3. When the trigger fires, Soty sends a normal message into the Agent chat.
4. The Agent continues from that message, verifies state with tools, and records
   sanitized memory if the trigger needed tuning.

Program-specific work should enter through `app.*`, `api.*`, or
`transaction.*` and then use the existing lower-level browser/desktop/console
routes only as implementation details.

## Transactions

For deals, orders, payments, publishing, or similarly irreversible work:

1. Prepare or preview the intended action.
2. Show/return structured proof of what will happen.
3. Require explicit confirmation before submit/cancel.
4. Submit through the local approved app, browser profile, or API adapter.
5. Return result proof.

Secrets stay in the local program, browser profile, OS store, or user-approved
adapter. They should not be copied into prompts, logs, memory receipts, or chat.

## Adding A Program Adapter

1. Name the family and actions: for example `app.connect`, `app.read`,
   `app.write`, `app.submit`, `api.post`, `transaction.submit`.
2. Decide risk and confirmation behavior.
3. Use durable jobs for long or state-changing actions.
4. Return proof fields that another agent can verify.
5. Promote repeated successful routes into tests and manifest-pinned toolkit
   entries.

The goal is a small stable runtime surface that can connect many future Soty
mini apps, chats, and remote workflows without rewriting the agent.
