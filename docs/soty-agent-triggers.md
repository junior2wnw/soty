# Soty Agent Triggers

Agent triggers are the small wake-up layer for Soty Agent. They are not a
second chat system: a trigger fires by sending a normal message into the Agent
dialog, so the same memory, tools, target rules, and user-visible proof still
apply.

## Contract

- Schema: `soty.agent.triggers.v1`
- Public entry: `computer` with `operation: "trigger"`
- Local endpoints:
  - `GET /operator/triggers`
  - `POST /operator/trigger`
  - `POST /operator/trigger-event`
- Kinds:
  - `time`: one-shot wake-up by `at` or `afterMs`
  - `interval`: repeated wake-up by `everyMs`
  - `event`: wake-up when an event name and optional `match` object pass

## Agent Use

Use a trigger only when the remaining work is genuinely idle/background
waiting. A trigger must never replace active investigation, durable polling, or
tool continuation while there is still progress to make.

1. Keep working normally until the next useful step depends on time or an event.
2. Set a trigger with a compact message describing the next exact check.
3. Give a short handoff in the user's language if the user needs to know why the
   chat will be quiet.
4. When the trigger fires, continue from the trigger message until a real
   terminal state or blocker.
5. If timing or matching was wrong, record a sanitized memory improvement so the
   next comparable trigger is faster and more precise.

Time trigger:

```json
{
  "operation": "trigger",
  "action": "set",
  "kind": "time",
  "afterMs": 300000,
  "label": "install status",
  "message": "Check the install status now, inspect proof, and continue until a terminal state or real blocker."
}
```

Event trigger:

```json
{
  "operation": "trigger",
  "action": "set",
  "kind": "event",
  "event": "action.finished",
  "match": {
    "family": "windows-reinstall",
    "status": { "oneOf": ["ok", "failed", "blocked"] }
  },
  "message": "A Windows reinstall action finished. Read status/proof and continue the user-facing dialog."
}
```

List or cancel:

```json
{ "operation": "trigger", "action": "list" }
{ "operation": "trigger", "action": "cancel", "triggerId": "<id>" }
```

Custom events can be emitted through `POST /operator/trigger-event` or
`computer` `operation: "trigger", action: "event"`. Keep event payloads
sanitized; do not place secrets, raw private files, or long transcripts into
trigger payloads.
