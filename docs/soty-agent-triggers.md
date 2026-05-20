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

Use a trigger when the user should not wait in a long open answer:

1. Say one short handoff in the user's language, for example: "Back in 5
   minutes." or "I will write when the task finishes."
2. Set a trigger with a compact message describing the next exact check.
3. When the trigger fires, continue from the trigger message.
4. If timing or matching was wrong, record a sanitized memory improvement so the
   next comparable trigger is faster and more precise.

Time trigger:

```json
{
  "operation": "trigger",
  "action": "set",
  "kind": "time",
  "afterMs": 300000,
  "label": "install status",
  "message": "Check the install status now. Start with one short update, then inspect proof."
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
