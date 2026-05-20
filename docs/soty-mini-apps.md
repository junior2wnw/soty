# Soty Mini Apps

Mini apps are small frontend bundles rendered inside the selected Soty chat. They are registered by a manifest, so a project can be updated or replaced without editing the chat shell.

Public/user-installed mini apps are currently gated off in the app shell. The
only enabled mini app today is the built-in remote commands panel in the
Agent account. Keep the manifest contract small so enabling user apps later is
one flag plus manifest entries, not another UI rewrite.

Remote connection technology lives in TrustLink Kernel
`docs/app-surfaces.md` (`junior2wnw/4-2-rf`). Soty is only the application
adapter: it owns the manifest, account gating, CSP env wiring, and the visible
chat panel.

## Register An App

1. Build the frontend project to a static folder.
2. Put it under `public/mini-apps/<app-id>/`.
3. Add one entry to `public/mini-apps/manifest.json`.

```json
{
  "schema": "soty.mini-apps.v1",
  "apps": [
    {
      "id": "my-tool",
      "title": "My Tool",
      "summary": "Short label shown in Soty.",
      "icon": "remote",
      "url": "/mini-apps/my-tool/index.html",
      "height": "clamp(260px, 46vh, 560px)",
      "capabilities": ["chat.append", "agent.invoke", "terminal.run"]
    }
  ]
}
```

Same-origin apps work by default. Soty resolves mini-app URLs through
`trustlink-kernel` app-surface helpers. During development, loopback URLs such
as `http://localhost:5174/` are accepted by the client and the server CSP allows
localhost frames. For other trusted HTTPS origins, set `SOTY_MINI_APP_FRAME_SRC`.

Do not give a mini app direct relay secrets or raw device authority. A mini app
is a frontend; Soty owns identity, trust, selected chat/device context, agent
invocation, terminal routing, file/artifact transfer, and proof.

## Bridge

The shell opens the app with:

- `sotyMiniApp`: app id
- `sotyNonce`: per-open nonce

The app sends messages to the parent:

```js
parent.postMessage({
  schema: "soty.mini-app.v1",
  nonce,
  type: "ready"
}, location.origin);
```

The shell replies with:

```js
{
  schema: "soty.mini-app.context.v1",
  nonce,
  appId,
  device: { id, nick },
  selected: {
    tunnelId,
    label,
    color,
    agent,
    remoteController,
    remoteHost,
    syncState
  },
  capabilities: ["chat.append", "agent.invoke", "terminal.run", "close"]
}
```

Supported app-to-shell messages:

- `chat.append`: `{ text }` appends a visible message to the selected chat.
- `agent.invoke`: `{ text, visibleText? }` sends a task to the agent in the selected chat.
- `terminal.run`: `{ command, timeoutMs?, runAs? }` runs through the selected remote console when control is ready.
- `close`: closes the mini app panel.

The contract intentionally stays tiny: apps own their frontend, Soty owns chat context, agent invocation, remote command routing, and room/device state.

## Agent Notes

When the Soty Agent or Codex integrates a mini app, read
`node_modules/trustlink-kernel/docs/app-surfaces.md` first, then this adapter
doc. Keep new app UI thin, register only the needed capabilities, add the origin
to CSP only when it is trusted, and keep remote-device access behind the Soty
kernel bridge. If a repeated integration teaches a better route, update
TrustLink Kernel first and then this Soty adapter.
