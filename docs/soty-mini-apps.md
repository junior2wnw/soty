# Soty Mini Apps

Mini apps are small frontend bundles rendered inside the selected Soty chat. They are registered by a manifest, so a project can be updated or replaced without editing the chat shell.

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

Same-origin apps work by default. During development, loopback URLs such as `http://localhost:5174/` are accepted by the client and the server CSP allows localhost frames. For other trusted origins, set `SOTY_MINI_APP_FRAME_SRC`.

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
