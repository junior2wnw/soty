# Soty Mini Apps

Mini apps are small frontend bundles rendered inside the selected Soty chat.
The static public catalog is still gated off, but an attached Agent can install
a safe app surface into the current local account, current chat, or selected
device scope.

Remote connection technology lives in TrustLink Kernel
`docs/app-surfaces.md` (`junior2wnw/4-2-rf`). Soty is only the application
adapter: it owns the manifest, account gating, CSP env wiring, and the visible
chat panel.

## Agent Install/Open

The agent path is:

1. Build or deploy the frontend.
2. Make it available at a same-origin URL, trusted HTTPS URL, loopback helper,
   or future Soty/kernel proxy URL.
3. Register it through the computer plane:

```json
{
  "operation": "mini_app",
  "appId": "my-tool",
  "title": "My Tool",
  "summary": "Short label shown in Soty.",
  "icon": "remote",
  "url": "/mini-apps/my-tool/index.html",
  "scope": "account",
  "open": true,
  "capabilities": ["chat.append", "agent.invoke", "terminal.run"]
}
```

Equivalent local CLI:

```powershell
node scripts/soty-agent.mjs ctl mini-app --scope=account my-tool "My Tool" /mini-apps/my-tool/index.html chat.append,agent.invoke
```

Scopes:

- `account`: appears for the current local Soty account.
- `chat`: appears only in the selected chat at install time.
- `device`: appears only when the selected/proven device matches.

The app is stored in `localStorage` under `soty:mini-apps:v1` and is not synced
to other users. Other Soty tabs on the same browser profile pick it up through
the storage event.

## Static Catalog

The static manifest remains disabled for normal users. To maintain first-party
apps for later enabling:

1. Build the frontend to a static folder.
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

Supported app-to-shell messages are capability-gated per app:

- `chat.append`: `{ text }` appends a visible message to the selected chat.
- `agent.invoke`: `{ text, visibleText? }` sends a task to the agent in the selected chat.
- `terminal.run`: `{ command, timeoutMs?, runAs? }` runs through the selected remote console when control is ready.
- `close`: closes the mini app panel.

`close` is always available; all other bridge actions must be listed in the
registered `capabilities`. The contract intentionally stays tiny: apps own
their frontend, Soty owns chat context, agent invocation, remote command
routing, and room/device state.

## Agent Notes

When the Soty Agent or Codex integrates a mini app, read
`node_modules/trustlink-kernel/docs/app-surfaces.md` first, then this adapter
doc. Keep new app UI thin, register only the needed capabilities, add the origin
to CSP only when it is trusted, and keep remote-device access behind the Soty
kernel bridge. Use `computer` operation `mini_app`/`surface` or `sotyctl
mini-app` to install/open the app in the current account. If a repeated
integration teaches a better route, update TrustLink Kernel first and then this
Soty adapter.
