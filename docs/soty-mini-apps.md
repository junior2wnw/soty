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

The default APPKA path for "сделай аппку" requests is:

1. Build one self-contained HTML document. Avoid external CDNs unless the user
   explicitly wants a hosted app.
2. Register it through the computer plane with `inlineHtml` and `scope=chat`.
3. Verify that the app appears in the selected chat's APPS launcher and opens
   in the lower half of the dialog.

```json
{
  "operation": "appka",
  "appId": "my-appka",
  "title": "My APPKA",
  "summary": "Short label shown in Soty.",
  "icon": "remote",
  "inlineHtml": "<!doctype html><html><head><meta charset=\"utf-8\"></head><body>...</body></html>",
  "scope": "chat",
  "layout": "half",
  "open": true,
  "capabilities": ["chat.append", "agent.invoke", "terminal.run"]
}
```

For a hosted app, build or deploy the frontend, make it available at a
same-origin URL, trusted HTTPS URL, loopback helper, or future Soty/kernel proxy
URL, then register it with `operation=mini_app` and `url`.

Equivalent local CLI for an inline app:

```powershell
node scripts/soty-agent.mjs ctl mini-app --scope=chat --layout=half --html-file=app.html --capabilities=chat.append,agent.invoke my-appka "My APPKA"
```

Scopes:

- `chat`: synced through the encrypted selected Soty room. This is the default
  for generated APPKA helpers and is visible from other devices in that chat.
- `device`: synced through the encrypted room but shown only when the
  selected/proven device matches.
- `account`: local account-wide convenience state in this browser profile.

`chat` and `device` apps are stored in the room Y.Doc state, so they travel with
the Soty chat. `account` apps are stored in `localStorage` under
`soty:mini-apps:v1`; other Soty tabs on the same browser profile pick them up
through the storage event.

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
      "layout": "half",
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

Hosted apps are opened with:

- `sotyMiniApp`: app id
- `sotyNonce`: per-open nonce

Inline APPKA apps are opened in a sandboxed `srcdoc` frame and receive:

```js
const boot = window.SOTY_MINI_APP;
const nonce = boot.nonce;
const targetOrigin = boot.targetOrigin || "*";
```

Mini apps are chat surfaces, not separate browser windows. The shell opens them
in the lower half of the current dialog by default. The app may ask the shell to
switch layout with `window.resize`; supported layouts are `half`, `compact`,
`large`, `full`, and `floating`, with optional safe CSS `height`/`width`.
The shell always owns window chrome. There is no app close affordance or close
capability; the required shell control is collapse/restore.

The app sends messages to the parent. Hosted same-origin apps should use
`location.origin`; inline sandboxed apps must use `targetOrigin` from the
bootstrap, normally `"*"`.

```js
parent.postMessage({
  schema: "soty.mini-app.v1",
  nonce,
  type: "ready"
}, targetOrigin);
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
  window: {
    layout: "half",
    height: "clamp(260px, 50svh, 620px)",
    width: "auto",
    collapsed: false,
    layouts: ["half", "compact", "large", "full", "floating"]
  },
  capabilities: ["chat.append", "agent.invoke", "terminal.run", "window.collapse", "window.resize"]
}
```

Supported app-to-shell messages are capability-gated per app:

- `chat.append`: `{ text }` appends a visible message to the selected chat.
- `agent.invoke`: `{ text, visibleText? }` sends a task to the agent in the selected chat.
- `terminal.run`: `{ command, timeoutMs?, runAs? }` runs through the selected remote console when control is ready.
- `window.resize`: `{ layout, height?, width? }` requests a shell-managed size/layout.
- `window.collapse`: collapses the mini app panel into its restore dock.

`window.collapse` and `window.resize` are always available; all other bridge
actions must be listed in the registered `capabilities`. The contract
intentionally stays tiny: apps own their frontend content and controls, Soty
owns chat context, panel chrome, agent invocation, remote command routing, and
room/device state. If the app has many controls, keep them inside the HTML with
normal pointer/touch scrolling instead of asking Soty for more buttons.

## Agent Notes

When the Soty Agent or Codex integrates a mini app, read
`node_modules/trustlink-kernel/docs/app-surfaces.md` first, then this adapter
doc. Keep new app UI thin, register only the needed capabilities, add the origin
to CSP only when it is trusted, and keep remote-device access behind the Soty
kernel bridge. Use `computer` operation `appka`/`mini_app`/`surface` or
`sotyctl mini-app` to install/open the app. For generated APPKA helpers, default
to inline HTML and `scope=chat`; use URL hosting only when the app is too large,
needs its own build pipeline, or the user asks for a domain/server. If a
repeated integration teaches a better route, update TrustLink Kernel first and
then this Soty adapter.
