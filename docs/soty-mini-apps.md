# Soty Mini Apps

Mini apps are small frontend bundles rendered inside the selected Soty cell.
They are stored as a small app index: title, tags, optional profile, scope,
visibility, placement, URL/inline bundle, display hints, and capability grants.
The static public catalog is still gated off, but an attached Agent can install
a safe app surface into the current local account, current chat, or selected
device scope.

Remote connection technology lives in TrustLink Kernel
`docs/app-surfaces.md` (`junior2wnw/4-2-rf`). Soty is only the application
adapter: it owns the manifest, account gating, CSP env wiring, and the visible
cell surface.

## Mobile Creation Status

Creating an APPKA from a phone is intentionally OFF for now:

```json
{
  "schema": "soty.mobile-appka.policy.v1",
  "enabled": false
}
```

Best future route: the phone should only be the controller UI. The actual build
and serving work should run on a selected trusted device through the Soty/Kernel
computer plane, then the result should be registered back into the chat as
inline HTML or a trusted hosted surface. This keeps server load low because the
server remains a relay/control plane, not a build host or asset CDN. For small
apps, prefer one sandboxed inline HTML document synced in the room; for larger
apps, prefer a device-local or user-provided host that is exposed through the
kernel app-surface route.

Until this policy is enabled, agents must not promise phone-local deployment.
They may design the app, choose the target trusted device, and ask the device
agent to build/register it through the normal APPKA path.

## Agent Install/Open

The default APPKA path for "сделай аппку" requests is:

1. Build one self-contained HTML document. Avoid external CDNs unless the user
   explicitly wants a hosted app.
2. Register it through the computer plane with `inlineHtml` and `scope=chat`.
3. Verify that the app appears on the selected cell's app shelf and opens in
   the lower half of the cell.

```json
{
  "operation": "appka",
  "appId": "my-appka",
  "title": "My APPKA",
  "summary": "Short label shown in Soty.",
  "icon": "remote",
  "tags": ["crm", "orders"],
  "profileTitle": "Work",
  "inlineHtml": "<!doctype html><html><head><meta charset=\"utf-8\"></head><body>...</body></html>",
  "scope": "chat",
  "visibility": "granted-cells",
  "layout": "half",
  "open": true,
  "capabilities": ["chat.append", "agent.invoke", "terminal.run"]
}
```

For a hosted app, build or deploy the frontend, make it available at a
same-origin URL, trusted HTTPS URL, loopback helper, or future Soty/kernel proxy
URL, then register it with `operation=mini_app` and `url`.

## Info Card Modules

Info cards may attach a mini app as a card module without using Soty as a
hosting server:

```json
{
  "kind": "miniapp",
  "title": "Tiny Tool",
  "summary": "One-screen helper",
  "href": "about:srcdoc",
  "inlineHtml": "<!doctype html><html><body>...</body></html>",
  "visibility": "public",
  "layout": "large"
}
```

Inline info-card mini apps are opened in a sandboxed `srcdoc` frame. They must
be self-contained, small, and must not receive relay secrets or raw device
authority. If a real HTTPS or loopback URL already exists, the card may store
that URL instead. Storing assets under the Soty web origin, `/mini-apps`, or the
built-in Soty server remains disabled until the hosting/sandbox model is a
separate TrustLink Kernel surface.

Equivalent local CLI for an inline app:

```powershell
node scripts/soty-agent.mjs ctl mini-app --scope=chat --visibility=granted-cells --layout=half --html-file=app.html --capabilities=chat.append,agent.invoke my-appka "My APPKA"
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

Visibility:

- `private`: for me.
- `granted-cells`: cells that were given access.
- `my-cells`: cells added by me.
- `public`: everyone, including cells that do not know about me yet.

Agents may set `visibility` during install/update. Missing visibility keeps the
old scope behavior: account apps are local to me, and chat/device apps stay in
their encrypted room/device context.

## Cell Surface And Gallery

Mini apps are the first visible entity of a selected cell. The cell header shows
a horizontal app shelf before chat, wall, and reputation controls. A cell may
hold any number of apps; the shelf scrolls sideways and each tile opens the app
directly. The shelf shows selected-cell apps first, then other known apps, so a
user does not need to open search just to reach an already registered app. The
first shelf button opens a gallery over every app known to the browser, so a
user can browse apps across cells without using search as the primary path.

Starting or hosting a web app is not enough: the app appears in Soty only after
it is registered as a mini app through `operation=appka`, `operation=mini_app`,
or `sotyctl mini-app`. That record binds the URL/inline HTML to the selected
cell, account, or selected device.

Room apps stay in the encrypted room state and belong to that cell. Account apps
stay in the local app index and can appear in any selected cell. Device apps stay
room-synced but only appear when the selected/proven device matches.

## Profiles And Find

Every app may carry:

- `profileId` / `profileTitle`: a user-facing group such as `Work`, `Shop`, or
  a created persona/profile.
- `tags`: short search terms.
- `placement`: derived by Soty from the app surface mode: `inline`,
  `same-origin`, `remote-origin`, `device-local`, or `kernel-proxy`.

The gallery is a global index over the selected chat, all known room apps, and
local account apps. Find ranks title matches first, then tags, then profile,
summary, id, and URL. A room app found through the gallery switches to its
owning chat before opening, so users can keep many apps across many profiles
without separate hard-coded launchers.

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
      "tags": ["crm", "orders"],
      "profileTitle": "Work",
      "url": "/mini-apps/my-tool/index.html",
      "visibility": "private",
      "layout": "half",
      "height": "clamp(260px, 46vh, 560px)",
      "capabilities": ["chat.append", "agent.invoke", "terminal.run"]
    }
  ]
}
```

Same-origin and HTTPS apps work by default. Soty resolves mini-app URLs through
`trustlink-kernel` app-surface helpers. During development, loopback URLs such
as `http://localhost:5174/` are accepted by the client and the server CSP allows
localhost frames. Use `SOTY_MINI_APP_FRAME_SRC` only for additional explicit
frame sources beyond the default HTTPS and loopback policy.

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
  app: {
    id,
    title,
    tags,
    profileId,
    profileTitle,
    visibility,
    placement
  },
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
- `agent.invoke`: `{ text, visibleText? }` sends a private task to the agent with the selected cell chat as context. The agent reply is local-only unless the app also sends an explicit `chat.append`.
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
