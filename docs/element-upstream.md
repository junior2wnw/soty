# Element Upstream

Soty keeps Element Web and Element Desktop as upstream git submodules, not as a
forked code copy.

## Layout

- `upstream/element-web` tracks `https://github.com/element-hq/element-web.git`
  on `develop`.
- `upstream/element-desktop` tracks
  `https://github.com/element-hq/element-desktop.git` on `develop`.
- Soty code, adapters, PWA routing, agent behavior, and branding stay outside
  `upstream/element-*`.

Do not edit files under `upstream/element-*` for Soty-specific behavior. Put
integration code in Soty-owned paths and keep Element updates as plain submodule
gitlink bumps. If a change belongs in Element itself, make it an upstream PR
instead of carrying a local patch.

## Commands

Initialize both upstream checkouts:

```powershell
pnpm element:init
```

Show pinned refs and local submodule state:

```powershell
pnpm element:status
```

Move both submodules to the latest upstream `develop` refs:

```powershell
pnpm element:update
```

After `pnpm element:update`, verify the Soty adapter layer and commit only the
changed submodule gitlinks plus any Soty-side compatibility changes.

## Product Direction

Use `6211cf72148777ea575f7b0462b230dcb421c4d0` as the behavior reference for
Soty's agent mode:

- selected cell is the active context;
- agent mode is explicit and private;
- agent history is separate from normal chat noise;
- launcher/search is the main way into actions and mini-apps.

Use current Soty PWA work as the delivery layer:

- installable client shell;
- standalone mode;
- scoped manifests;
- direct routes for cards, chats, and device contexts;
- notification and wake-up plumbing.

The new client should be Element-like in structure, not a forked Element skin:

- left rail: spaces, cells, devices, and saved contexts;
- center: active chat, task, or room timeline;
- right panel: identity card, access, files, mini-apps, and agent tools;
- composer modes: message, agent, and command;
- launcher: unified actions and mini-app discovery.

## Soty Client Shell

The first Soty-owned shell lives in `src/features/soty-client-shell.ts` and is
available at `/client`, `?client=1`, `?view=client`, or `?view=soty-client`.
It reads the existing Soty local state and keeps compatibility with the current
runtime:

- cell selection writes `soty:selected:v1`;
- agent mode writes `soty:agent-mode:v1`;
- timeline preview reads `soty:text-snapshots:v1`;
- private agent preview reads `soty:agent-private-log:v1`;
- app preview reads `soty:mini-apps:v1`;
- opening a live flow routes back to the existing `/?pwa=1&chat=...` runtime.
