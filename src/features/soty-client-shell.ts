import { icon } from "../icons";
import { loadSelectedTunnelId, loadTunnels, saveSelectedTunnelId } from "../trustlink";
import type { TunnelRecord } from "../trustlink";

type ClientSection = "messages" | "agent" | "apps" | "pwa";
type ClientComposerMode = "message" | "agent" | "command";

type ClientMiniApp = {
  readonly id: string;
  readonly title: string;
  readonly summary: string;
  readonly scope: string;
  readonly visibility: string;
};

type ClientTimelineLine = {
  readonly id: string;
  readonly text: string;
  readonly mine: boolean;
};

type ClientAgentLine = {
  readonly role: "user" | "agent";
  readonly text: string;
  readonly createdAt: string;
};

type ClientState = {
  readonly tunnels: readonly TunnelRecord[];
  readonly visibleTunnels: readonly TunnelRecord[];
  readonly selected: TunnelRecord | null;
  readonly selectedId: string;
  readonly query: string;
  readonly section: ClientSection;
  readonly composerMode: ClientComposerMode;
  readonly agentMode: boolean;
  readonly standalone: boolean;
  readonly miniApps: readonly ClientMiniApp[];
  readonly timeline: readonly ClientTimelineLine[];
  readonly agentLines: readonly ClientAgentLine[];
};

const textSnapshotsKey = "soty:text-snapshots:v1";
const agentModeKey = "soty:agent-mode:v1";
const agentPrivateLogKey = "soty:agent-private-log:v1";
const miniAppsRegistryKey = "soty:mini-apps:v1";

export function isSotyClientRoute(location: Location = window.location): boolean {
  const url = new URL(location.href);
  return url.pathname === "/client"
    || url.searchParams.get("client") === "1"
    || url.searchParams.get("view") === "client"
    || url.searchParams.get("view") === "soty-client"
    || isSotyClientHomeRoute(url);
}

function isSotyClientHomeRoute(url: URL): boolean {
  if (url.pathname !== "/" && url.pathname !== "") {
    return false;
  }
  const host = url.hostname.toLowerCase();
  if (host !== "xn--n1afe0b.online" && host !== "соты.online") {
    return false;
  }
  const legacyParams = [
    "reset-local",
    "j",
    "to",
    "room",
    "chat",
    "space",
    "module",
    "restore-local"
  ];
  return legacyParams.every((param) => !url.searchParams.has(param));
}

export function renderSotyClientShell(root: HTMLElement): void {
  document.body.classList.remove("personal-space-mode", "self-start-mode");
  document.body.classList.add("soty-client-mode");
  let query = "";
  let section: ClientSection = "messages";
  let composerMode: ClientComposerMode = "message";

  const paint = (focusSearch = false) => {
    const state = clientState(query, section, composerMode);
    root.innerHTML = renderClient(state);
    if (focusSearch) {
      const search = root.querySelector<HTMLInputElement>("[data-client-search]");
      search?.focus();
      search?.setSelectionRange(query.length, query.length);
    }
  };

  bindClient(root, () => clientState(query, section, composerMode), {
    setQuery: (value) => {
      query = value;
      paint(true);
    },
    setSection: (value) => {
      section = value;
      composerMode = value === "agent" ? "agent" : composerMode;
      paint();
    },
    setComposerMode: (value) => {
      composerMode = value;
      if (value === "agent") {
        section = "agent";
      }
      paint();
    },
    repaint: () => paint()
  });
  paint();
}

function clientState(query: string, section: ClientSection, composerMode: ClientComposerMode): ClientState {
  const tunnels = sortTunnels(loadTunnels());
  const selectedId = selectedTunnelId(tunnels);
  const selected = tunnels.find((tunnel) => tunnel.id === selectedId) || tunnels[0] || null;
  const actualSelectedId = selected?.id || "";
  const normalizedQuery = normalizeSearch(query);
  const visibleTunnels = normalizedQuery
    ? tunnels.filter((tunnel) => normalizeSearch(tunnelLabel(tunnel)).includes(normalizedQuery) || tunnel.id.includes(normalizedQuery))
    : tunnels;
  const miniApps = loadClientMiniApps();
  return {
    tunnels,
    visibleTunnels,
    selected,
    selectedId: actualSelectedId,
    query,
    section,
    composerMode,
    agentMode: selected ? selected.agent === true || loadAgentModes().get(selected.id) === true : false,
    standalone: isStandalonePwa(),
    miniApps,
    timeline: actualSelectedId ? loadTimeline(actualSelectedId) : [],
    agentLines: actualSelectedId ? loadAgentLines(actualSelectedId) : []
  };
}

function renderClient(state: ClientState): string {
  return `
    <section class="soty-client" data-client-section="${escapeAttr(state.section)}">
      <aside class="soty-client-rail" aria-label="${escapeAttr("Soty")}">
        <div class="soty-client-brand">
          <span>S</span>
          <b>Soty</b>
        </div>
        <nav class="soty-client-nav" aria-label="${escapeAttr("Разделы")}">
          ${renderSectionButton("messages", "Диалог", "mail", state)}
          ${renderSectionButton("agent", "Агент", "agent", state)}
          ${renderSectionButton("apps", "Apps", "apps", state)}
          ${renderSectionButton("pwa", "PWA", "install", state)}
        </nav>
      </aside>
      <aside class="soty-client-list" aria-label="${escapeAttr("Ячейки")}">
        <header>
          <span>
            <b>Ячейки</b>
            <small>${escapeHtml(String(state.tunnels.length))}</small>
          </span>
          <button type="button" data-client-open="classic" data-tooltip="Открыть текущий Soty">${icon("expand")}</button>
        </header>
        <label class="soty-client-search">
          ${icon("search")}
          <input data-client-search type="search" value="${escapeAttr(state.query)}" placeholder="найти" />
        </label>
        <div class="soty-client-cells">
          ${state.visibleTunnels.length
            ? state.visibleTunnels.map((tunnel) => renderTunnelRow(tunnel, state)).join("")
            : `<div class="soty-client-empty">${icon("search")}<span>Пусто</span></div>`}
        </div>
      </aside>
      <main class="soty-client-main">
        ${state.selected ? renderWorkspace(state) : renderEmptyWorkspace()}
      </main>
      <aside class="soty-client-side" aria-label="${escapeAttr("Контекст")}">
        ${renderInspector(state)}
      </aside>
    </section>
  `;
}

function renderSectionButton(section: ClientSection, label: string, iconName: Parameters<typeof icon>[0], state: ClientState): string {
  const active = state.section === section;
  return `
    <button type="button" data-client-section="${escapeAttr(section)}" aria-pressed="${active ? "true" : "false"}" data-tooltip="${escapeAttr(label)}">
      ${icon(iconName)}
      <span>${escapeHtml(label)}</span>
    </button>
  `;
}

function renderTunnelRow(tunnel: TunnelRecord, state: ClientState): string {
  const selected = tunnel.id === state.selectedId;
  const agentOn = tunnel.agent === true || loadAgentModes().get(tunnel.id) === true;
  return `
    <button class="soty-client-cell${selected ? " is-active" : ""}" type="button" data-client-select="${escapeAttr(tunnel.id)}">
      <span class="soty-client-avatar" style="--cell-color:${escapeAttr(tunnelColor(tunnel))}">${escapeHtml(tunnelInitial(tunnel))}</span>
      <span>
        <b>${escapeHtml(tunnelLabel(tunnel))}</b>
        <small>${escapeHtml(tunnelMeta(tunnel, agentOn))}</small>
      </span>
      ${tunnel.unread ? `<i aria-label="${escapeAttr("новое")}"></i>` : ""}
    </button>
  `;
}

function renderWorkspace(state: ClientState): string {
  const selected = state.selected;
  if (!selected) {
    return renderEmptyWorkspace();
  }
  return `
    <header class="soty-client-head">
      <span class="soty-client-avatar is-large" style="--cell-color:${escapeAttr(tunnelColor(selected))}">${escapeHtml(tunnelInitial(selected))}</span>
      <span class="soty-client-title">
        <small>${state.agentMode ? "agent on" : "dialog"}</small>
        <b>${escapeHtml(tunnelLabel(selected))}</b>
      </span>
      <div class="soty-client-head-actions">
        <button type="button" data-client-toggle-agent aria-pressed="${state.agentMode ? "true" : "false"}" data-tooltip="Агент">${icon("agent")}</button>
        <button type="button" data-client-open="chat" data-tooltip="Открыть поток">${icon("expand")}</button>
      </div>
    </header>
    <section class="soty-client-stream" aria-label="${escapeAttr("Поток")}">
      ${renderMainSection(state)}
    </section>
    <form class="soty-client-compose">
      <div class="soty-client-modes" role="tablist" aria-label="${escapeAttr("Режим")}">
        ${renderComposerMode("message", "Сообщение", state)}
        ${renderComposerMode("agent", "Агент", state)}
        ${renderComposerMode("command", "Команда", state)}
      </div>
      <textarea rows="1" name="draft" placeholder="${escapeAttr(composerPlaceholder(state))}"></textarea>
      <button type="submit">${icon(state.composerMode === "agent" ? "agent" : "send")}<span>Открыть</span></button>
    </form>
  `;
}

function renderMainSection(state: ClientState): string {
  if (state.section === "agent") {
    return renderAgentSection(state);
  }
  if (state.section === "apps") {
    return renderAppsSection(state);
  }
  if (state.section === "pwa") {
    return renderPwaSection(state);
  }
  return renderTimelineSection(state);
}

function renderTimelineSection(state: ClientState): string {
  if (state.timeline.length === 0) {
    return `
      <div class="soty-client-zero">
        ${icon("mail")}
        <b>Поток чистый</b>
        <span>${escapeHtml(shortTunnelId(state.selectedId))}</span>
      </div>
    `;
  }
  return `
    <div class="soty-client-timeline">
      ${state.timeline.map((line) => `
        <article class="${line.mine ? "is-mine" : ""}">
          <span>${line.mine ? icon("person") : icon("hexagon")}</span>
          <p>${escapeHtml(line.text)}</p>
        </article>
      `).join("")}
    </div>
  `;
}

function renderAgentSection(state: ClientState): string {
  const lines = state.agentLines.slice(-8);
  return `
    <div class="soty-client-agent-panel">
      <div class="soty-client-agent-state${state.agentMode ? " is-on" : ""}">
        ${icon("agent")}
        <span>
          <b>${state.agentMode ? "Агент включен" : "Агент выключен"}</b>
          <small>${escapeHtml(state.selected ? tunnelLabel(state.selected) : "нет ячейки")}</small>
        </span>
        <button type="button" data-client-toggle-agent>${state.agentMode ? "Выключить" : "Включить"}</button>
      </div>
      <div class="soty-client-agent-log">
        ${lines.length
          ? lines.map(renderAgentLine).join("")
          : `<div class="soty-client-zero">${icon("agent")}<b>Нет приватного лога</b><span>${escapeHtml(shortTunnelId(state.selectedId))}</span></div>`}
      </div>
    </div>
  `;
}

function renderAgentLine(line: ClientAgentLine): string {
  return `
    <article class="${line.role === "user" ? "is-mine" : ""}">
      <span>${icon(line.role === "user" ? "person" : "agent")}</span>
      <p>${escapeHtml(line.text)}</p>
      <time>${escapeHtml(shortDate(line.createdAt))}</time>
    </article>
  `;
}

function renderAppsSection(state: ClientState): string {
  const apps = state.miniApps.slice(0, 12);
  if (apps.length === 0) {
    return `<div class="soty-client-zero">${icon("apps")}<b>Apps пусто</b><span>0</span></div>`;
  }
  return `
    <div class="soty-client-apps">
      ${apps.map((appItem) => `
        <button type="button" data-client-open="chat">
          ${icon("apps")}
          <span>
            <b>${escapeHtml(appItem.title)}</b>
            <small>${escapeHtml([appItem.scope, appItem.visibility].filter(Boolean).join(" / "))}</small>
          </span>
        </button>
      `).join("")}
    </div>
  `;
}

function renderPwaSection(state: ClientState): string {
  return `
    <div class="soty-client-pwa">
      <div>
        ${icon("install")}
        <span>
          <b>${state.standalone ? "Standalone" : "Browser"}</b>
          <small>${state.selected ? escapeHtml(tunnelLabel(state.selected)) : "Soty"}</small>
        </span>
      </div>
      <button type="button" data-client-open="pwa">${icon("expand")}<span>PWA</span></button>
      <button type="button" data-client-open="classic">${icon("hexagon")}<span>Soty</span></button>
    </div>
  `;
}

function renderComposerMode(mode: ClientComposerMode, label: string, state: ClientState): string {
  return `
    <button type="button" data-client-composer-mode="${escapeAttr(mode)}" aria-pressed="${state.composerMode === mode ? "true" : "false"}">${escapeHtml(label)}</button>
  `;
}

function renderEmptyWorkspace(): string {
  return `
    <div class="soty-client-empty-workspace">
      ${icon("hexagon")}
      <b>Soty</b>
      <span>Нет ячеек</span>
      <button type="button" data-client-open="classic">${icon("expand")}<span>Открыть</span></button>
    </div>
  `;
}

function renderInspector(state: ClientState): string {
  if (!state.selected) {
    return `
      <section class="soty-client-inspector">
        <h2>Контекст</h2>
        <div class="soty-client-inspector-empty">${icon("hexagon")}<span>Пусто</span></div>
      </section>
    `;
  }
  return `
    <section class="soty-client-inspector">
      <header>
        <h2>${escapeHtml(tunnelLabel(state.selected))}</h2>
        <small>${escapeHtml(shortTunnelId(state.selected.id))}</small>
      </header>
      <dl>
        <div><dt>Агент</dt><dd>${state.agentMode ? "on" : "off"}</dd></div>
        <div><dt>PWA</dt><dd>${state.standalone ? "standalone" : "browser"}</dd></div>
        <div><dt>Apps</dt><dd>${escapeHtml(String(state.miniApps.length))}</dd></div>
        <div><dt>Обновлено</dt><dd>${escapeHtml(shortDate(state.selected.updatedAt))}</dd></div>
      </dl>
      <div class="soty-client-inspector-actions">
        <button type="button" data-client-open="chat">${icon("mail")}<span>Поток</span></button>
        <button type="button" data-client-open="agent">${icon("agent")}<span>Агент</span></button>
        <button type="button" data-client-open="pwa">${icon("install")}<span>PWA</span></button>
      </div>
      <section class="soty-client-upstream">
        <b>Element</b>
        <span>upstream</span>
      </section>
    </section>
  `;
}

function bindClient(
  root: HTMLElement,
  stateFor: () => ClientState,
  handlers: {
    readonly setQuery: (value: string) => void;
    readonly setSection: (value: ClientSection) => void;
    readonly setComposerMode: (value: ClientComposerMode) => void;
    readonly repaint: () => void;
  }
): void {
  root.addEventListener("input", (event) => {
    const input = event.target instanceof HTMLInputElement ? event.target : null;
    if (input?.matches("[data-client-search]")) {
      handlers.setQuery(input.value.slice(0, 80));
    }
  });
  root.addEventListener("click", (event) => {
    const button = event.target instanceof Element ? event.target.closest<HTMLButtonElement>("button") : null;
    if (!button || !root.contains(button)) {
      return;
    }
    const selected = button.dataset.clientSelect || "";
    if (selected) {
      saveSelectedTunnelId(selected);
      handlers.repaint();
      return;
    }
    const section = clientSection(button.dataset.clientSection || "");
    if (section) {
      handlers.setSection(section);
      return;
    }
    const mode = clientComposerMode(button.dataset.clientComposerMode || "");
    if (mode) {
      handlers.setComposerMode(mode);
      return;
    }
    const state = stateFor();
    if (button.hasAttribute("data-client-toggle-agent")) {
      if (state.selectedId) {
        writeAgentMode(state.selectedId, !state.agentMode);
        handlers.setSection("agent");
      }
      return;
    }
    const target = button.dataset.clientOpen || "";
    if (target) {
      openClientTarget(target, state);
    }
  });
  root.addEventListener("submit", (event) => {
    const form = event.target instanceof HTMLFormElement ? event.target : null;
    if (!form?.matches(".soty-client-compose")) {
      return;
    }
    const state = stateFor();
    if (state.composerMode === "agent" && state.selectedId) {
      writeAgentMode(state.selectedId, true);
    }
    event.preventDefault();
    openClientTarget(state.composerMode === "agent" ? "agent" : "chat", state);
  });
}

function openClientTarget(target: string, state: ClientState): void {
  if (state.selectedId) {
    saveSelectedTunnelId(state.selectedId);
  }
  if (target === "agent" && state.selectedId) {
    writeAgentMode(state.selectedId, true);
  }
  if (target === "classic") {
    window.location.assign("/?pwa=1");
    return;
  }
  if (target === "pwa") {
    window.location.assign(state.selectedId ? `/?pwa=1&bare=1&chat=${encodeURIComponent(state.selectedId)}` : "/?pwa=1&bare=1");
    return;
  }
  window.location.assign(state.selectedId ? `/?pwa=1&chat=${encodeURIComponent(state.selectedId)}` : "/?pwa=1");
}

function selectedTunnelId(tunnels: readonly TunnelRecord[]): string {
  const stored = loadSelectedTunnelId() || "";
  if (stored && tunnels.some((tunnel) => tunnel.id === stored)) {
    return stored;
  }
  return tunnels[0]?.id || "";
}

function sortTunnels(tunnels: readonly TunnelRecord[]): readonly TunnelRecord[] {
  return [...tunnels].sort((left, right) => timestamp(right) - timestamp(left) || tunnelLabel(left).localeCompare(tunnelLabel(right)));
}

function loadTimeline(tunnelId: string): readonly ClientTimelineLine[] {
  const snapshots = readRecord(localStorage.getItem(textSnapshotsKey));
  const raw = typeof snapshots[tunnelId] === "string" ? snapshots[tunnelId] : "";
  return raw
    .split(/\r?\n/u)
    .map((line) => line.trim())
    .filter(Boolean)
    .slice(-14)
    .map((line, index) => ({
      id: `${tunnelId}:${index}`,
      text: line,
      mine: /^(?:я|me|you)\b|:\s/u.test(line.toLowerCase())
    }));
}

function loadAgentLines(tunnelId: string): readonly ClientAgentLine[] {
  const logs = readRecord(localStorage.getItem(agentPrivateLogKey));
  const raw = Array.isArray(logs[tunnelId]) ? logs[tunnelId] : [];
  return raw
    .map(normalizeAgentLine)
    .filter((line): line is ClientAgentLine => Boolean(line))
    .slice(-20);
}

function normalizeAgentLine(value: unknown): ClientAgentLine | null {
  if (!isRecord(value)) {
    return null;
  }
  const text = cleanText(value.text, 1200);
  if (!text) {
    return null;
  }
  return {
    role: value.role === "agent" ? "agent" : "user",
    text,
    createdAt: cleanText(value.createdAt, 80) || new Date().toISOString()
  };
}

function loadClientMiniApps(): readonly ClientMiniApp[] {
  const parsed = readRecord(localStorage.getItem(miniAppsRegistryKey));
  const items = Array.isArray(parsed.apps) ? parsed.apps : [];
  return items
    .map((item) => {
      if (!isRecord(item)) {
        return null;
      }
      const id = cleanText(item.id, 80);
      const title = cleanText(item.title, 80) || id;
      if (!id || !title) {
        return null;
      }
      return {
        id,
        title,
        summary: cleanText(item.summary, 140),
        scope: cleanText(item.scope, 40) || "account",
        visibility: cleanText(item.visibility, 40) || "private"
      };
    })
    .filter((item): item is ClientMiniApp => Boolean(item))
    .slice(0, 40);
}

function loadAgentModes(): Map<string, boolean> {
  const record = readRecord(localStorage.getItem(agentModeKey));
  return new Map(Object.entries(record).filter(([, value]) => value === true).map(([key]) => [key, true]));
}

function writeAgentMode(tunnelId: string, enabled: boolean): void {
  const modes = loadAgentModes();
  if (enabled) {
    modes.set(tunnelId, true);
  } else {
    modes.delete(tunnelId);
  }
  localStorage.setItem(agentModeKey, JSON.stringify(Object.fromEntries(modes)));
}

function clientSection(value: string): ClientSection | "" {
  return value === "messages" || value === "agent" || value === "apps" || value === "pwa" ? value : "";
}

function clientComposerMode(value: string): ClientComposerMode | "" {
  return value === "message" || value === "agent" || value === "command" ? value : "";
}

function composerPlaceholder(state: ClientState): string {
  if (state.composerMode === "agent") {
    return state.agentMode ? "задача агенту" : "включить агента и отправить";
  }
  if (state.composerMode === "command") {
    return "команда";
  }
  return "сообщение";
}

function tunnelLabel(tunnel: TunnelRecord): string {
  if (tunnel.self) {
    return "Я";
  }
  if (tunnel.agent) {
    return "Агент";
  }
  return cleanText(tunnel.label, 80) || shortTunnelId(tunnel.id);
}

function tunnelMeta(tunnel: TunnelRecord, agentOn: boolean): string {
  const parts = [
    tunnel.self ? "self" : tunnel.counterparty ? "contact" : "cell",
    agentOn ? "agent" : "",
    shortDate(tunnel.lastActionAt || tunnel.updatedAt)
  ].filter(Boolean);
  return parts.join(" / ");
}

function tunnelInitial(tunnel: TunnelRecord): string {
  const label = tunnelLabel(tunnel);
  return Array.from(label)[0]?.toUpperCase() || "S";
}

function tunnelColor(tunnel: TunnelRecord): string {
  return /^#[0-9a-f]{6}$/iu.test(tunnel.color || "") ? tunnel.color || "#8af0a2" : "#8af0a2";
}

function shortTunnelId(value: string): string {
  return value ? value.slice(0, 8) : "none";
}

function shortDate(value: string): string {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) {
    return "";
  }
  return new Intl.DateTimeFormat("ru", {
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  }).format(date);
}

function timestamp(tunnel: TunnelRecord): number {
  const date = new Date(tunnel.lastActionAt || tunnel.updatedAt || tunnel.createdAt);
  return Number.isFinite(date.getTime()) ? date.getTime() : 0;
}

function isStandalonePwa(): boolean {
  const navigatorWithStandalone = navigator as Navigator & { readonly standalone?: boolean };
  return window.matchMedia("(display-mode: standalone)").matches || navigatorWithStandalone.standalone === true;
}

function readRecord(raw: string | null): Record<string, unknown> {
  if (!raw) {
    return {};
  }
  try {
    const parsed = JSON.parse(raw) as unknown;
    return isRecord(parsed) ? parsed : {};
  } catch {
    return {};
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function cleanText(value: unknown, max: number): string {
  return String(typeof value === "string" || typeof value === "number" ? value : "")
    .replace(/\s+/gu, " ")
    .trim()
    .slice(0, max);
}

function normalizeSearch(value: string): string {
  return value.toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;");
}

function escapeAttr(value: string): string {
  return escapeHtml(value).replace(/'/gu, "&#39;");
}
