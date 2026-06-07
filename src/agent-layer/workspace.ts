import { icon } from "../icons";
import type { AgentProviderKind, AgentRunEnvelope, AgentRisk, AgentSurface, AgentTargetKind, AgentOperatingSystem, AgentShell } from "./core";
import { createAgentRunEnvelope, cleanText, cleanToken, validateAgentRunEnvelope } from "./core";
import type { AgentStyleConfig } from "./core";
import { agentStylePacks, applyAgentStyleVars, normalizeAgentStyleConfig } from "./styles";

export interface AgentWorkspaceProviderOption {
  readonly id: string;
  readonly kind: AgentProviderKind;
  readonly label: string;
  readonly summary: string;
  readonly capabilities?: readonly string[];
}

export interface AgentWorkspaceMode {
  readonly id: string;
  readonly label: string;
  readonly intentPrefix?: string;
  readonly placeholder?: string;
}

export interface AgentWorkspaceContextItem {
  readonly label: string;
  readonly value: string;
}

export interface AgentWorkspaceRunEvent {
  readonly kind: "status" | "message" | "terminal" | "proof";
  readonly text: string;
}

export interface AgentWorkspaceRunRequest {
  readonly envelope: AgentRunEnvelope;
  readonly text: string;
  readonly mode: AgentWorkspaceMode;
  readonly provider: AgentWorkspaceProviderOption;
  readonly style: AgentStyleConfig;
  readonly signal: AbortSignal;
  readonly emit: (event: AgentWorkspaceRunEvent) => void;
}

export interface AgentWorkspaceRunResult {
  readonly ok: boolean;
  readonly message: string;
  readonly reply: string;
  readonly proof?: readonly string[];
}

export interface AgentWorkspaceOptions {
  readonly title: string;
  readonly subtitle?: string;
  readonly userId: string;
  readonly projectId?: string;
  readonly surface: AgentSurface;
  readonly providers: readonly AgentWorkspaceProviderOption[];
  readonly modes?: readonly AgentWorkspaceMode[];
  readonly defaultProviderId?: string;
  readonly defaultModeId?: string;
  readonly defaultStyle?: Partial<AgentStyleConfig>;
  readonly contextItems?: readonly AgentWorkspaceContextItem[];
  readonly initialText?: string;
  readonly onRun: (request: AgentWorkspaceRunRequest) => Promise<AgentWorkspaceRunResult>;
  readonly onResult?: (result: AgentWorkspaceRunResult, request: AgentWorkspaceRunRequest) => Promise<void> | void;
  readonly onClose?: () => void;
}

export interface AgentWorkspaceController {
  readonly element: HTMLElement;
  close(): void;
}

const targetKinds: readonly AgentTargetKind[] = ["unknown", "browser", "server", "ssh", "windows-device", "mac-device", "linux-device", "container", "miniapp"];
const operatingSystems: readonly AgentOperatingSystem[] = ["unknown", "windows", "macos", "linux"];
const shells: readonly AgentShell[] = ["unknown", "powershell", "cmd", "bash", "zsh", "sh"];
const risks: readonly AgentRisk[] = ["low", "medium", "high"];
const fallbackProvider: AgentWorkspaceProviderOption = {
  id: "soty-codex",
  kind: "soty-codex",
  label: "Soty Codex",
  summary: "Server Codex executor.",
  capabilities: ["computer"]
};
const fallbackMode: AgentWorkspaceMode = {
  id: "general",
  label: "General",
  placeholder: "Describe the result you want."
};

export function showAgentWorkspace(mount: HTMLElement, options: AgentWorkspaceOptions): AgentWorkspaceController {
  const providers = normalizeProviders(options.providers);
  const modes = normalizeModes(options.modes);
  const overlay = document.createElement("div");
  overlay.className = "agent-workspace-overlay";
  const initialStyle = normalizeAgentStyleConfig(options.defaultStyle);
  overlay.innerHTML = workspaceHtml(options, providers, modes, initialStyle);
  mount.append(overlay);

  const panel = overlay.querySelector<HTMLElement>("[data-agent-workspace]");
  const textInput = overlay.querySelector<HTMLTextAreaElement>("[data-agent-intent]");
  const providerInput = overlay.querySelector<HTMLSelectElement>("[data-agent-provider]");
  const modeInput = overlay.querySelector<HTMLSelectElement>("[data-agent-mode]");
  const targetInput = overlay.querySelector<HTMLSelectElement>("[data-agent-target]");
  const osInput = overlay.querySelector<HTMLSelectElement>("[data-agent-os]");
  const shellInput = overlay.querySelector<HTMLSelectElement>("[data-agent-shell]");
  const riskInput = overlay.querySelector<HTMLSelectElement>("[data-agent-risk]");
  const styleInput = overlay.querySelector<HTMLSelectElement>("[data-agent-style]");
  const runButton = overlay.querySelector<HTMLButtonElement>("[data-agent-run]");
  const stopButton = overlay.querySelector<HTMLButtonElement>("[data-agent-stop]");
  const copyButton = overlay.querySelector<HTMLButtonElement>("[data-agent-copy]");
  const status = overlay.querySelector<HTMLElement>("[data-agent-status]");
  const eventLog = overlay.querySelector<HTMLElement>("[data-agent-events]");
  const reply = overlay.querySelector<HTMLElement>("[data-agent-reply]");
  const proof = overlay.querySelector<HTMLElement>("[data-agent-proof]");

  if (panel) {
    applyAgentStyleVars(panel, initialStyle);
  }
  textInput?.focus();

  let abortController: AbortController | null = null;
  let lastReply = "";

  const close = () => {
    abortController?.abort();
    overlay.remove();
    options.onClose?.();
  };

  overlay.querySelector<HTMLElement>("[data-agent-close]")?.addEventListener("click", close);
  overlay.addEventListener("click", (event) => {
    if (event.target === overlay) {
      close();
    }
  });
  styleInput?.addEventListener("change", () => {
    if (panel) {
      applyAgentStyleVars(panel, { themeId: styleInput.value });
    }
  });
  modeInput?.addEventListener("change", () => {
    const mode = modes.find((item) => item.id === modeInput.value) || firstMode(modes);
    if (textInput) {
      textInput.placeholder = mode.placeholder || "Опишите результат.";
    }
  });
  stopButton?.addEventListener("click", () => {
    abortController?.abort();
    appendEvent(eventLog, { kind: "status", text: "Остановка запрошена" });
    setStatus(status, "Останавливаю...");
  });
  copyButton?.addEventListener("click", () => {
    if (lastReply) {
      void navigator.clipboard?.writeText(lastReply).catch(() => undefined);
    }
  });
  overlay.querySelector<HTMLFormElement>("[data-agent-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    void run();
  });

  async function run(): Promise<void> {
    const provider = providers.find((item) => item.id === providerInput?.value) || firstProvider(providers);
    const mode = modes.find((item) => item.id === modeInput?.value) || firstMode(modes);
    const text = cleanText(textInput?.value || "", 4000);
    if (!text) {
      setStatus(status, "Напишите задачу.");
      textInput?.focus();
      return;
    }
    abortController?.abort();
    abortController = new AbortController();
    clearNode(eventLog);
    clearNode(proof);
    if (reply) {
      reply.hidden = true;
      reply.textContent = "";
    }
    const style = normalizeAgentStyleConfig({ themeId: styleInput?.value || initialStyle.themeId });
    const intent = [mode.intentPrefix, text].filter(Boolean).join("\n\n");
    const envelope = createAgentRunEnvelope({
      userId: options.userId,
      ...(options.projectId ? { projectId: options.projectId } : {}),
      surface: options.surface,
      provider: provider.kind,
      target: {
        kind: targetValue(targetInput?.value),
        os: osValue(osInput?.value),
        shell: shellValue(shellInput?.value)
      },
      intent,
      capabilities: provider.capabilities || [],
      risk: riskValue(riskInput?.value),
      style,
      context: contextRecord(options.contextItems || []),
      timeoutMs: 2 * 60 * 60_000
    });
    const errors = validateAgentRunEnvelope(envelope);
    if (errors.length > 0) {
      for (const error of errors) {
        appendEvent(eventLog, { kind: "status", text: error });
      }
      setStatus(status, "Нужна точная цель.");
      return;
    }
    const request: AgentWorkspaceRunRequest = {
      envelope,
      text,
      mode,
      provider,
      style,
      signal: abortController.signal,
      emit: (item) => appendEvent(eventLog, item)
    };
    if (runButton) {
      runButton.disabled = true;
    }
    if (stopButton) {
      stopButton.disabled = false;
    }
    setStatus(status, "В работе...");
    appendEvent(eventLog, { kind: "status", text: `${provider.label}: run started` });
    try {
      const result = await options.onRun(request);
      lastReply = result.reply;
      if (reply) {
        reply.hidden = false;
        reply.textContent = result.reply || result.message;
      }
      for (const item of result.proof || []) {
        appendProof(proof, item);
      }
      setStatus(status, result.ok ? "Готово" : result.message || "Ошибка");
      await options.onResult?.(result, request);
    } catch (error) {
      const message = abortController.signal.aborted ? "Остановлено." : error instanceof Error ? error.message : "Agent недоступен.";
      lastReply = message;
      if (reply) {
        reply.hidden = false;
        reply.textContent = message;
      }
      setStatus(status, message);
    } finally {
      if (runButton && overlay.isConnected) {
        runButton.disabled = false;
      }
      if (stopButton && overlay.isConnected) {
        stopButton.disabled = true;
      }
    }
  }

  return { element: overlay, close };
}

function workspaceHtml(
  options: AgentWorkspaceOptions,
  providers: readonly AgentWorkspaceProviderOption[],
  modes: readonly AgentWorkspaceMode[],
  style: AgentStyleConfig
): string {
  const defaultProvider = firstProvider(providers);
  const defaultMode = modes.find((mode) => mode.id === options.defaultModeId) || firstMode(modes);
  return `
    <section class="agent-workspace" data-agent-workspace role="dialog" aria-modal="true" aria-label="${escapeHtml(options.title)}">
      <header class="agent-workspace-header">
        <div class="agent-workspace-mark">${icon("agent")}</div>
        <div>
          <h2>${escapeHtml(options.title)}</h2>
          <p>${escapeHtml(options.subtitle || "Universal agent workspace")}</p>
        </div>
        <button class="agent-workspace-icon-button" type="button" data-agent-close data-tooltip="Закрыть">${icon("close")}</button>
      </header>
      <form class="agent-workspace-grid" data-agent-form>
        <aside class="agent-workspace-sidebar">
          <label>Агент
            <select data-agent-provider>${providers.map((provider) => optionHtml(provider.id, provider.label, provider.id === (options.defaultProviderId || defaultProvider.id))).join("")}</select>
          </label>
          <label>Режим
            <select data-agent-mode>${modes.map((mode) => optionHtml(mode.id, mode.label, mode.id === (options.defaultModeId || defaultMode.id))).join("")}</select>
          </label>
          <label>Тема
            <select data-agent-style>${agentStylePacks.map((pack) => optionHtml(pack.id, pack.label, pack.id === style.themeId)).join("")}</select>
          </label>
          <label>Цель
            <select data-agent-target>${targetKinds.map((kind) => optionHtml(kind, targetLabel(kind), kind === "unknown")).join("")}</select>
          </label>
          <div class="agent-workspace-pair">
            <label>ОС
              <select data-agent-os>${operatingSystems.map((os) => optionHtml(os, osLabel(os), os === "unknown")).join("")}</select>
            </label>
            <label>Shell
              <select data-agent-shell>${shells.map((shell) => optionHtml(shell, shellLabel(shell), shell === "unknown")).join("")}</select>
            </label>
          </div>
          <label>Риск
            <select data-agent-risk>${risks.map((risk) => optionHtml(risk, riskLabel(risk), risk === "low")).join("")}</select>
          </label>
          <div class="agent-workspace-context">
            ${(options.contextItems || []).map((item) => `
              <span>${escapeHtml(item.label)}</span>
              <b>${escapeHtml(item.value)}</b>
            `).join("")}
          </div>
        </aside>
        <main class="agent-workspace-main">
          <textarea data-agent-intent maxlength="4000" required placeholder="${escapeHtml(defaultMode.placeholder || "Опишите результат.")}">${escapeHtml(options.initialText || "")}</textarea>
          <div class="agent-workspace-toolbar">
            <span data-agent-status>Готов</span>
            <button class="agent-workspace-secondary" type="button" data-agent-copy>${icon("copy")} <span>Копировать</span></button>
            <button class="agent-workspace-danger" type="button" data-agent-stop disabled>${icon("stop")} <span>Стоп</span></button>
            <button class="agent-workspace-primary" type="submit" data-agent-run>${icon("send")} <span>Запустить</span></button>
          </div>
          <div class="agent-workspace-output">
            <section>
              <h3>Ход</h3>
              <div class="agent-workspace-events" data-agent-events></div>
            </section>
            <section>
              <h3>Ответ</h3>
              <div class="agent-workspace-reply" data-agent-reply hidden></div>
              <div class="agent-workspace-proof" data-agent-proof></div>
            </section>
          </div>
        </main>
      </form>
    </section>
  `;
}

function normalizeProviders(providers: readonly AgentWorkspaceProviderOption[]): readonly AgentWorkspaceProviderOption[] {
  const cleaned = providers
    .map((provider) => ({
      id: cleanToken(provider.id, 80),
      kind: provider.kind,
      label: cleanText(provider.label, 80),
      summary: cleanText(provider.summary, 180),
      ...(provider.capabilities ? { capabilities: provider.capabilities.map((item) => cleanToken(item, 80)).filter(Boolean).slice(0, 32) } : {})
    }))
    .filter((provider) => provider.id && provider.label);
  return cleaned.length > 0 ? cleaned : [fallbackProvider];
}

function normalizeModes(modes: readonly AgentWorkspaceMode[] | undefined): readonly AgentWorkspaceMode[] {
  const cleaned = (modes || [])
    .map((mode) => ({
      id: cleanToken(mode.id, 60),
      label: cleanText(mode.label, 80),
      ...(mode.intentPrefix ? { intentPrefix: cleanText(mode.intentPrefix, 500) } : {}),
      ...(mode.placeholder ? { placeholder: cleanText(mode.placeholder, 180) } : {})
    }))
    .filter((mode) => mode.id && mode.label);
  return cleaned.length > 0 ? cleaned : [fallbackMode];
}

function firstProvider(providers: readonly AgentWorkspaceProviderOption[]): AgentWorkspaceProviderOption {
  return providers[0] || fallbackProvider;
}

function firstMode(modes: readonly AgentWorkspaceMode[]): AgentWorkspaceMode {
  return modes[0] || fallbackMode;
}

function appendEvent(container: HTMLElement | null, event: AgentWorkspaceRunEvent): void {
  if (!container) {
    return;
  }
  const line = document.createElement("p");
  line.dataset.kind = event.kind;
  line.textContent = event.text;
  container.append(line);
  container.scrollTop = container.scrollHeight;
}

function appendProof(container: HTMLElement | null, text: string): void {
  if (!container || !text) {
    return;
  }
  const item = document.createElement("span");
  item.textContent = text;
  container.append(item);
}

function clearNode(node: HTMLElement | null): void {
  if (node) {
    node.textContent = "";
  }
}

function setStatus(node: HTMLElement | null, value: string): void {
  if (node) {
    node.textContent = value;
  }
}

function contextRecord(items: readonly AgentWorkspaceContextItem[]): Readonly<Record<string, string>> {
  const record: Record<string, string> = {};
  for (const item of items.slice(0, 24)) {
    const key = cleanToken(item.label, 60);
    const value = cleanText(item.value, 300);
    if (key && value) {
      record[key] = value;
    }
  }
  return record;
}

function optionHtml(value: string, label: string, selected: boolean): string {
  return `<option value="${escapeHtml(value)}"${selected ? " selected" : ""}>${escapeHtml(label)}</option>`;
}

function targetValue(value: string | undefined): AgentTargetKind {
  return targetKinds.includes(value as AgentTargetKind) ? value as AgentTargetKind : "unknown";
}

function osValue(value: string | undefined): AgentOperatingSystem {
  return operatingSystems.includes(value as AgentOperatingSystem) ? value as AgentOperatingSystem : "unknown";
}

function shellValue(value: string | undefined): AgentShell {
  return shells.includes(value as AgentShell) ? value as AgentShell : "unknown";
}

function riskValue(value: string | undefined): AgentRisk {
  return value === "medium" || value === "high" ? value : "low";
}

function targetLabel(value: AgentTargetKind): string {
  if (value === "unknown") {
    return "Авто/чтение";
  }
  const labels: Record<AgentTargetKind, string> = {
    unknown: "Авто/чтение",
    browser: "Браузер",
    server: "Сервер",
    ssh: "SSH",
    "windows-device": "Windows",
    "mac-device": "macOS",
    "linux-device": "Linux",
    container: "Контейнер",
    miniapp: "Mini-app"
  };
  return labels[value];
}

function osLabel(value: AgentOperatingSystem): string {
  return value === "macos" ? "macOS" : value === "unknown" ? "Авто" : value;
}

function shellLabel(value: AgentShell): string {
  return value === "unknown" ? "Авто" : value;
}

function riskLabel(value: AgentRisk): string {
  if (value === "high") {
    return "Высокий";
  }
  if (value === "medium") {
    return "Средний";
  }
  return "Низкий";
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/gu, "&amp;")
    .replace(/</gu, "&lt;")
    .replace(/>/gu, "&gt;")
    .replace(/"/gu, "&quot;");
}
