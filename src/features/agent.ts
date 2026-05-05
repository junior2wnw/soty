export interface LocalAgentStatus {
  readonly ok: boolean;
  readonly managed?: boolean;
  readonly scope?: string;
  readonly platform?: string;
  readonly shell?: string;
  readonly version?: string;
  readonly windowsUser?: string;
  readonly system?: boolean;
  readonly maintenance?: boolean;
}

export interface LocalAgentReply {
  readonly ok: boolean;
  readonly text: string;
  readonly exitCode?: number;
}

export function checkLocalAgent(timeoutMs = 850): Promise<LocalAgentStatus> {
  return checkLocalAgentHttp(timeoutMs);
}

export async function askLocalAgentReply(
  text: string,
  context: string,
  timeoutMs = 130_000
): Promise<LocalAgentReply> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:49424/agent/reply", {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text, context }),
      signal: controller.signal,
      targetAddressSpace: "loopback"
    } as RequestInit & { readonly targetAddressSpace: "loopback" });
    const payload = await response.json() as {
      readonly ok?: boolean;
      readonly text?: string;
      readonly exitCode?: number;
    };
    return {
      ok: Boolean(payload.ok && response.ok),
      text: typeof payload.text === "string" ? payload.text : "",
      ...(typeof payload.exitCode === "number" ? { exitCode: payload.exitCode } : {})
    };
  } catch {
    return {
      ok: false,
      text: "Сообщение отправлено, но браузер пока не смог достучаться до локального агента. Проверь разрешение на локальную сеть для Сот и что Soty Agent запущен.",
      exitCode: 127
    };
  } finally {
    window.clearTimeout(timer);
  }
}

async function checkLocalAgentHttp(timeoutMs: number): Promise<LocalAgentStatus> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:49424/health", {
      cache: "no-store",
      signal: controller.signal,
      targetAddressSpace: "loopback"
    } as RequestInit & { readonly targetAddressSpace: "loopback" });
    if (!response.ok) {
      return { ok: false };
    }
    const message = await response.json() as {
      readonly managed?: boolean;
      readonly scope?: string;
      readonly platform?: string;
      readonly shell?: string;
      readonly version?: string;
      readonly windowsUser?: string;
      readonly system?: boolean;
      readonly maintenance?: boolean;
    };
    return {
      ok: true,
      ...(typeof message.managed === "boolean" ? { managed: message.managed } : {}),
      ...(typeof message.scope === "string" ? { scope: message.scope } : {}),
      ...(typeof message.platform === "string" ? { platform: message.platform } : {}),
      ...(typeof message.shell === "string" ? { shell: message.shell } : {}),
      ...(typeof message.version === "string" ? { version: message.version } : {}),
      ...(typeof message.windowsUser === "string" ? { windowsUser: message.windowsUser } : {}),
      ...(typeof message.system === "boolean" ? { system: message.system } : {}),
      ...(typeof message.maintenance === "boolean" ? { maintenance: message.maintenance } : {})
    };
  } catch {
    return { ok: false };
  } finally {
    window.clearTimeout(timer);
  }
}

export function isWindowsPlatform(): boolean {
  const nav = navigator as Navigator & { readonly userAgentData?: { readonly platform?: string } };
  const platform = `${nav.userAgentData?.platform || navigator.platform || navigator.userAgent}`.toLowerCase();
  return platform.includes("win");
}

export function agentInstallUrl(scope: "user" | "machine" = "user"): string {
  return isWindowsPlatform()
    ? (scope === "machine" ? "/agent/install-windows-machine.cmd" : "/agent/install-windows.cmd")
    : "/agent/install-macos-linux.sh";
}

export function downloadAgentInstaller(scope: "user" | "machine" = "user"): void {
  const link = document.createElement("a");
  const url = agentInstallUrl(scope);
  link.href = url;
  link.download = url.endsWith(".cmd")
    ? (scope === "machine" ? "install-soty-agent-machine.cmd" : "install-soty-agent.cmd")
    : "install-soty-agent.sh";
  document.body.append(link);
  link.click();
  link.remove();
}
