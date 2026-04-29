export interface LocalAgentStatus {
  readonly ok: boolean;
  readonly managed?: boolean;
  readonly platform?: string;
  readonly shell?: string;
  readonly version?: string;
}

export function checkLocalAgent(timeoutMs = 850): Promise<LocalAgentStatus> {
  return checkLocalAgentHttp(timeoutMs);
}

async function checkLocalAgentHttp(timeoutMs: number): Promise<LocalAgentStatus> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:49424/health", {
      cache: "no-store",
      signal: controller.signal
    });
    if (!response.ok) {
      return { ok: false };
    }
    const message = await response.json() as {
      readonly managed?: boolean;
      readonly platform?: string;
      readonly shell?: string;
      readonly version?: string;
    };
    return {
      ok: true,
      ...(typeof message.managed === "boolean" ? { managed: message.managed } : {}),
      ...(typeof message.platform === "string" ? { platform: message.platform } : {}),
      ...(typeof message.shell === "string" ? { shell: message.shell } : {}),
      ...(typeof message.version === "string" ? { version: message.version } : {})
    };
  } catch {
    return { ok: false };
  } finally {
    window.clearTimeout(timer);
  }
}

export function agentInstallUrl(): string {
  const nav = navigator as Navigator & { readonly userAgentData?: { readonly platform?: string } };
  const platform = `${nav.userAgentData?.platform || navigator.platform || navigator.userAgent}`.toLowerCase();
  return platform.includes("win")
    ? "/agent/install-windows.ps1"
    : "/agent/install-macos-linux.sh";
}

export function downloadAgentInstaller(): void {
  const link = document.createElement("a");
  const url = agentInstallUrl();
  link.href = url;
  link.download = url.endsWith(".ps1") ? "install-soty-agent.ps1" : "install-soty-agent.sh";
  document.body.append(link);
  link.click();
  link.remove();
}
