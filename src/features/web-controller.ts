export type WebControllerTargetRef = string | {
  readonly tunnelId?: string;
  readonly id?: string;
  readonly label?: string;
  readonly hostDeviceId?: string;
  readonly target?: string;
};

export interface WebControllerTargetInfo {
  readonly tunnelId: string;
  readonly label: string;
  readonly hostDeviceId: string;
  readonly deviceIds: readonly string[];
  readonly selected: boolean;
  readonly syncState: "open" | "closed" | "connecting";
  readonly terminalState: "idle" | "run" | "ok" | "bad" | "off";
}

export interface WebControllerRunOptions {
  readonly target?: WebControllerTargetRef;
  readonly timeoutMs?: number;
  readonly runAs?: string;
  readonly shell?: string;
  readonly name?: string;
}

export type WebControllerCommandInput = string | (WebControllerRunOptions & {
  readonly command?: string;
  readonly script?: string;
  readonly text?: string;
});

export interface WebControllerRunRequest {
  readonly kind: "run" | "script";
  readonly body: string;
  readonly options: WebControllerRunOptions;
}

export interface WebControllerRunResult {
  readonly ok: boolean;
  readonly tunnelId: string;
  readonly label: string;
  readonly hostDeviceId: string;
  readonly commandId: string;
  readonly text: string;
  readonly exitCode: number;
  readonly startedAt: string;
  readonly finishedAt: string;
  readonly timedOut?: boolean;
}

export interface WebControllerPending {
  readonly tunnelId: string;
  readonly label: string;
  readonly hostDeviceId: string;
  readonly commandId: string;
  readonly startedAt: string;
  readonly timer: number;
  readonly chunks: string[];
  readonly resolve: (result: WebControllerRunResult) => void;
}

export interface WebControllerStatus {
  readonly schema: "soty.web-controller.status.v1";
  readonly deviceId: string;
  readonly deviceNick: string;
  readonly localAgentOk: boolean;
  readonly targets: WebControllerTargetInfo[];
  readonly pending: readonly string[];
}

export interface WebControllerApi {
  readonly schema: "soty.web-controller.v1";
  readonly targets: () => WebControllerTargetInfo[];
  readonly list: () => WebControllerTargetInfo[];
  readonly status: () => WebControllerStatus;
  readonly select: (target?: WebControllerTargetRef) => WebControllerTargetInfo;
  readonly run: (input: WebControllerCommandInput, second?: string | WebControllerRunOptions, third?: WebControllerRunOptions) => Promise<WebControllerRunResult>;
  readonly script: (input: WebControllerCommandInput, second?: string | WebControllerRunOptions, third?: WebControllerRunOptions) => Promise<WebControllerRunResult>;
  readonly cancel: (commandId: string) => boolean;
  readonly tail: (target?: WebControllerTargetRef, lines?: number) => string[];
}

export interface WebControllerAdapter {
  readonly targets: () => WebControllerTargetInfo[];
  readonly status: () => Omit<WebControllerStatus, "schema" | "targets">;
  readonly select: (target?: WebControllerTargetRef) => WebControllerTargetInfo;
  readonly send: (request: WebControllerRunRequest) => Promise<WebControllerRunResult>;
  readonly cancel: (commandId: string) => boolean;
  readonly tail: (target?: WebControllerTargetRef, lines?: number) => string[];
}

export function installWebController(adapter: WebControllerAdapter): WebControllerApi {
  const api: WebControllerApi = {
    schema: "soty.web-controller.v1",
    targets: adapter.targets,
    list: adapter.targets,
    status: () => ({
      schema: "soty.web-controller.status.v1",
      ...adapter.status(),
      targets: adapter.targets()
    }),
    select: adapter.select,
    run: (input, second, third) => adapter.send(normalizeWebControllerRun("run", input, second, third)),
    script: (input, second, third) => adapter.send(normalizeWebControllerRun("script", input, second, third)),
    cancel: adapter.cancel,
    tail: adapter.tail
  };
  if (window.top !== window) {
    return api;
  }
  const win = window as Window & {
    SOTY?: Record<string, unknown> & { remote?: WebControllerApi; remoteController?: WebControllerApi };
    SOTY_REMOTE_CONTROLLER?: WebControllerApi;
  };
  win.SOTY = {
    ...(win.SOTY && typeof win.SOTY === "object" ? win.SOTY : {}),
    remote: api,
    remoteController: api
  };
  Object.defineProperty(win, "SOTY_REMOTE_CONTROLLER", {
    value: api,
    configurable: true
  });
  return api;
}

export function resolveWebControllerTarget(
  targets: readonly WebControllerTargetInfo[],
  selectedTunnelId = "",
  target?: WebControllerTargetRef
): WebControllerTargetInfo {
  const key = webControllerTargetKey(target);
  if (!key) {
    const selected = targets.find((item) => item.tunnelId === selectedTunnelId);
    if (selected) {
      return selected;
    }
    if (targets.length === 1 && targets[0]) {
      return targets[0];
    }
    throw new Error("SOTY.remote: choose a target from SOTY.remote.targets()");
  }
  const exact = targets.find((item) => webControllerExactTargetMatch(item, key));
  if (exact) {
    return exact;
  }
  const fuzzy = targets.filter((item) => item.label.toLowerCase().includes(key.toLowerCase()));
  if (fuzzy.length === 1 && fuzzy[0]) {
    return fuzzy[0];
  }
  throw new Error(fuzzy.length > 1 ? `SOTY.remote: ambiguous target: ${key}` : `SOTY.remote: target not found: ${key}`);
}

function normalizeWebControllerRun(
  kind: "run" | "script",
  input: WebControllerCommandInput,
  second?: string | WebControllerRunOptions,
  third: WebControllerRunOptions = {}
): WebControllerRunRequest {
  if (typeof input === "object" && input) {
    const body = String(input.command || input.script || input.text || "").trim();
    return { kind, body, options: input };
  }
  if (typeof second === "string") {
    return {
      kind,
      body: second.trim(),
      options: { ...third, target: third.target || input }
    };
  }
  return { kind, body: String(input || "").trim(), options: second || {} };
}

function webControllerTargetKey(target?: WebControllerTargetRef): string {
  if (!target) {
    return "";
  }
  if (typeof target === "string") {
    return target.trim();
  }
  return String(target.tunnelId || target.id || target.hostDeviceId || target.label || target.target || "").trim();
}

function webControllerExactTargetMatch(target: WebControllerTargetInfo, key: string): boolean {
  const normalized = key.toLowerCase();
  return target.tunnelId === key
    || target.hostDeviceId === key
    || target.deviceIds.includes(key)
    || target.label.toLowerCase() === normalized;
}
