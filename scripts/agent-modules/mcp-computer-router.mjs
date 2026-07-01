export function createMcpComputerRouter(dependencies = {}) {
  const {
    cleanActionToken = (value, fallback = "") => String(value || fallback || "").trim().toLowerCase()
  } = dependencies;

  const toolNameAliases = Object.freeze({
    computer: "soty_computer",
    artifact: "soty_artifact",
    artifacts: "soty_artifact",
    os_reinstall: "soty_reinstall",
    reinstall: "soty_reinstall",
    jobs: "soty_action_list",
    job_status: "soty_action_status",
    job_stop: "soty_action_stop",
    shell: "soty_action",
    filesystem: "soty_file",
    file: "soty_file",
    web: "soty_web",
    internet: "soty_web",
    fetch: "soty_web",
    search: "soty_web",
    browser: "soty_browser",
    desktop: "soty_desktop",
    process: "soty_process",
    processes: "soty_process",
    proc: "soty_process",
    clipboard: "soty_clipboard",
    network: "soty_network",
    net: "soty_network",
    audio: "soty_audio"
  });

  const noAliasOperations = new Set(["discover", "describe", "capabilities", "tools", "plane", "route-profiles", "route_profiles", "profiles", "routes"]);
  const linkStatusOperations = new Set(["health", "link", "source", "source-status"]);
  const actionListOperations = new Set(["jobs", "list", "action-list"]);
  const actionStatusOperations = new Set(["job-status", "job_status", "result", "action-status"]);
  const actionStopOperations = new Set(["stop", "cancel", "job-stop", "job_stop", "action-stop"]);
  const reinstallCapabilities = new Set(["windows-reinstall", "os-reinstall", "reinstall"]);
  const fileOperations = new Set(["file", "filesystem", "read", "write", "append", "list", "stat", "mkdir", "move", "copy", "delete", "publish", "cycle"]);
  const webOperations = new Set(["web", "internet", "web-fetch", "web_fetch", "fetch", "fetch-url", "fetch_url", "web-search", "web_search", "search"]);
  const webCapabilities = new Set(["web", "internet", "web-search"]);
  const processOperations = new Set(["process", "processes", "proc", "ps", "task", "tasklist", "start-process", "start_process", "stop-process", "stop_process"]);
  const processCapabilities = new Set(["process", "processes", "proc", "task"]);
  const clipboardOperations = new Set(["clipboard", "clipboard-read", "clipboard_read", "clipboard-write", "clipboard_write"]);
  const clipboardCapabilities = new Set(["clipboard"]);
  const networkOperations = new Set(["network", "net", "interfaces", "connectivity", "probe", "ping", "dns", "tcp"]);
  const networkCapabilities = new Set(["network", "net", "connectivity"]);
  const directRunOperations = new Set(["run", "script"]);
  const actionOperations = new Set(["run", "script", "action", "execute", "shell", "terminal", "console", "long-job", "long_job"]);
  const browserActions = new Set(["open", "goto", "title", "text", "eval", "click_text", "type", "screenshot"]);
  const fileActions = new Set(["read", "write", "append", "list", "stat", "mkdir", "search", "move", "copy", "delete", "download", "publish", "cycle"]);
  const processActions = new Set(["list", "status", "start", "launch", "open", "stop", "kill", "close"]);
  const clipboardActions = new Set(["read", "get", "paste", "write", "set", "copy"]);
  const networkActions = new Set(["status", "interfaces", "probe", "connect", "ping"]);
  const reinstallActions = new Set(["preflight", "prepare", "status", "repair", "cancel", "arm"]);
  const desktopOperations = new Set(["desktop", "screen", "display", "screenshot", "windows", "window", "focus", "click", "type", "key", "keyboard", "mouse", "wallpaper"]);

  function canonicalSotyMcpToolName(value) {
    const name = String(value || "").trim();
    const normalized = name.toLowerCase().replace(/-/gu, "_");
    return toolNameAliases[normalized] || name;
  }

  function computerToolAlias(operationValue, capabilityValue, args = {}) {
    const operation = cleanActionToken(operationValue, "");
    const capability = cleanActionToken(capabilityValue, "");
    const key = `${operation} ${capability}`.toLowerCase();
    if (noAliasOperations.has(operation)) {
      return "";
    }
    if (linkStatusOperations.has(operation)) {
      return "soty_link_status";
    }
    if (operation === "status" && (processCapabilities.has(capability) || args.pid || args.processName)) {
      return "soty_process";
    }
    if (operation === "status" && clipboardCapabilities.has(capability)) {
      return "soty_clipboard";
    }
    if (operation === "status" && networkCapabilities.has(capability)) {
      return "soty_network";
    }
    if (operation === "status" && !args.jobId && !reinstallCapabilities.has(capability)) {
      return "soty_link_status";
    }
    if (actionListOperations.has(operation)) {
      return "soty_action_list";
    }
    if (actionStatusOperations.has(operation) || (operation === "status" && args.jobId)) {
      return "soty_action_status";
    }
    if (actionStopOperations.has(operation)) {
      return "soty_action_stop";
    }
    if (operation === "toolkit" || operation === "toolkits" || capability === "capability-gateway") {
      return "soty_toolkit";
    }
    if (operation === "reinstall" || reinstallCapabilities.has(capability)) {
      return "soty_reinstall";
    }
    if (operation === "artifact" || capability === "artifact" || args.localPath || args.targetPath) {
      return "soty_artifact";
    }
    const fileOperation = fileOperations.has(operation)
      || (["search", "download"].includes(operation) && (args.path || args.pattern || args.glob));
    if ((fileOperation && (args.path || operation === "file" || operation === "filesystem")) || key.includes("filesystem") || key.includes("file")) {
      return "soty_file";
    }
    if (webOperations.has(operation) || webCapabilities.has(capability) || args.query) {
      return "soty_web";
    }
    if (operation === "image" || operation === "generate-image" || capability === "image" || args.prompt) {
      return "native_openai_image_required";
    }
    if (operation === "open-url" || operation === "open_url" || capability === "url") {
      return "soty_open_url";
    }
    if (processOperations.has(operation) || processCapabilities.has(capability) || args.pid || args.processName) {
      return "soty_process";
    }
    if (clipboardOperations.has(operation) || clipboardCapabilities.has(capability)) {
      return "soty_clipboard";
    }
    if (networkOperations.has(operation) || networkCapabilities.has(capability) || args.host || args.port) {
      return "soty_network";
    }
    if (directRunOperations.has(operation) && args.durable === false) {
      return operation === "script" ? "soty_script" : "soty_run";
    }
    if (actionOperations.has(operation)
      || /\b(?:shell|terminal|console|service|package|install|repair|diagnostic|probe|verify|long-job|long_job)\b/u.test(key)
      || args.command
      || args.script) {
      return "soty_action";
    }
    if (operation === "browser" || key.includes("browser")) {
      return "soty_browser";
    }
    if (operation === "audio" || key.includes("audio") || key.includes("volume") || key.includes("mute")) {
      return "soty_audio";
    }
    if (desktopOperations.has(operation) || /\b(?:desktop|screen|display|screenshot|window|keyboard|mouse|wallpaper)\b/u.test(key)) {
      return "soty_desktop";
    }
    return "";
  }

  function computerToolArguments(alias, args, operationValue, capabilityValue) {
    const operation = cleanActionToken(operationValue, "");
    const capability = cleanActionToken(capabilityValue, "");
    const next = { ...(args || {}) };
    delete next.operation;
    delete next.capability;
    if (alias === "soty_toolkit") {
      next.operation = operation === "toolkit" || operation === "toolkits" ? "describe" : operation;
      return next;
    }
    if (alias === "soty_file" && !next.action) {
      next.action = fileActions.has(operation) ? operation : "stat";
    }
    if (alias === "soty_browser" && !next.action) {
      next.action = browserActions.has(operation) ? operation : "text";
    }
    if (alias === "soty_web" && !next.action) {
      next.action = ["search", "web-search", "web_search"].includes(operation) ? "search" : "fetch";
    }
    if (alias === "soty_process" && !next.action) {
      next.action = processActions.has(operation) ? operation : "list";
    }
    if (alias === "soty_clipboard" && !next.action) {
      next.action = clipboardActions.has(operation) ? operation : "read";
    }
    if (alias === "soty_network" && !next.action) {
      next.action = networkActions.has(operation) ? operation : "status";
    }
    if (alias === "soty_desktop" && !next.action) {
      next.action = operation === "screen" ? "display" : operation;
    }
    if (alias === "soty_reinstall" && !next.action) {
      next.action = reinstallActions.has(operation)
        ? operation
        : (next.phase || (operation === "reinstall" ? "prepare" : "status"));
    }
    if (alias === "soty_action") {
      if (!next.mode) {
        next.mode = typeof next.script === "string" ? "script" : "run";
      }
      if (capability && !next.family && !next.toolkit) {
        next.family = capability;
      }
      if (next.detached !== true && next.waitForCompletion !== false) {
        next.waitForCompletion = true;
      }
    }
    return next;
  }

  return Object.freeze({
    canonicalSotyMcpToolName,
    computerToolAlias,
    computerToolArguments
  });
}
