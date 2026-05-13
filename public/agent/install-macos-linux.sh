#!/usr/bin/env sh
set -eu

BASE="https://xn--n1afe0b.online/agent"
RELAY_ID=""
SCOPE="CurrentUser"
INSTALL_DIR=""
LAUNCH_APP_AT_LOGON="0"
APP_URL="https://xn--n1afe0b.online/?pwa=1"
INSTALL_CODEX="0"
CODEX_PROXY_URL="${SOTY_CODEX_PROXY_URL:-${SOTY_AGENT_PROXY_URL:-}}"

die() {
  printf '%s\n' "$*" >&2
  exit 1
}

have() {
  command -v "$1" >/dev/null 2>&1
}

shell_quote() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

applescript_quote() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

usage() {
  cat <<'EOF'
Usage:
  sh install-macos-linux.sh [--scope user|machine] [--base URL] [--relay-id ID] [--codex-proxy-url URL] [--install-codex]

Legacy positional form is still supported:
  sh install-macos-linux.sh BASE RELAY_ID
EOF
}

POSITIONAL=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --base)
      [ "$#" -ge 2 ] || die "--base requires a value"
      BASE="$2"
      shift 2
      ;;
    --base=*)
      BASE="${1#--base=}"
      shift
      ;;
    --relay-id|--relay)
      [ "$#" -ge 2 ] || die "--relay-id requires a value"
      RELAY_ID="$2"
      shift 2
      ;;
    --relay-id=*|--relay=*)
      RELAY_ID="${1#*=}"
      shift
      ;;
    --scope)
      [ "$#" -ge 2 ] || die "--scope requires a value"
      SCOPE="$2"
      shift 2
      ;;
    --scope=*)
      SCOPE="${1#--scope=}"
      shift
      ;;
    --machine)
      SCOPE="Machine"
      shift
      ;;
    --user)
      SCOPE="CurrentUser"
      shift
      ;;
    --install-dir)
      [ "$#" -ge 2 ] || die "--install-dir requires a value"
      INSTALL_DIR="$2"
      shift 2
      ;;
    --install-dir=*)
      INSTALL_DIR="${1#--install-dir=}"
      shift
      ;;
    --launch-app-at-logon)
      LAUNCH_APP_AT_LOGON="1"
      shift
      ;;
    --app-url)
      [ "$#" -ge 2 ] || die "--app-url requires a value"
      APP_URL="$2"
      shift 2
      ;;
    --app-url=*)
      APP_URL="${1#--app-url=}"
      shift
      ;;
    --install-codex)
      INSTALL_CODEX="1"
      shift
      ;;
    --codex-proxy-url)
      [ "$#" -ge 2 ] || die "--codex-proxy-url requires a value"
      CODEX_PROXY_URL="$2"
      shift 2
      ;;
    --codex-proxy-url=*)
      CODEX_PROXY_URL="${1#--codex-proxy-url=}"
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      die "unknown option: $1"
      ;;
    *)
      if [ "$POSITIONAL" -eq 0 ]; then
        BASE="$1"
      elif [ "$POSITIONAL" -eq 1 ]; then
        RELAY_ID="$1"
      else
        die "unexpected argument: $1"
      fi
      POSITIONAL=$((POSITIONAL + 1))
      shift
      ;;
  esac
done

case "$SCOPE" in
  Machine|machine) SCOPE="Machine" ;;
  CurrentUser|currentuser|user|User) SCOPE="CurrentUser" ;;
  *) die "unsupported scope: $SCOPE" ;;
esac

case "$RELAY_ID" in
  ""|*[!A-Za-z0-9_-]*) RELAY_ID="" ;;
esac

OS_NAME="$(uname -s)"
case "$OS_NAME" in
  Darwin|Linux) ;;
  *) die "Unsupported OS: $OS_NAME" ;;
esac

request_machine_privileges() {
  [ "$SCOPE" = "Machine" ] || return 0
  [ "$(id -u)" != "0" ] || return 0

  script_path="$0"
  base_arg="$(shell_quote "$BASE")"
  relay_arg="$(shell_quote "$RELAY_ID")"
  install_arg=""
  if [ -n "$INSTALL_DIR" ]; then
    install_arg=" --install-dir $(shell_quote "$INSTALL_DIR")"
  fi
  launch_arg=""
  if [ "$LAUNCH_APP_AT_LOGON" = "1" ]; then
    launch_arg=" --launch-app-at-logon --app-url $(shell_quote "$APP_URL")"
  fi
  codex_arg=""
  if [ "$INSTALL_CODEX" = "1" ]; then
    codex_arg=" --install-codex"
  fi
  command_line="sh $(shell_quote "$script_path") --scope machine --base $base_arg --relay-id $relay_arg$install_arg$launch_arg$codex_arg"

  if [ "$OS_NAME" = "Darwin" ] && have osascript; then
    escaped="$(applescript_quote "$command_line")"
    exec osascript -e "do shell script \"$escaped\" with administrator privileges"
  fi
  if have pkexec; then
    if [ "$INSTALL_CODEX" = "1" ]; then
      exec pkexec sh "$script_path" --scope machine --base "$BASE" --relay-id "$RELAY_ID" --install-codex
    fi
    exec pkexec sh "$script_path" --scope machine --base "$BASE" --relay-id "$RELAY_ID"
  fi
  if have sudo; then
    if [ "$INSTALL_CODEX" = "1" ]; then
      exec sudo sh "$script_path" --scope machine --base "$BASE" --relay-id "$RELAY_ID" --install-codex
    fi
    exec sudo sh "$script_path" --scope machine --base "$BASE" --relay-id "$RELAY_ID"
  fi
  die "Administrator rights are required for machine install"
}

resolve_agent_dir() {
  if [ -n "$INSTALL_DIR" ]; then
    printf '%s\n' "$INSTALL_DIR"
    return
  fi
  if [ "$SCOPE" = "Machine" ]; then
    if [ "$OS_NAME" = "Darwin" ]; then
      printf '%s\n' "/Library/Application Support/Soty Agent"
    else
      printf '%s\n' "/opt/soty-agent"
    fi
    return
  fi
  printf '%s\n' "${HOME}/.soty-agent"
}

request_machine_privileges

AGENT_DIR="$(resolve_agent_dir)"
AGENT_PATH="${AGENT_DIR}/soty-agent.mjs"
RUNNER_PATH="${AGENT_DIR}/start-agent.sh"
CTL_PATH="${AGENT_DIR}/sotyctl"
MANIFEST_URL="${BASE}/manifest.json"
LOG_PATH="${AGENT_DIR}/install.log"
PROXY_ENV_PATH="${AGENT_DIR}/proxy.env"

mkdir -p "$AGENT_DIR"
touch "$LOG_PATH" 2>/dev/null || true

fetch_file() {
  url="$1"
  out="$2"
  part="${out}.download"
  attempt=1
  mkdir -p "$(dirname "$out")"
  while [ "$attempt" -le 4 ]; do
    if have curl; then
      if curl -fL --connect-timeout 20 --retry 2 --retry-delay 2 -C - "$url" -o "$part" >>"$LOG_PATH" 2>&1; then
        mv "$part" "$out"
        return 0
      fi
      rm -f "$part"
      if curl -fL --connect-timeout 20 --retry 2 --retry-delay 2 "$url" -o "$part" >>"$LOG_PATH" 2>&1; then
        mv "$part" "$out"
        return 0
      fi
    elif have wget; then
      if wget -q -c -O "$part" "$url" >>"$LOG_PATH" 2>&1; then
        mv "$part" "$out"
        return 0
      fi
      rm -f "$part"
    else
      die "curl or wget is required"
    fi
    attempt=$((attempt + 1))
    sleep "$attempt"
  done
  rm -f "$part"
  die "download failed: $url"
}

fetch_text() {
  tmp="${AGENT_DIR}/fetch.txt"
  fetch_file "$1" "$tmp"
  cat "$tmp"
  rm -f "$tmp"
}

sha256_file() {
  if have sha256sum; then
    sha256sum "$1" | awk '{print $1}'
    return
  fi
  if have shasum; then
    shasum -a 256 "$1" | awk '{print $1}'
    return
  fi
  if have openssl; then
    openssl dgst -sha256 -r "$1" | awk '{print $1}'
    return
  fi
  printf '%s\n' ''
}

node_ok() {
  "$1" -e "const v=process.versions.node.split('.').map(Number); process.exit(v[0] > 22 || (v[0] === 22 && v[1] >= 12) ? 0 : 1)" >/dev/null 2>&1
}

portable_node_name() {
  arch="$(uname -m)"
  case "$OS_NAME" in
    Darwin) node_os="darwin" ;;
    Linux) node_os="linux" ;;
    *) die "Unsupported OS: $OS_NAME" ;;
  esac
  case "$arch" in
    x86_64|amd64) node_arch="x64" ;;
    arm64|aarch64) node_arch="arm64" ;;
    *) die "Unsupported architecture: $arch" ;;
  esac
  printf '%s-%s\n' "$node_os" "$node_arch"
}

install_portable_node() {
  platform="$(portable_node_name)"
  node_dir="${AGENT_DIR}/node"
  local_node="${node_dir}/bin/node"
  if [ -x "$local_node" ] && node_ok "$local_node"; then
    printf '%s\n' "$local_node"
    return
  fi

  node_base="https://nodejs.org/dist/latest-v22.x"
  sums="$(fetch_text "${node_base}/SHASUMS256.txt")"
  archive="$(printf '%s\n' "$sums" | grep "node-v.*-${platform}.tar.xz$" | awk '{print $2}' | head -n 1)"
  [ -n "$archive" ] || die "No Node.js archive for $platform"
  expected="$(printf '%s\n' "$sums" | awk -v archive="$archive" '$2 == archive { print $1; exit }')"
  [ -n "$expected" ] || die "Node.js checksum is missing for $archive"

  tmp_dir="${AGENT_DIR}/node-download"
  archive_path="${AGENT_DIR}/${archive}"
  rm -rf "$tmp_dir"
  mkdir -p "$tmp_dir"
  fetch_file "${node_base}/${archive}" "$archive_path"
  actual="$(sha256_file "$archive_path")"
  [ -n "$actual" ] || die "sha256 tool is required"
  [ "$actual" = "$expected" ] || die "Node.js checksum mismatch"

  rm -rf "$node_dir"
  tar -xJf "$archive_path" -C "$tmp_dir"
  inner="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  [ -n "$inner" ] || die "Node.js archive is empty"
  mv "$inner" "$node_dir"
  rm -rf "$tmp_dir" "$archive_path"
  printf '%s\n' "$local_node"
}

resolve_node() {
  if have node && node_ok "$(command -v node)"; then
    command -v node
    return
  fi
  install_portable_node
}

codex_ok() {
  test -n "$1" && test -x "$1" && "$1" --version 2>/dev/null | grep -qi 'codex'
}

resolve_npm() {
  node_bin_dir="$(dirname "$NODE_PATH")"
  if [ -x "${node_bin_dir}/npm" ]; then
    printf '%s\n' "${node_bin_dir}/npm"
    return
  fi
  if have npm; then
    command -v npm
    return
  fi
  printf '%s\n' ''
}

resolve_codex() {
  if have codex && codex_ok "$(command -v codex)"; then
    command -v codex
    return
  fi
  codex_dir="${AGENT_DIR}/codex-cli"
  local_codex="${codex_dir}/node_modules/.bin/codex"
  if codex_ok "$local_codex"; then
    printf '%s\n' "$local_codex"
    return
  fi
  npm_bin="$(resolve_npm)"
  if [ -z "$npm_bin" ]; then
    printf '%s\n' "soty-codex-cli:install-skipped:npm-missing" >>"$LOG_PATH"
    printf '%s\n' ''
    return
  fi
  node_bin_dir="$(dirname "$NODE_PATH")"
  PATH="${node_bin_dir}:${PATH}"
  mkdir -p "$codex_dir"
  if "$npm_bin" install --prefix "$codex_dir" "@openai/codex@latest" --no-audit --no-fund >>"${AGENT_DIR}/codex-install.log" 2>&1; then
    if codex_ok "$local_codex"; then
      printf '%s\n' "$local_codex"
      return
    fi
  fi
  printf '%s\n' "soty-codex-cli:install-skipped" >>"$LOG_PATH"
  printf '%s\n' ''
}

install_agent_script() {
  manifest_path="${AGENT_DIR}/manifest.json"
  fetch_file "$MANIFEST_URL" "$manifest_path"
  meta="$("$NODE_PATH" -e 'const fs=require("fs"); const base=process.argv[1]; const file=process.argv[2]; const m=JSON.parse(fs.readFileSync(file,"utf8")); const url=typeof m.agentUrl==="string"?new URL(m.agentUrl,base).href:""; const hash=typeof m.sha256==="string"&&/^[a-f0-9]{64}$/i.test(m.sha256)?m.sha256.toLowerCase():""; console.log(url); console.log(hash);' "$MANIFEST_URL" "$manifest_path" 2>/dev/null || true)"
  agent_url="$(printf '%s\n' "$meta" | sed -n '1p')"
  expected_hash="$(printf '%s\n' "$meta" | sed -n '2p')"
  [ -n "$agent_url" ] || agent_url="${BASE}/soty-agent.mjs"
  next_path="${AGENT_PATH}.next"
  fetch_file "$agent_url" "$next_path"
  if [ -n "$expected_hash" ]; then
    actual_hash="$(sha256_file "$next_path")"
    [ -n "$actual_hash" ] || die "sha256 tool is required"
    [ "$actual_hash" = "$expected_hash" ] || die "agent checksum mismatch"
  fi
  mv "$next_path" "$AGENT_PATH"
  chmod 755 "$AGENT_PATH"
}

write_agent_config() {
  if [ -z "$RELAY_ID" ]; then
    return
  fi
  "$NODE_PATH" -e 'const fs=require("fs"); const path=process.argv[1]; const relayId=process.argv[2]; const relayBaseUrl=process.argv[3]; let existing={}; try { existing=JSON.parse(fs.readFileSync(path,"utf8")); } catch {} fs.writeFileSync(path, JSON.stringify({relayId, relayBaseUrl, installId: typeof existing.installId==="string"?existing.installId:""}, null, 2));' "${AGENT_DIR}/agent-config.json" "$RELAY_ID" "https://xn--n1afe0b.online"
}

is_codex_proxy_url() {
  case "$1" in
    http://*|https://*|socks5://*|socks5h://*) return 0 ;;
    *) return 1 ;;
  esac
}

proxy_scheme() {
  printf '%s\n' "$1" | sed -n 's,^\([A-Za-z0-9+.-][A-Za-z0-9+.-]*\)://.*,\1,p' | tr '[:upper:]' '[:lower:]'
}

resolve_codex_proxy_url() {
  if is_codex_proxy_url "$CODEX_PROXY_URL"; then
    printf '%s\n' "$CODEX_PROXY_URL"
    return
  fi
  if [ -f "$PROXY_ENV_PATH" ]; then
    existing="$(sed -n 's/^[[:space:]]*SOTY_CODEX_PROXY_URL[[:space:]]*=[[:space:]]*//p' "$PROXY_ENV_PATH" 2>/dev/null | head -n 1 || true)"
    if is_codex_proxy_url "$existing"; then
      printf '%s\n' "$existing"
      return
    fi
  fi
  if [ -f "$RUNNER_PATH" ]; then
    existing="$(sed -n 's/^[[:space:]]*export[[:space:]]*SOTY_CODEX_PROXY_URL[[:space:]]*=[[:space:]]*"\{0,1\}\([^"]*\)"\{0,1\}[[:space:]]*$/\1/p' "$RUNNER_PATH" 2>/dev/null | head -n 1 || true)"
    if is_codex_proxy_url "$existing"; then
      printf '%s\n' "$existing"
      return
    fi
  fi
}

write_codex_proxy_secret() {
  proxy="$(resolve_codex_proxy_url || true)"
  if [ -z "$proxy" ]; then
    return
  fi
  printf 'SOTY_CODEX_PROXY_URL=%s\n' "$proxy" > "$PROXY_ENV_PATH"
  chmod 600 "$PROXY_ENV_PATH" 2>/dev/null || true
  scheme="$(proxy_scheme "$proxy")"
  if [ -n "$scheme" ]; then
    printf 'soty-codex-proxy:configured:%s\n' "$scheme"
  else
    printf 'soty-codex-proxy:configured\n'
  fi
}

write_runner() {
  node_bin_dir="$(dirname "$NODE_PATH")"
  codex_bin_dir=""
  if [ -n "$CODEX_PATH" ]; then
    codex_bin_dir="$(dirname "$CODEX_PATH")"
  fi
  cat > "$RUNNER_PATH" <<EOF
#!/usr/bin/env sh
export SOTY_AGENT_MANAGED=1
export SOTY_AGENT_SCOPE="${SCOPE}"
export SOTY_AGENT_UPDATE_URL="${MANIFEST_URL}"
export SOTY_AGENT_RELAY_ID="${RELAY_ID}"
export SOTY_AGENT_RELAY_URL="https://xn--n1afe0b.online"
if [ -f "${AGENT_DIR}/proxy.env" ]; then
  proxy="\$(sed -n 's/^[[:space:]]*SOTY_CODEX_PROXY_URL[[:space:]]*=[[:space:]]*//p' "${AGENT_DIR}/proxy.env" 2>/dev/null | head -n 1 || true)"
  case "\$proxy" in
    http://*|https://*|socks5://*|socks5h://*) export SOTY_CODEX_PROXY_URL="\$proxy" ;;
  esac
fi
export PATH="${node_bin_dir}${codex_bin_dir:+:$codex_bin_dir}:\${PATH}"
while true; do
  "${NODE_PATH}" "${AGENT_PATH}"
  code=\$?
  if [ "\$code" = "75" ]; then
    sleep 1
  else
    sleep 3
  fi
done
EOF
  chmod 755 "$RUNNER_PATH"

  cat > "$CTL_PATH" <<EOF
#!/usr/bin/env sh
export PATH="${node_bin_dir}${codex_bin_dir:+:$codex_bin_dir}:\${PATH}"
exec "${NODE_PATH}" "${AGENT_PATH}" ctl "\$@"
EOF
  chmod 755 "$CTL_PATH"
}

agent_health() {
  if have curl; then
    payload="$(curl -fsS --max-time 3 "http://127.0.0.1:49424/health" 2>/dev/null || true)"
  elif have wget; then
    payload="$(wget -qO- --timeout=3 "http://127.0.0.1:49424/health" 2>/dev/null || true)"
  else
    return 1
  fi
  [ -n "$payload" ] || return 1
  printf '%s' "$payload" | "$NODE_PATH" -e 'let s=""; process.stdin.on("data", c => s += c); process.stdin.on("end", () => { try { const h=JSON.parse(s); const scope=process.argv[1]; const ok=h.ok===true && (scope!=="Machine" || (h.scope==="Machine" && h.maintenance===true)); process.exit(ok ? 0 : 1); } catch { process.exit(1); } });' "$SCOPE"
}

wait_agent_health() {
  seconds="$1"
  end=$(( $(date +%s) + seconds ))
  while [ "$(date +%s)" -le "$end" ]; do
    if agent_health; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_now() {
  if agent_health; then
    return
  fi
  nohup "$RUNNER_PATH" >>"${AGENT_DIR}/agent.log" 2>>"${AGENT_DIR}/agent.err" &
}

write_desktop_autostart() {
  [ "$SCOPE" = "CurrentUser" ] || return
  AUTOSTART_DIR="${HOME}/.config/autostart"
  mkdir -p "$AUTOSTART_DIR"
  cat > "${AUTOSTART_DIR}/soty-agent.desktop" <<EOF
[Desktop Entry]
Type=Application
Name=soty-agent
Exec=${RUNNER_PATH}
X-GNOME-Autostart-enabled=true
EOF
}

disable_user_autostart_for_machine() {
  [ "$SCOPE" = "Machine" ] || return
  user_name="${SUDO_USER:-}"
  [ -n "$user_name" ] || return
  user_home=""
  if have getent; then
    user_home="$(getent passwd "$user_name" | awk -F: '{print $6}' || true)"
  fi
  if [ -z "$user_home" ] && [ "$OS_NAME" = "Darwin" ]; then
    user_home="/Users/$user_name"
  fi
  [ -n "$user_home" ] || return
  rm -f "${user_home}/.config/autostart/soty-agent.desktop" 2>/dev/null || true
  rm -f "${user_home}/Library/LaunchAgents/online.soty.agent.plist" 2>/dev/null || true
  if have systemctl; then
    su - "$user_name" -c 'systemctl --user disable --now soty-agent.service >/dev/null 2>&1 || true' >/dev/null 2>&1 || true
  fi
}

stop_existing_agents_for_machine() {
  [ "$SCOPE" = "Machine" ] || return
  have pgrep || return
  for pid in $(pgrep -f 'soty-agent\.mjs|start-agent\.sh' 2>/dev/null || true); do
    [ "$pid" = "$$" ] && continue
    kill "$pid" >/dev/null 2>&1 || true
  done
  sleep 1
}

enable_macos_launchd() {
  if [ "$SCOPE" = "Machine" ]; then
    PLIST_PATH="/Library/LaunchDaemons/online.soty.agent.plist"
    cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>online.soty.agent</string>
  <key>ProgramArguments</key>
  <array><string>${RUNNER_PATH}</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>${AGENT_DIR}/agent.log</string>
  <key>StandardErrorPath</key><string>${AGENT_DIR}/agent.err</string>
</dict>
</plist>
EOF
    chown root:wheel "$PLIST_PATH" 2>/dev/null || true
    chmod 644 "$PLIST_PATH"
    launchctl bootout system "$PLIST_PATH" >/dev/null 2>&1 || true
    launchctl bootstrap system "$PLIST_PATH" >/dev/null 2>&1 || launchctl load -w "$PLIST_PATH" >/dev/null 2>&1 || start_now
    printf '%s\n' "soty-agent:autostart:launch-daemon"
    return
  fi

  PLIST_DIR="${HOME}/Library/LaunchAgents"
  PLIST_PATH="${PLIST_DIR}/online.soty.agent.plist"
  mkdir -p "$PLIST_DIR"
  cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>online.soty.agent</string>
  <key>ProgramArguments</key>
  <array><string>${RUNNER_PATH}</string></array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>${AGENT_DIR}/agent.log</string>
  <key>StandardErrorPath</key><string>${AGENT_DIR}/agent.err</string>
</dict>
</plist>
EOF
  launchctl bootout "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH" >/dev/null 2>&1 || launchctl load "$PLIST_PATH" >/dev/null 2>&1 || start_now
  printf '%s\n' "soty-agent:autostart:launch-agent"
}

enable_linux_systemd() {
  if [ "$SCOPE" = "Machine" ]; then
    SERVICE_PATH="/etc/systemd/system/soty-agent.service"
    cat > "$SERVICE_PATH" <<EOF
[Unit]
Description=soty.online machine local agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${RUNNER_PATH}
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
    chmod 644 "$SERVICE_PATH"
    systemctl daemon-reload >/dev/null 2>&1 \
      && systemctl enable --now soty-agent.service >/dev/null 2>&1 \
      && printf '%s\n' "soty-agent:autostart:systemd-system" \
      || { start_now; printf '%s\n' "soty-agent:autostart:systemd-system-fallback"; }
    return
  fi

  SERVICE_DIR="${HOME}/.config/systemd/user"
  SERVICE_PATH="${SERVICE_DIR}/soty-agent.service"
  mkdir -p "$SERVICE_DIR"
  cat > "$SERVICE_PATH" <<EOF
[Unit]
Description=soty.online local agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${RUNNER_PATH}
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload >/dev/null 2>&1 \
    && systemctl --user enable --now soty-agent.service >/dev/null 2>&1 \
    && printf '%s\n' "soty-agent:autostart:systemd-user" \
    || { write_desktop_autostart; start_now; printf '%s\n' "soty-agent:autostart:desktop"; }
}

enable_autostart() {
  if [ "$SCOPE" = "Machine" ]; then
    disable_user_autostart_for_machine
    stop_existing_agents_for_machine
  fi
  if [ "$OS_NAME" = "Darwin" ]; then
    enable_macos_launchd
  elif have systemctl; then
    enable_linux_systemd
  else
    write_desktop_autostart
    start_now
    printf '%s\n' "soty-agent:autostart:desktop"
  fi
}

NODE_PATH="$(resolve_node)"
CODEX_PATH=""
if [ "$INSTALL_CODEX" = "1" ]; then
  CODEX_PATH="$(resolve_codex)"
else
  printf '%s\n' "soty-codex-cli:install-skipped:default-light-agent" >>"$LOG_PATH"
fi
install_agent_script
write_agent_config
write_codex_proxy_secret
write_runner
enable_autostart
start_now

if wait_agent_health 25; then
  printf '%s\n' "soty-agent:health:${SCOPE}"
  printf '%s\n' "soty-agent:installed"
else
  printf '%s\n' "soty-agent:installed-no-health"
  printf '%s\n' "Check log: ${LOG_PATH}" >&2
  exit 1
fi
