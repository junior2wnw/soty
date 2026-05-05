#!/usr/bin/env sh
set -eu

BASE="${1:-https://xn--n1afe0b.online/agent}"
RELAY_ID="${2:-}"
AGENT_DIR="${HOME}/.soty-agent"
AGENT_PATH="${AGENT_DIR}/soty-agent.mjs"
RUNNER_PATH="${AGENT_DIR}/start-agent.sh"
CTL_PATH="${AGENT_DIR}/sotyctl"
MANIFEST_URL="${BASE}/manifest.json"

case "$RELAY_ID" in
  ""|*[!A-Za-z0-9_-]*) RELAY_ID="" ;;
esac

fetch_file() {
  url="$1"
  out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$out"
    return
  fi
  if command -v wget >/dev/null 2>&1; then
    wget -qO "$out" "$url"
    return
  fi
  printf '%s\n' 'curl or wget is required'
  exit 1
}

fetch_text() {
  tmp="${AGENT_DIR}/fetch.txt"
  fetch_file "$1" "$tmp"
  cat "$tmp"
  rm -f "$tmp"
}

node_ok() {
  "$1" -e "process.exit(Number(process.versions.node.split('.')[0]) >= 22 ? 0 : 1)" >/dev/null 2>&1
}

portable_node_name() {
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os" in
    Darwin) node_os="darwin" ;;
    Linux) node_os="linux" ;;
    *) printf '%s\n' "Unsupported OS: $os" >&2; exit 1 ;;
  esac
  case "$arch" in
    x86_64|amd64) node_arch="x64" ;;
    arm64|aarch64) node_arch="arm64" ;;
    *) printf '%s\n' "Unsupported architecture: $arch" >&2; exit 1 ;;
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
  if [ -z "$archive" ]; then
    printf '%s\n' "No Node.js archive for $platform" >&2
    exit 1
  fi
  expected="$(printf '%s\n' "$sums" | awk -v archive="$archive" '$2 == archive { print $1; exit }')"
  tmp_dir="${AGENT_DIR}/node-download"
  archive_path="${AGENT_DIR}/${archive}"
  rm -rf "$tmp_dir" "$node_dir"
  mkdir -p "$tmp_dir"
  fetch_file "${node_base}/${archive}" "$archive_path"

  if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$archive_path" | awk '{print $1}')"
  else
    actual="$(shasum -a 256 "$archive_path" | awk '{print $1}')"
  fi
  if [ "$actual" != "$expected" ]; then
    printf '%s\n' 'Node.js checksum mismatch' >&2
    exit 1
  fi

  tar -xJf "$archive_path" -C "$tmp_dir"
  inner="$(find "$tmp_dir" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
  mv "$inner" "$node_dir"
  rm -rf "$tmp_dir" "$archive_path"
  printf '%s\n' "$local_node"
}

resolve_node() {
  if command -v node >/dev/null 2>&1 && node_ok "$(command -v node)"; then
    command -v node
    return
  fi
  install_portable_node
}

agent_health() {
  if command -v curl >/dev/null 2>&1; then
    curl -fsS -H "Origin: https://xn--n1afe0b.online" "http://127.0.0.1:49424/health" >/dev/null 2>&1
    return $?
  fi
  return 1
}

mkdir -p "${AGENT_DIR}"
NODE_PATH="$(resolve_node)"
fetch_file "${MANIFEST_URL}" "${AGENT_DIR}/manifest.json"
fetch_file "${BASE}/soty-agent.mjs" "${AGENT_PATH}"
chmod 755 "${AGENT_PATH}"

cat > "${RUNNER_PATH}" <<EOF
#!/usr/bin/env sh
export SOTY_AGENT_MANAGED=1
export SOTY_AGENT_UPDATE_URL="${MANIFEST_URL}"
export SOTY_AGENT_RELAY_ID="${RELAY_ID}"
export SOTY_AGENT_RELAY_URL="https://xn--n1afe0b.online"
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
chmod 755 "${RUNNER_PATH}"

cat > "${CTL_PATH}" <<EOF
#!/usr/bin/env sh
exec "${NODE_PATH}" "${AGENT_PATH}" ctl "\$@"
EOF
chmod 755 "${CTL_PATH}"

start_now() {
  if agent_health; then
    return
  fi
  nohup "${RUNNER_PATH}" >/dev/null 2>&1 &
}

write_desktop_autostart() {
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

if [ "$(uname -s)" = "Darwin" ]; then
  PLIST_DIR="${HOME}/Library/LaunchAgents"
  PLIST_PATH="${PLIST_DIR}/online.soty.agent.plist"
  mkdir -p "${PLIST_DIR}"
  cat > "${PLIST_PATH}" <<EOF
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
  launchctl bootout "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || true
  launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || launchctl load "${PLIST_PATH}" >/dev/null 2>&1 || start_now
elif command -v systemctl >/dev/null 2>&1; then
  SERVICE_DIR="${HOME}/.config/systemd/user"
  SERVICE_PATH="${SERVICE_DIR}/soty-agent.service"
  mkdir -p "${SERVICE_DIR}"
  cat > "${SERVICE_PATH}" <<EOF
[Unit]
Description=soty.online local agent

[Service]
ExecStart=${RUNNER_PATH}
Restart=always
RestartSec=3

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload >/dev/null 2>&1 \
    && systemctl --user enable --now soty-agent.service >/dev/null 2>&1 \
    || { write_desktop_autostart; start_now; }
else
  write_desktop_autostart
  start_now
fi

start_now
printf '%s\n' 'soty-agent:installed'
