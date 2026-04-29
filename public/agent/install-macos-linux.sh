#!/usr/bin/env sh
set -eu

BASE="${1:-https://xn--n1afe0b.online/agent}"
AGENT_DIR="${HOME}/.soty-agent"
AGENT_PATH="${AGENT_DIR}/soty-agent.mjs"
RUNNER_PATH="${AGENT_DIR}/start-agent.sh"
MANIFEST_URL="${BASE}/manifest.json"

if ! command -v node >/dev/null 2>&1; then
  printf '%s\n' 'Node.js is required: https://nodejs.org/'
  exit 1
fi

mkdir -p "${AGENT_DIR}"
curl -fsSL "${MANIFEST_URL}" -o "${AGENT_DIR}/manifest.json"
curl -fsSL "${BASE}/soty-agent.mjs" -o "${AGENT_PATH}"
chmod 755 "${AGENT_PATH}"

cat > "${RUNNER_PATH}" <<EOF
#!/usr/bin/env sh
export SOTY_AGENT_MANAGED=1
export SOTY_AGENT_UPDATE_URL="${MANIFEST_URL}"
while true; do
  node "${AGENT_PATH}"
  code=\$?
  if [ "\$code" = "75" ]; then
    sleep 1
  else
    sleep 3
  fi
done
EOF
chmod 755 "${RUNNER_PATH}"

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
  launchctl bootstrap "gui/$(id -u)" "${PLIST_PATH}" >/dev/null 2>&1 || launchctl load "${PLIST_PATH}"
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
  systemctl --user daemon-reload
  systemctl --user enable --now soty-agent.service
else
  nohup "${RUNNER_PATH}" >/dev/null 2>&1 &
fi

printf '%s\n' 'soty-agent:installed'
