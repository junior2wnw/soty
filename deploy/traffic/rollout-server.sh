#!/bin/sh
set -eu

image="${1:?usage: rollout-server.sh IMAGE}"
current="soty-online-chat"
stamp="$(date -u +%Y%m%d%H%M%S)"
next="${current}-next-${stamp}"
previous="${current}-rollback-${stamp}"
runtime_dir="${HOME}/.config/soty"
runtime_env="${runtime_dir}/online-chat.env"
target="${SOTY_TRAFFIC_TUNNEL_TARGET:-http://172.17.0.1:24444/inside}"
public_path="${SOTY_TRAFFIC_TUNNEL_PATH:-/api/traffic/tunnel}"
ws_target="${SOTY_TRAFFIC_WS_TARGET:-ws://172.17.0.1:24446/inside-ws}"
ws_path="${SOTY_TRAFFIC_WS_PATH:-/api/traffic/ws}"
xray_api="${SOTY_TRAFFIC_XRAY_API:-172.17.0.1:24445}"

mkdir -p "${runtime_dir}"
chmod 700 "${runtime_dir}"
umask 077
docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "${current}" \
  | grep -Ev '^(SOTY_TRAFFIC_|SOTY_CODEX_|SOTY_GONKA_FALLBACK_MODEL=)' > "${runtime_env}.tmp"
{
  printf 'SOTY_TRAFFIC_TUNNEL_TARGET=%s\n' "${target}"
  printf 'SOTY_TRAFFIC_TUNNEL_PATH=%s\n' "${public_path}"
  printf 'SOTY_TRAFFIC_WS_TARGET=%s\n' "${ws_target}"
  printf 'SOTY_TRAFFIC_WS_PATH=%s\n' "${ws_path}"
  printf 'SOTY_TRAFFIC_XRAY_API=%s\n' "${xray_api}"
} >> "${runtime_env}.tmp"
mv "${runtime_env}.tmp" "${runtime_env}"
chmod 600 "${runtime_env}"

docker image inspect "${image}" >/dev/null
docker run --rm "${image}" node --check server/index.js >/dev/null
docker run --rm "${image}" node -e "import('./server/index.js').then(() => setTimeout(() => process.exit(0), 500))" >/dev/null
docker rm -f "${next}" >/dev/null 2>&1 || true
docker create \
  --name "${next}" \
  --restart unless-stopped \
  --env-file "${runtime_env}" \
  --mount type=volume,src=soty-online-chat-data,dst=/data \
  --publish 127.0.0.1:18182:8080 \
  "${image}" >/dev/null

rollback() {
  docker rm -f "${current}" >/dev/null 2>&1 || true
  docker rename "${previous}" "${current}" >/dev/null 2>&1 || true
  docker start "${current}" >/dev/null 2>&1 || true
}

docker stop -t 20 "${current}" >/dev/null
docker rename "${current}" "${previous}"
docker rename "${next}" "${current}"
trap rollback HUP INT TERM
docker start "${current}" >/dev/null

healthy=0
i=0
while [ "${i}" -lt 30 ]; do
  if curl --fail --silent --max-time 2 http://127.0.0.1:18182/ready \
      | grep -q '"ok":true'; then
    healthy=1
    break
  fi
  i=$((i + 1))
  sleep 1
done

if [ "${healthy}" -ne 1 ]; then
  docker logs --tail 100 "${current}" >&2 || true
  rollback
  exit 1
fi

trap - HUP INT TERM
printf 'deployed=%s rollback=%s\n' "${image}" "${previous}"
