export const localAgentHost = "127.0.0.1";
export const localAgentPort = 49424;
export const localAgentAuthority = `${localAgentHost}:${localAgentPort}`;
export const localAgentHttpBaseUrl = `http://${localAgentAuthority}`;
export const localAgentWsUrl = `ws://${localAgentAuthority}`;
export const localAgentUnavailableText = `! ${localAgentAuthority}`;

export function localAgentHttpUrl(path = ""): string {
  const suffix = path ? (path.startsWith("/") ? path : `/${path}`) : "";
  return `${localAgentHttpBaseUrl}${suffix}`;
}

export function isLocalAgentUnavailableText(value: string): boolean {
  return value.includes(localAgentAuthority);
}
