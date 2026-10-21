/** Convert only the TLS transports actually issued by the shared-internet API.
 * Credentials remain on the device; this helper never calls a converter service. */
export function buildSingBoxProfile(value) {
  const invalid = () => { throw new Error('invalid-sing-box-profile'); };
  let uri;
  try {
    if (typeof value !== 'string' || value.length > 2048) invalid();
    uri = new URL(value);
  } catch { invalid(); }
  if (uri.protocol !== 'vless:' || uri.password || !/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/iu.test(uri.username)
    || !uri.hostname || uri.searchParams.get('security') !== 'tls') invalid();
  const transport = uri.searchParams.get('type') || 'tcp';
  if (!['tcp', 'ws'].includes(transport)) throw new Error('sing-box-transport-unsupported');
  const port = Number(uri.port || 443);
  if (!Number.isInteger(port) || port < 1 || port > 65535) invalid();
  const host = uri.searchParams.get('host') || uri.hostname;
  const serverName = uri.searchParams.get('sni') || uri.hostname;
  const path = uri.searchParams.get('path') || '/';
  if ([host, serverName].some(item => item.length > 253 || !/^[a-z0-9.:[\]-]+$/iu.test(item))
    || !path.startsWith('/') || path.length > 512 || /[\u0000-\u001f\u007f]/u.test(path)) invalid();
  const flow = uri.searchParams.get('flow') || '';
  if (flow && (flow !== 'xtls-rprx-vision' || transport !== 'tcp')) invalid();
  return {
    log: { level: 'warn' },
    dns: {
      servers: [
        { type: 'local', tag: 'entry-bootstrap' },
        { type: 'tcp', tag: 'exit-dns', server: '1.1.1.1', server_port: 53, detour: 'soty-internet' },
      ],
      final: 'exit-dns', strategy: 'prefer_ipv4',
    },
    inbounds: [{ type: 'tun', tag: 'soty-tun', address: ['172.19.0.1/30', 'fdfe:dcba:9876::1/126'], auto_route: true, strict_route: true, stack: 'mixed' }],
    outbounds: [{
      type: 'vless', tag: 'soty-internet', server: uri.hostname.replace(/^\[|\]$/gu, ''), server_port: port, uuid: uri.username,
      domain_resolver: { server: 'entry-bootstrap', strategy: 'prefer_ipv4' },
      tls: { enabled: true, server_name: serverName, insecure: false },
      ...(flow ? { flow } : {}),
      ...(transport === 'ws' ? { transport: { type: 'ws', path, headers: { Host: host } } } : {}),
    }],
    route: {
      rules: [{ port: 53, action: 'hijack-dns' }, { ip_is_private: true, action: 'reject' }],
      final: 'soty-internet', auto_detect_interface: true,
    },
  };
}
