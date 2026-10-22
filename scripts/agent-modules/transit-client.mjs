export const transitClientVersion = '1.13.15';
export const transitClientWindowsRelease = Object.freeze({
  version: transitClientVersion, platform: 'win32', arch: 'x64',
  sha256: '599b296f6e57511d36d2a6f3011aed1a86fa98418578bbb06bd6dc241b5d8877',
  binarySha256: '4db8218dea131668ccd5e0b32e773e916a37be730e655b176e0d3a930276cbe7',
  executable: 'sing-box-1.13.15-windows-amd64/sing-box.exe',
  urls: ['https://github.com/SagerNet/sing-box/releases/download/v1.13.15/sing-box-1.13.15-windows-amd64.zip'],
});

/** The installer establishes this authority after OS consent. Public health and
 * a copied Device ID never grant control or disclose an access profile. */
export async function executeTransitClientAction(body, authority) {
  if (!authority.deviceId || body.deviceId !== authority.deviceId) throw new Error('transit-device-mismatch');
  if (typeof body.controlToken !== 'string' || !/^[A-Za-z0-9_-]{43}$/u.test(body.controlToken)
    || !authority.verifyControlToken(body.controlToken)) throw new Error('transit-control-not-paired');
  const runtime = authority.runtime;
  switch (body.action) {
    case 'authorize': return { runtime: await runtime.status() };
    case 'import': return { runtime: await runtime.configure(body.profile) };
    case 'start': return { runtime: await runtime.start() };
    case 'stop': return { runtime: await runtime.stop() };
    case 'export': return { profile: await runtime.exportProfile() };
    default: throw new Error('transit-action-invalid');
  }
}

/** This is an existing access profile, never an account creator or issuer. */
export function normalizeTransitClientProfile(input) {
  let value;
  try {
    const text = JSON.stringify(input);
    if (!text || text.length > 256_000) throw new Error();
    value = JSON.parse(text);
  } catch { throw new Error('transit-profile-invalid'); }
  const deny = () => { throw new Error('transit-profile-invalid'); };
  if (!value || !Array.isArray(value.outbounds) || value.outbounds.length < 1 || value.outbounds.length > 32
    || !value.dns?.servers?.length || !value.route?.final || value.route.rule_set?.length
    || value.experimental || value.services || value.endpoints || value.ntp || value.certificate) deny();
  const tags = new Set();
  for (const row of value.outbounds) {
    if (!row.tag || tags.has(row.tag) || !['vless', 'hysteria2', 'urltest', 'selector'].includes(row.type)) deny();
    tags.add(row.tag);
    if (['vless', 'hysteria2'].includes(row.type) && (!row.server || row.tls?.enabled !== true || row.tls?.insecure !== false)) deny();
  }
  if (!tags.has(value.route.final)) deny();
  for (const row of value.outbounds) {
    if (['urltest', 'selector'].includes(row.type)
      && (!Array.isArray(row.outbounds) || !row.outbounds.length || row.outbounds.some(tag => !tags.has(tag) || tag === row.tag))) deny();
  }
  const finalDns = value.dns.servers.find(row => row.tag === value.dns.final);
  if (!finalDns || !tags.has(finalDns.detour)) deny();
  // Preserve the existing peer pins, access credential, route policy and DNS.
  // Only the OS-owned inbound changes; imported listeners/controllers cannot open.
  value.inbounds = [{ type: 'tun', tag: 'transit-soty', interface_name: 'SotyTransit',
    address: ['172.19.0.1/30', 'fdfe:dcba:9876::1/126'], mtu: 1400,
    auto_route: true, strict_route: true, stack: 'mixed' }];
  return value;
}

export function transitUnderlayProfile(input, interfaceName) {
  if (typeof interfaceName !== 'string' || !interfaceName.trim() || interfaceName.length > 160) throw new Error('transit-underlay-unavailable');
  const value = normalizeTransitClientProfile(input);
  for (const row of value.outbounds) if (['vless', 'hysteria2'].includes(row.type)) row.bind_interface = interfaceName;
  return value;
}

/** Filter only the runtime copy. A fast entry canary does not prove that the
 * peer forwards real HTTPS, so an unhealthy leaf must not win urltest later. */
export function transitCheckedProfile(input, healthyTags) {
  const value = normalizeTransitClientProfile(input), healthy = new Set(healthyTags);
  value.outbounds = value.outbounds.filter(row => !['vless', 'hysteria2'].includes(row.type) || healthy.has(row.tag));
  let changed;
  do {
    changed = false;
    const tags = new Set(value.outbounds.map(row => row.tag));
    value.outbounds = value.outbounds.filter(row => {
      if (!['urltest', 'selector'].includes(row.type)) return true;
      row.outbounds = row.outbounds.filter(tag => tags.has(tag));
      if (row.default && !row.outbounds.includes(row.default)) row.default = row.outbounds[0];
      if (row.outbounds.length) return true;
      changed = true; return false;
    });
  } while (changed);
  const tags = new Set(value.outbounds.map(row => row.tag));
  if (!tags.has(value.route.final)
    || value.dns.servers.some(row => row.detour && !tags.has(row.detour))
    || value.route.rules?.some(row => row.outbound && !tags.has(row.outbound))) throw new Error('transit-probe-failed');
  return normalizeTransitClientProfile(value);
}

/** Dependencies own private files, OS processes and probes. All lifecycle effects
 * serialize here so double clicks, stop and shutdown cannot create two tunnels. */
export function createTransitClientRuntime(deps) {
  let child = null, phase = 'off', lastError = '', availableEntryCount = 0, queue = Promise.resolve(), recovery = null;
  const serial = action => {
    const run = queue.then(action);
    queue = run.catch(() => undefined);
    return run;
  };
  const isAlive = owned => owned && owned.exitCode === null && !owned.signalCode;
  const alive = () => isAlive(child);
  const safeError = error => /^transit-[a-z-]+$/u.test(error?.message || '') ? error.message : 'transit-client-failed';
  function watch(owned) {
    child = owned;
    owned.once('error', () => {
      if (child === owned) { child = null; phase = 'error'; lastError = 'transit-client-exited'; }
    });
    owned.once('exit', () => {
      void deps.forgetOwned?.(owned).catch(() => undefined);
      if (child !== owned) return;
      child = null;
      if (phase !== 'stopping') { phase = 'error'; lastError = 'transit-client-exited'; }
    });
  }
  function recoverOnce() {
    return recovery ||= (async () => {
      const owned = await deps.recoverOwned?.();
      if (isAlive(owned)) { watch(owned); phase = 'unknown'; lastError = 'transit-recovered-process'; }
    })();
  }
  async function status() {
    await recoverOnce();
    const profile = await deps.readProfile();
    if (!alive() && phase === 'off') {
      try {
        if (!(await deps.confirmStopped())) { phase = 'unknown'; lastError = 'transit-stop-unconfirmed'; }
      } catch { phase = 'unknown'; lastError = 'transit-stop-unconfirmed'; }
    }
    return { schema: 'soty.transit-client.v1', configured: !!profile, running: !!alive(),
      active: !!alive() && phase === 'connected', phase, version: transitClientVersion, lastError,
      availableEntryCount,
      entryCount: profile?.outbounds?.filter(row => ['vless', 'hysteria2'].includes(row.type)).length || 0 };
  }
  async function prepare() {
    const core = await deps.installCore();
    if (!(await deps.verifyCore(core))) throw new Error('transit-core-pin-mismatch');
    return core;
  }
  async function requireStopped() {
    try { if (await deps.confirmStopped() === true) return; } catch {}
    phase = 'unknown';
    throw new Error('transit-stop-unconfirmed');
  }
  async function stopOwned() {
    if (!alive()) {
      child = null;
      await requireStopped();
      phase = 'off'; return;
    }
    const owned = child;
    phase = 'stopping';
    owned.kill();
    await Promise.race([deps.onceExit(owned), deps.wait(5000)]);
    if (isAlive(owned)) {
      owned.kill('SIGKILL');
      await Promise.race([deps.onceExit(owned), deps.wait(2000)]);
    }
    if (isAlive(owned)) { phase = 'unknown'; throw new Error('transit-stop-unconfirmed'); }
    if (child === owned) child = null;
    await requireStopped();
    phase = 'off';
  }
  return {
    status,
    prepare: () => serial(async () => { await prepare(); return status(); }),
    check: input => serial(async () => {
      const profile = normalizeTransitClientProfile(input), core = await prepare();
      const proof = await deps.probe(core, transitUnderlayProfile(profile, await deps.underlayInterface()));
      return { schema: 'soty.transit-check.v1', systemTunnelStarted: false,
        entryCount: profile.outbounds.filter(row => ['vless', 'hysteria2'].includes(row.type)).length,
        availableEntryCount: proof.availableEntryCount || 1 };
    }),
    configure: input => serial(async () => {
      await recoverOnce();
      if (alive()) throw new Error('transit-stop-before-import');
      const profile = normalizeTransitClientProfile(input), core = await prepare();
      await deps.saveCheckedProfile(core, profile);
      lastError = ''; phase = 'off';
      return status();
    }),
    start: () => serial(async () => {
      await recoverOnce();
      if (alive() && phase === 'connected') return status();
      try {
        if (alive()) await stopOwned();
        const profile = await deps.readProfile();
        if (!profile) throw new Error('transit-profile-required');
        phase = 'checking'; lastError = '';
        const core = await prepare();
        const prepared = transitUnderlayProfile(profile, await deps.underlayInterface());
        // Probe before creating a TUN; an unavailable network leaves the old VPN alone.
        const proof = await deps.probe(core, prepared);
        const file = await deps.writeRunProfile(core, proof.profile ? normalizeTransitClientProfile(proof.profile) : prepared);
        availableEntryCount = proof.availableEntryCount || prepared.outbounds.filter(row => ['vless', 'hysteria2'].includes(row.type)).length;
        phase = 'starting';
        const owned = deps.spawnCore(core, file);
        watch(owned);
        await deps.rememberOwned?.(owned, core, file);
        await deps.wait(1500);
        if (!alive() || !(await deps.confirmRunning(owned, proof))) throw new Error('transit-start-unconfirmed');
        phase = 'connected';
        return status();
      } catch (error) {
        lastError = safeError(error);
        try { await stopOwned(); } catch { lastError = 'transit-stop-unconfirmed'; }
        if (phase !== 'unknown') phase = 'error';
        throw new Error(lastError);
      }
    }),
    stop: () => serial(async () => {
      await recoverOnce();
      try { await stopOwned(); lastError = ''; return status(); }
      catch (error) { lastError = safeError(error); throw new Error(lastError); }
    }),
    exportProfile: () => serial(async () => {
      const profile = await deps.readProfile();
      if (!profile) throw new Error('transit-profile-required');
      return normalizeTransitClientProfile(profile);
    }),
  };
}
