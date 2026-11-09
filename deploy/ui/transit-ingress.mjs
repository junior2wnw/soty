import { verifyStructuralDelta } from './release.mjs';

const need = (value, code) => { if (!value) throw Object.assign(new Error(code), { code }); };
export const transitSubscriptionPath = '/transit/subscriptions/*';

/** Extend the existing Root API boundary, preserving every other adapted node.
 * The caller separately qualifies disk/active provenance and encrypted CAS.
 * This function never reads custody, writes a file, or signals a process. */
export async function prepareTransitSubscriptionIngress({ currentDisk, adapt, host = '4-2.xn--p1ai' }) {
  need(Buffer.isBuffer(currentDisk) && currentDisk.length > 0 && currentDisk.length <= 131072
    && !/\{\$/u.test(currentDisk.toString()) && typeof adapt === 'function'
    && /^[a-z0-9.-]+$/u.test(host), 'transit_ingress_input_invalid');
  const before = await adapt(currentDisk), candidates = [];
  const proxies = value => {
    if (!value || typeof value !== 'object') return 0;
    const own = value.handler === 'reverse_proxy' && value.upstreams?.length === 1
      && value.upstreams[0].dial === '127.0.0.1:18182' ? 1 : 0;
    return own + Object.values(value).reduce((sum, child) => sum + proxies(child), 0);
  };
  const visit = (value, pointer = [], hosts = []) => {
    if (Array.isArray(value)) { value.forEach((child, i) => visit(child, [...pointer, i], hosts)); return; }
    if (!value || typeof value !== 'object') return;
    if (Array.isArray(value.match)) {
      const local = value.match.flatMap(match => match.host ?? []); if (local.length) hosts = local;
      if (hosts.length === 1 && hosts[0] === host && value.match.length === 1
        && Object.keys(value.match[0]).join(',') === 'expression'
        && typeof value.match[0].expression?.expr === 'string'
        && value.match[0].expression.expr.startsWith('path(') && proxies(value) > 0)
        candidates.push({ pointer: [...pointer, 'match', 0, 'expression', 'expr'], expression: value.match[0].expression.expr });
    }
    Object.entries(value).forEach(([key, child]) => visit(child, [...pointer, key], hosts));
  };
  visit(before); need(candidates.length === 1, 'transit_ingress_boundary_ambiguous');
  const selected = candidates[0], boundary = selected.expression.indexOf(') || ');
  need(boundary > 5, 'transit_ingress_boundary_invalid');
  let paths; try { paths = JSON.parse('[' + selected.expression.slice(5, boundary) + ']'); } catch { need(false, 'transit_ingress_boundary_invalid'); }
  need(Array.isArray(paths) && ['/', '/assets/*', '/api/*'].every(path => paths.includes(path))
    && new Set(paths).size === paths.length && paths.every(path => typeof path === 'string'
      && /^\/[A-Za-z0-9_./-]*\*?$/u.test(path)), 'transit_ingress_boundary_invalid');
  need(!paths.some(path => path.startsWith('/transit/')), 'transit_ingress_already_configured');
  const expression = selected.expression.slice(0, boundary) + ', ' + JSON.stringify(transitSubscriptionPath) + selected.expression.slice(boundary);
  const text = currentDisk.toString(), needle = '`' + selected.expression + '`';
  need(text.split(needle).length === 2, 'transit_ingress_disk_boundary_mismatch');
  const candidate = Buffer.from(text.replace(needle, '`' + expression + '`'));
  const expected = structuredClone(before); let target = expected;
  for (const part of selected.pointer.slice(0, -1)) target = target[part];
  target[selected.pointer.at(-1)] = expression;
  const after = await adapt(candidate); verifyStructuralDelta(expected, after);
  return { candidate, before, after, changedPaths: [selected.pointer.join('.')], addedPath: transitSubscriptionPath,
    otherConfigurationIdentical: true };
}

/** The existing deferred general header must not overwrite capability privacy.
 * A preceding, request-scoped deferred header wraps that same original handler.
 * Every original route/header is retained byte-for-byte in the adapted tree. */
export async function prepareTransitSubscriptionReferrer({ currentDisk, adapt, host = '4-2.xn--p1ai' }) {
  need(Buffer.isBuffer(currentDisk) && currentDisk.length > 0 && currentDisk.length <= 131072
    && !/\{\$/u.test(currentDisk.toString()) && typeof adapt === 'function'
    && /^[a-z0-9.-]+$/u.test(host), 'transit_ingress_input_invalid');
  const before = await adapt(currentDisk), found = [];
  const visit = (value, pointer = [], hosts = []) => {
    if (Array.isArray(value)) { value.forEach((child, i) => visit(child, [...pointer, i], hosts)); return; }
    if (!value || typeof value !== 'object') return;
    if (Array.isArray(value.match)) { const local = value.match.flatMap(match => match.host ?? []); if (local.length) hosts = local; }
    if (hosts.length === 1 && hosts[0] === host && value.handler === 'headers'
      && value.response?.deferred === true && JSON.stringify(value.response.set?.['Referrer-Policy']) === '["strict-origin-when-cross-origin"]') found.push(pointer);
    Object.entries(value).forEach(([key, child]) => visit(child, [...pointer, key], hosts));
  };
  visit(before); need(found.length === 1, 'transit_referrer_boundary_ambiguous');
  const text = currentDisk.toString();
  need(!text.includes('@transit_subscription_policy'), 'transit_referrer_already_configured');
  const matcher = [...text.matchAll(/^\s*@[A-Za-z0-9_]+ expression `path\([^\r\n]+\/transit\/subscriptions\/\*[^\r\n]+`\s*$/gmu)];
  need(matcher.length === 1, 'transit_referrer_boundary_invalid');
  const blocks = [...text.slice(0, matcher[0].index).matchAll(/^([\t ]*)header[\t ]+\{[\t ]*\r?$/gmu)];
  need(blocks.length > 0, 'transit_referrer_boundary_invalid');
  const block = blocks.at(-1), between = text.slice(block.index, matcher[0].index);
  need(between.length < 8192 && /^[\t ]*Referrer-Policy[\t ]+"strict-origin-when-cross-origin"[\t ]*\r?$/mu.test(between), 'transit_referrer_boundary_invalid');
  const indent = block[1], addition = `${indent}@transit_subscription_policy path ${transitSubscriptionPath}\n${indent}header @transit_subscription_policy {\n${indent}  Referrer-Policy no-referrer\n${indent}  defer\n${indent}}\n`;
  const candidate = Buffer.from(text.slice(0, block.index) + addition + text.slice(block.index));
  const pointer = found[0];
  need(pointer.at(-2) === 'handle' && pointer.at(-1) === 0 && pointer.at(-4) === 'routes', 'transit_referrer_boundary_invalid');
  const expected = structuredClone(before); let routes = expected;
  for (const part of pointer.slice(0, -3)) routes = routes[part];
  const index = pointer.at(-3);
  // Caddy can coalesce the following encode/subroute handlers into this same
  // unconditional route. Preserve that entire chain and prepend one scope.
  need(Number.isSafeInteger(index) && Object.keys(routes[index]).join(',') === 'handle'
    && routes[index].handle.length > 0 && routes[index].handle[0].handler === 'headers', 'transit_referrer_boundary_invalid');
  // Caddy orders this positive path matcher before the existing negated/general
  // header routes. Original order and complete handlers remain unchanged.
  routes.unshift({ match: [{ path: [transitSubscriptionPath] }], handle: [{ handler: 'headers',
    response: { set: { 'Referrer-Policy': ['no-referrer'] }, deferred: true } }] });
  const after = await adapt(candidate); verifyStructuralDelta(expected, after);
  return { candidate, before, after, changedPaths: [pointer.slice(0, -3).join('.') + '.insert:0'],
    addedPath: transitSubscriptionPath, otherConfigurationIdentical: true };
}
