import { canonicalJson, ConnectError, MAX_ARGS_BYTES } from '../modules/connect/server/index.mjs';

const check = (value, code) => { if (!value) throw new ConnectError(code); };

/** Combine trusted host services without changing their lane, policy or individual limits. */
export function composeConnectExtensions(extensions) {
  check(Array.isArray(extensions) && extensions.length > 0 && extensions.length <= 32, 'invalid_extensions');
  const routes = new Map();
  let lane, policy, maximum = 0, hasDefault = false;
  for (const extension of extensions) {
    const sync = typeof extension?.execute === 'function' && extension.execute.constructor.name !== 'AsyncFunction';
    const async = typeof extension?.executeAsync === 'function';
    check(extension?.operations instanceof Set && extension.operations.size > 0
      && ((sync && !extension.executeAsync) || (async && !extension.execute)), 'invalid_extension');
    const nextLane = async ? 'executeAsync' : 'execute';
    const nextPolicy = extension.challengeStorage ?? 'persistent';
    check(['persistent', 'memory'].includes(nextPolicy), 'invalid_extension_challenge_storage');
    check(lane === undefined || lane === nextLane, 'extension_lane_mismatch');
    check(policy === undefined || policy === nextPolicy, 'extension_challenge_storage_mismatch');
    lane = nextLane; policy = nextPolicy;
    const limit = extension.maxResultBytes ?? MAX_ARGS_BYTES;
    check(Number.isSafeInteger(limit) && limit >= 1024 && limit <= 8 * 1024 * 1024, 'invalid_extension_result_limit');
    hasDefault ||= extension.maxResultBytes === undefined;
    maximum = Math.max(maximum, limit);
    const execute = extension[nextLane].bind(extension);
    for (const op of extension.operations) {
      check(typeof op === 'string' && /^[a-z][a-z0-9]*(?:\.[a-z][a-zA-Z0-9]*){1,4}$/u.test(op)
        && op.length <= 100 && !routes.has(op), 'invalid_extension_operation');
      routes.set(op, { execute, limit });
    }
  }
  const routeFor = request => {
    const route = routes.get(request?.op);
    check(route, 'invalid_extension_operation');
    return route;
  };
  const resultFor = (result, limit) => {
    check(result !== null && typeof result === 'object' && !Array.isArray(result)
      && [Object.prototype, null].includes(Object.getPrototypeOf(result))
      && !Object.hasOwn(result, 'ok') && !Object.hasOwn(result, 'error'), 'invalid_extension_result');
    const bytes = Buffer.byteLength(canonicalJson(result), 'utf8');
    check(bytes <= limit && bytes <= maximum, 'extension_result_too_large');
    return result;
  };
  return Object.freeze({
    operations: new Set(routes.keys()),
    ...(lane === 'executeAsync' ? { async executeAsync(request) {
      const route = routeFor(request);
      return resultFor(await route.execute(request), route.limit);
    } } : { execute(request) {
      const route = routeFor(request);
      return resultFor(route.execute(request), route.limit);
    } }),
    ...(hasDefault && maximum === MAX_ARGS_BYTES ? {} : { maxResultBytes: maximum }),
    ...(policy === 'memory' ? { challengeStorage: 'memory' } : {}),
  });
}
