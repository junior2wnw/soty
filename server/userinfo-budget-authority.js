// Host-only rate allocation. A reference is neither authentication nor a grant.
// Only the actual service supplies `current`; inbound DTOs never configure it.
const authorities = new WeakMap();
const references = new WeakMap();

function bearer(req) {
  if (req?.method !== 'GET') return null;
  const target = req.originalUrl || req.url;
  if (typeof target !== 'string' || target.length > 8192
    || !['/human-identity/userinfo', '/userinfo'].includes(target)) return null;
  if (!Array.isArray(req.rawHeaders)) return null;
  let value;
  for (let index = 0; index < req.rawHeaders.length; index += 2) {
    if (typeof req.rawHeaders[index] !== 'string' || req.rawHeaders[index].toLowerCase() !== 'authorization') continue;
    if (value !== undefined) return null;
    value = req.rawHeaders[index + 1];
  }
  if (typeof value !== 'string' || value.length > 263) return null;
  const match = /^Bearer ([A-Za-z0-9_-]{16,256})$/iu.exec(value);
  return match ? { token: match[1], header: value, target, method: req.method } : null;
}

function proof(current, token) {
  const value = current(token);
  if (value === null || value === undefined) return null;
  if (!value || typeof value !== 'object' || Object.keys(value).length !== 2
    || !/^[a-f0-9]{64}$/u.test(value.key) || !/^[a-f0-9]{64}$/u.test(value.pin)) {
    throw new Error('userinfo_budget_authority_invalid');
  }
  return value;
}

/** Closure branding is an in-process trust boundary, not a signed HTTP proof.
 * `current` must synchronously verify the real AT, binding and current actor
 * under its authoritative host fence, returning only private digests. */
export function createUserinfoBudgetAuthority(current) {
  if (typeof current !== 'function' || ['AsyncFunction', 'AsyncGeneratorFunction'].includes(current.constructor?.name)) {
    throw new Error('userinfo_budget_authority_invalid');
  }
  const authority = Object.freeze({
    capture(req) {
      const input = bearer(req);
      if (!input) return undefined;
      const checked = proof(current, input.token);
      if (!checked) return undefined;
      const reference = Object.freeze(Object.create(null));
      references.set(reference, { authority, req, input, checked });
      return reference;
    },
  });
  authorities.set(authority, current);
  return authority;
}

export function isUserinfoBudgetAuthority(value) { return authorities.has(value); }

/** Unknown, stale, reused or differently bound refs get no authenticated rate
 * allocation. They retain the original peer budget and SDK authorization. */
export function consumeUserinfoBudget(authority, req, reference) {
  const captured = reference && references.get(reference);
  if (!captured || captured.authority !== authority || captured.req !== req) return undefined;
  references.delete(reference); // One attempt, including capacity refusal.
  const input = bearer(req);
  if (!input || input.header !== captured.input.header || input.target !== captured.input.target
    || input.method !== captured.input.method) return undefined;
  const checked = proof(authorities.get(authority), input.token);
  return checked?.key === captured.checked.key && checked.pin === captured.checked.pin ? checked.key : undefined;
}
