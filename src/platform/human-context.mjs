const closed = (value, keys) => value && typeof value === 'object' && !Array.isArray(value)
  && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const opaque = value => typeof value === 'string' && /^[A-Za-z0-9_-]{16,128}$/u.test(value);
const fail = () => { throw new TypeError('human_login_context_unavailable'); };

/** A login context contains presentation and browser binding, never application
 * tokens, redirect authority, client secrets or a caller-selected subject. */
export function parseHumanLoginContext(value, { interactionId, checkedAt } = {}) {
  if (!opaque(interactionId) || !Number.isSafeInteger(checkedAt)
    || !closed(value, ['schema', 'interactionId', 'browserNonce', 'csrf', 'client', 'scopes', 'expiresAt', 'decision'])
    || value.schema !== 'soty.human-login-context.v1' || value.interactionId !== interactionId
    || !opaque(value.browserNonce) || !opaque(value.csrf)
    || !closed(value.client, ['id', 'label'])
    || typeof value.client.id !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,127}$/u.test(value.client.id)
    || value.client.id.includes('..') || value.client.id.includes('//')
    || typeof value.client.label !== 'string' || !value.client.label.trim() || !value.client.label.isWellFormed()
    || value.client.label.length > 120 || /[\u0000-\u001f\u007f]/u.test(value.client.label)
    || !Array.isArray(value.scopes) || !value.scopes.includes('openid') || value.scopes.length > 2
    || new Set(value.scopes).size !== value.scopes.length || value.scopes.some(scope => !['openid', 'profile'].includes(scope))
    || !Number.isSafeInteger(value.expiresAt) || value.expiresAt <= checkedAt
    || value.expiresAt - checkedAt > 601000 || !['pending', 'approved', 'denied'].includes(value.decision)) fail();
  return Object.freeze({ ...value, client: Object.freeze({ ...value.client }), scopes: Object.freeze([...value.scopes]),
    remainingMs: value.expiresAt - checkedAt });
}
