// A short-lived connector challenge, never a stored credential or an identity export.
export function parseDeviceLink(text, expectedOrigin) {
  const fail = () => { throw new Error('device_link_invalid'); };
  if (typeof text !== 'string' || new TextEncoder().encode(text).length > 4096) fail();
  let value; try { value = JSON.parse(text); } catch { fail(); }
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail();
  const fields = ['schema', 'origin', 'hostDeviceId', 'connectorId', 'claimCode'];
  if (Object.keys(value).length !== fields.length || fields.some(key => !Object.hasOwn(value, key))) fail();
  if (value.schema !== 'soty.device-link.v1' || value.origin !== expectedOrigin) fail();
  for (const key of ['hostDeviceId', 'connectorId']) if (typeof value[key] !== 'string' || !/^[A-Za-z0-9:_-]{1,160}$/.test(value[key])) fail();
  if (typeof value.claimCode !== 'string' || !/^[A-Za-z0-9_-]{43}$/.test(value.claimCode)) fail();
  return { hostDeviceId: value.hostDeviceId, connectorId: value.connectorId, claimCode: value.claimCode };
}
