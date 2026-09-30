/** Device and connector IDs may both contain colons. Never concatenate them. */
export const deviceKey = target => JSON.stringify([target.hostDeviceId, target.connectorId]);

/** A legacy selection can only migrate when its binding is unambiguous. An
 * absent/ambiguous selection remains absent rather than selecting another host. */
export function resolveDeviceKey(value, devices) {
  if (!value || devices.some(device => deviceKey(device) === value)) return value;
  // The live inventory cannot disambiguate a departed/revoked old target.
  // With more than one colon, even one remaining match may be another device.
  if (value.split(':').length !== 2) return value;
  const legacy = devices.filter(device => `${device.hostDeviceId}:${device.connectorId}` === value);
  return legacy.length === 1 ? deviceKey(legacy[0]) : value;
}
