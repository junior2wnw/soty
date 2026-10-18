import { types } from 'node:util';

const invalid = () => {
  throw Object.assign(new Error('world_attachment_configuration_invalid'), { code: 'world_attachment_configuration_invalid' });
};

/** Private host option. It never migrates a database or changes reader mode. */
export function validateWorldAttachmentWrites(value = false) {
  if (typeof value !== 'boolean') invalid();
  return value;
}

/** Snapshot only this own host option. Do not invoke accessors or Proxy traps. */
export function captureWorldAttachmentWrites(options) {
  if (!options || typeof options !== 'object' || types.isProxy(options) || Array.isArray(options)) invalid();
  const field = Object.getOwnPropertyDescriptor(options, 'worldAttachmentWrites');
  if (field && !Object.hasOwn(field, 'value')) invalid();
  return validateWorldAttachmentWrites(field?.value);
}

/** Root process entry only; no ambient env, files, grants or client flag. */
export function loadWorldConfiguration(env) {
  if (!env || typeof env !== 'object' || types.isProxy(env) || Array.isArray(env)) invalid();
  const field = Object.getOwnPropertyDescriptor(env, 'SOTY_WORLD_ATTACHMENT_WRITES');
  if (field && !Object.hasOwn(field, 'value')) invalid();
  const flag = field ? field.value : '';
  if (typeof flag !== 'string' || !['', '0', '1'].includes(flag)) invalid();
  return Object.freeze({ worldAttachmentWrites: flag === '1' });
}
