export const INVITATION_SCHEMA = 'soty.invitations.v1';
export const FIELD_TYPES = Object.freeze(['text', 'multiline', 'number', 'email', 'phone', 'date', 'select', 'checkbox']);
export const FIELD_PRESETS = Object.freeze([
  { label: 'Фамилия', type: 'text', required: true },
  { label: 'Имя', type: 'text', required: true },
  { label: 'Отчество', type: 'text', required: false },
  { label: 'Должность', type: 'text', required: false },
  { label: 'Организация', type: 'text', required: false },
  { label: 'Телефон', type: 'phone', required: false },
  { label: 'Электронная почта', type: 'email', required: false },
  { label: 'Возраст', type: 'number', required: false, min: 0, max: 130 },
  { label: 'Комментарий', type: 'multiline', required: false },
]);
export class InvitationError extends Error {
  constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}
export function check(value, code = 'invitation_invalid', status = 400) { if (!value) throw new InvitationError(code, status); }
export const record = value => value !== null && typeof value === 'object' && !Array.isArray(value);
export function exact(value, allowed, required = allowed) {
  check(record(value) && Object.keys(value).every(key => allowed.includes(key)) && required.every(key => Object.hasOwn(value, key)));
}
export function text(value, max = 100) {
  check(typeof value === 'string' && value.isWellFormed() && value.trim().length > 0 && value.length <= max
    && !/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/u.test(value));
  return value.trim();
}
export function identity(value) { check(typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,180}$/u.test(value)); return value; }
export function appIdentity(value) { check(typeof value === 'string' && /^app-[a-f0-9]{32}$/u.test(value)); return value; }
export function invitationToken(value) { check(typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value), 'invitation_unavailable', 404); return value; }
export function fields(value) {
  check(Array.isArray(value) && value.length <= 20, 'invitation_fields_invalid');
  const keys = new Set();
  return value.map(field => {
    exact(field, ['key', 'label', 'type', 'required', 'options', 'min', 'max'], ['key', 'label', 'type', 'required']);
    check(typeof field.key === 'string' && /^f_[A-Za-z0-9_-]{1,64}$/u.test(field.key) && !keys.has(field.key), 'invitation_fields_invalid');
    keys.add(field.key);
    check(FIELD_TYPES.includes(field.type) && typeof field.required === 'boolean', 'invitation_fields_invalid');
    const result = { key: field.key, label: text(field.label, 80), type: field.type, required: field.required };
    if (field.type === 'select') {
      check(Array.isArray(field.options) && field.options.length >= 2 && field.options.length <= 20, 'invitation_fields_invalid');
      result.options = field.options.map(option => text(option, 100));
      check(new Set(result.options).size === result.options.length, 'invitation_fields_invalid');
    } else check(field.options === undefined, 'invitation_fields_invalid');
    for (const key of ['min', 'max']) if (field[key] !== undefined) {
      check(field.type === 'number' && Number.isFinite(field[key]) && Math.abs(field[key]) <= 1e12, 'invitation_fields_invalid'); result[key] = field[key];
    }
    check(result.min === undefined || result.max === undefined || result.min <= result.max, 'invitation_fields_invalid');
    return result;
  });
}
export function answers(form, value) {
  check(record(value) && Object.keys(value).every(key => form.some(field => field.key === key)), 'invitation_answers_invalid');
  const result = Object.create(null);
  for (const field of form) {
    const raw = value[field.key];
    if (raw === undefined || raw === '' || raw === false) { check(!field.required, 'invitation_required_field'); continue; }
    if (field.type === 'checkbox') { check(raw === true, 'invitation_answers_invalid'); result[field.key] = true; continue; }
    if (field.type === 'number') {
      check(typeof raw === 'number' && Number.isFinite(raw) && Math.abs(raw) <= 1e12
        && (field.min === undefined || raw >= field.min) && (field.max === undefined || raw <= field.max), 'invitation_answers_invalid');
      result[field.key] = raw; continue;
    }
    const clean = text(raw, field.type === 'multiline' ? 2000 : 300);
    if (field.type === 'email') check(/^[^\s@]+@[^\s@]+\.[^\s@]+$/u.test(clean), 'invitation_answers_invalid');
    if (field.type === 'phone') check(/^[+\d\s().-]{5,40}$/u.test(clean), 'invitation_answers_invalid');
    if (field.type === 'date') check(/^\d{4}-\d{2}-\d{2}$/u.test(clean) && Number.isFinite(Date.parse(clean))
      && new Date(clean).toISOString().slice(0, 10) === clean, 'invitation_answers_invalid');
    if (field.type === 'select') check(field.options.includes(clean), 'invitation_answers_invalid');
    result[field.key] = clean;
  }
  return result;
}
export function invitationUrl(origin, token) {
  const base = new URL(origin); check(base.origin === origin && ['http:', 'https:'].includes(base.protocol));
  return `${origin}/#invite/${invitationToken(token)}`;
}
export function parseInvitationRoute(hash) {
  const match = /^#invite\/([a-f0-9]{64})$/u.exec(hash);
  return match ? { token: match[1], route: hash } : null;
}
