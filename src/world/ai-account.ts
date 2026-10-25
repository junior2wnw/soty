import type { WorldApi } from './types';

export type AiState = 'provisioning' | 'ready' | 'limited' | 'suspended' | 'unavailable';
export type AiLimits = Record<string, string | number | null>;
export interface AiKey {
  id: string; name: string; prefix: string; status: string; createdAt: string; lastUsedAt: string | null;
  expiresAt: string | null; limits: AiLimits; allowedModels: string[] | null; spentMicrorubles: string;
  tokens: number; policyRevision: number;
}
export interface AiAccount {
  schema: 'soty.ai-account.v1'; accountId: string; state: AiState;
  allowance: { kind: 'included'; unit: 'microrubles'; grantedMicrorubles: string; availableMicrorubles: string;
    heldMicrorubles: string; spentMicrorubles: string; pendingMicrorubles: string; limits: AiLimits };
  usage: { requests: number; tokens: number; inputTokens: number; outputTokens: number };
  models: { id: string; status: string }[]; updatedAt: string;
}
export type AiMutation = 'create' | 'update' | 'rotate' | 'revoke';
export interface AiPending { schema: 1; accountId: string; operationId: string; kind: AiMutation; args: Record<string, unknown>; createdAt: number; }
export interface AiView {
  account: AiAccount | null; keys: AiKey[] | null; loading: boolean; keysLoading: boolean; busy: boolean;
  pending: AiPending | null; secret: string | null; secretKey: AiKey | null; notice: string; error: string;
}
const moneyFields = ['dailyMicrorubles', 'monthlyMicrorubles', 'totalMicrorubles', 'maxRequestMicrorubles'];
const numberFields = ['rpm', 'tpm', 'concurrency', 'dailyTokens', 'monthlyTokens', 'maxOutputTokens'];
const pendingTtl = 24 * 60 * 60 * 1000;
const fail = (code: string): Error & { code: string } => Object.assign(new Error(code), { code });
const record = (value: unknown): value is Record<string, unknown> => Boolean(value) && typeof value === 'object' && !Array.isArray(value);
const short = (value: unknown, max = 256): value is string => typeof value === 'string' && value.length > 0 && value.length <= max;
const money = (value: unknown): value is string => typeof value === 'string' && /^(0|[1-9]\d{0,24})$/u.test(value);
const count = (value: unknown): value is number => Number.isSafeInteger(value) && Number(value) >= 0;
function limits(value: unknown): AiLimits {
  if (!record(value)) throw fail('ai_invalid_response');
  const result: AiLimits = {};
  for (const [key, item] of Object.entries(value)) {
    if (item !== null && !(moneyFields.includes(key) ? money(item) : numberFields.includes(key) && count(item))) throw fail('ai_invalid_response');
    if (!moneyFields.includes(key) && !numberFields.includes(key)) throw fail('ai_invalid_response');
    result[key] = item as string | number | null;
  }
  return result;
}
function models(value: unknown): string[] | null {
  if (value === null) return null;
  if (!Array.isArray(value) || value.length > 100 || value.some(item => !short(item, 250))) throw fail('ai_invalid_response');
  return [...new Set(value as string[])];
}
function date(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (!short(value, 64) || !Number.isFinite(Date.parse(value))) throw fail('ai_invalid_response');
  return value;
}
export function readAiKey(value: unknown): AiKey {
  if (!record(value) || !short(value.id) || !short(value.name, 100) || !short(value.prefix, 100)
      || value.purpose !== 'customer'
      || !['active', 'paused', 'revoked', 'pending_activation', 'expired'].includes(String(value.status))
      || !money(value.spentMicrorubles) || !count(value.tokens) || !count(value.policyRevision)) throw fail('ai_invalid_response');
  const createdAt = date(value.createdAt); if (!createdAt) throw fail('ai_invalid_response');
  return { id: value.id, name: value.name, prefix: value.prefix, status: String(value.status), createdAt,
    lastUsedAt: date(value.lastUsedAt), expiresAt: date(value.expiresAt), limits: limits(value.limits),
    allowedModels: models(value.allowedModels), spentMicrorubles: value.spentMicrorubles, tokens: value.tokens, policyRevision: value.policyRevision };
}
export function readAiAccount(value: unknown, accountId: string): AiAccount {
  if (!record(value) || value.schema !== 'soty.ai-account.v1' || value.accountId !== accountId
      || !['provisioning', 'ready', 'limited', 'suspended', 'unavailable'].includes(String(value.state))
      || !record(value.allowance) || value.allowance.kind !== 'included' || value.allowance.unit !== 'microrubles'
      || !record(value.usage) || !Array.isArray(value.models) || value.models.length > 300) throw fail('ai_invalid_response');
  const allowance = value.allowance;
  for (const field of ['grantedMicrorubles', 'availableMicrorubles', 'heldMicrorubles', 'spentMicrorubles', 'pendingMicrorubles']) {
    if (!money(allowance[field])) throw fail('ai_invalid_response');
  }
  for (const field of ['requests', 'tokens', 'inputTokens', 'outputTokens']) if (!count(value.usage[field])) throw fail('ai_invalid_response');
  const modelList = value.models.map(item => {
    if (!record(item) || !short(item.id, 250) || !short(item.status, 64)) throw fail('ai_invalid_response');
    return { id: item.id, status: item.status };
  });
  const updatedAt = date(value.updatedAt); if (!updatedAt) throw fail('ai_invalid_response');
  // Copy only public fields. Provider/default credentials never enter the view.
  return { schema: 'soty.ai-account.v1', accountId, state: value.state as AiState,
    allowance: { kind: 'included', unit: 'microrubles', grantedMicrorubles: String(allowance.grantedMicrorubles),
      availableMicrorubles: String(allowance.availableMicrorubles), heldMicrorubles: String(allowance.heldMicrorubles),
      spentMicrorubles: String(allowance.spentMicrorubles), pendingMicrorubles: String(allowance.pendingMicrorubles), limits: limits(allowance.limits) },
    usage: { requests: Number(value.usage.requests), tokens: Number(value.usage.tokens), inputTokens: Number(value.usage.inputTokens), outputTokens: Number(value.usage.outputTokens) },
    models: modelList, updatedAt };
}
/** Parse ₽ as decimal text, without a floating point round-trip. */
export function aiRublesToMicrorubles(value: string): string | null {
  const text = value.trim().replace(',', '.'); if (!text) return null;
  if (!/^\d{1,12}(?:\.\d{1,6})?$/u.test(text)) throw fail('ai_invalid_amount');
  const [whole = '0', fraction = ''] = text.split('.');
  return (BigInt(whole) * 1_000_000n + BigInt(fraction.padEnd(6, '0'))).toString();
}
export function aiRublesInput(value: string | number | null | undefined): string {
  if (!money(value)) return '';
  const amount = BigInt(value); return `${amount / 1_000_000n}${amount % 1_000_000n ? `.${String(amount % 1_000_000n).padStart(6, '0').replace(/0+$/u, '')}` : ''}`;
}
export function aiMoneyLabel(value: string): string {
  if (!money(value)) return '—';
  const amount = BigInt(value), whole = (amount / 1_000_000n).toLocaleString('ru-RU');
  const fraction = String(amount % 1_000_000n).padStart(6, '0').slice(0, 2).replace(/0+$/u, '');
  return amount > 0n && amount < 10_000n ? '< 0,01 ₽' : `${whole}${fraction ? `,${fraction}` : ''} ₽`;
}
export function aiErrorLabel(reason: unknown): string {
  const code = record(reason) && typeof reason.code === 'string' ? reason.code : '';
  return ({ ai_context_changed: 'Аккаунт изменился. Откройте настройки заново.', ACTIVE_PROFILE_CHANGED: 'Аккаунт изменился. Откройте настройки заново.',
    ai_unavailable: 'Сервис ИИ временно недоступен. Соты продолжат работать.',
    ai_maintenance: 'Обновляем доступ к API. Прежний запрос сохранён и будет проверен после обновления.',
    ai_invalid_response: 'Не удалось подтвердить данные ИИ. Обновите состояние.',
    ai_invalid_request: 'Проверьте название, лимит и срок действия ключа.', ai_invalid_amount: 'Укажите сумму в рублях: не больше 6 знаков после запятой.',
    ai_key_unavailable: 'Ключ изменился или больше недоступен. Обновите список.',
    ai_key_changed: 'Настройки ключа изменились в другом окне. Ввод сохранён. Откройте актуальные настройки ключа перед сохранением.',
    ai_request_conflict: 'Результат этого запроса требует проверки. Новое изменение не отправлено.',
    ai_storage_unavailable: 'Не удалось сохранить запрос на этом устройстве. Изменение не отправлено.',
    ai_pending_unresolved: 'Сначала проверьте результат предыдущего изменения.',
  } as Record<string, string>)[code] ?? 'Не удалось получить ответ. Проверьте соединение и повторите проверку.';
}
function mutationArgs(kind: AiMutation, value: unknown): Record<string, unknown> {
  if (!record(value)) throw fail('ai_invalid_request');
  const allowed = kind === 'create' ? ['name', 'limits', 'expiresAt', 'allowedModels'] : kind === 'update' ? ['keyId', 'name', 'limits', 'expiresAt', 'allowedModels', 'status', 'expectedPolicyRevision'] : ['keyId'];
  if (Object.keys(value).some(key => !allowed.includes(key))) throw fail('ai_invalid_request');
  const result: Record<string, unknown> = {};
  if (kind !== 'create') { if (!short(value.keyId)) throw fail('ai_invalid_request'); result.keyId = value.keyId; }
  if (kind === 'create' || kind === 'update') {
    if (kind === 'create' || value.name !== undefined) {
      if (!short(value.name, 100) || !value.name.trim()) throw fail('ai_invalid_request'); result.name = value.name.trim();
    }
    if (value.limits !== undefined) result.limits = limits(value.limits);
    if (value.expiresAt !== undefined) result.expiresAt = date(value.expiresAt);
    if (value.allowedModels !== undefined) result.allowedModels = models(value.allowedModels);
    if (kind === 'update' && value.status !== undefined) {
      if (!['active', 'paused'].includes(String(value.status))) throw fail('ai_invalid_request'); result.status = value.status;
    }
    if (kind === 'update' && value.expectedPolicyRevision !== undefined) {
      if (!count(value.expectedPolicyRevision) || value.expectedPolicyRevision < 1) throw fail('ai_invalid_request'); result.expectedPolicyRevision = value.expectedPolicyRevision;
    }
  }
  return result;
}

export function createAiAccountController(options: {
  api: WorldApi; accountId: string; isCurrent(): boolean; onChange(): void;
  storage: Pick<Storage, 'getItem' | 'setItem' | 'removeItem'>; locks?: Pick<LockManager, 'request'>;
  randomId?: () => string; clock?: () => number;
}) {
  const { api, accountId, storage } = options, storageKey = `soty.ai.pending.v1:${encodeURIComponent(accountId)}`;
  const clock = options.clock ?? Date.now, randomId = options.randomId ?? (() => crypto.randomUUID());
  const abort = new AbortController();
  const state: AiView = { account: null, keys: null, loading: false, keysLoading: false, busy: false, pending: null, secret: null, secretKey: null, notice: '', error: '' };
  let disposed = false, accountRead = 0, keysRead = 0, secretGeneration = 0;
  let errorSource: 'account' | 'keys' | 'mutation' | 'copy' | null = null;
  const current = (): boolean => !disposed && !abort.signal.aborted && options.isCurrent();
  const emit = (): void => { if (current()) options.onChange(); };
  // The Connect transport owns its network cancellation. Abort this mount's
  // wait immediately; late RPC results can never recover the mounted authority.
  function wait<T>(promise: Promise<T>): Promise<T | undefined> {
    return new Promise((resolve, reject) => {
      const stop = (): void => resolve(undefined); abort.signal.addEventListener('abort', stop, { once: true });
      promise.then(value => { abort.signal.removeEventListener('abort', stop); resolve(value); }, reason => { abort.signal.removeEventListener('abort', stop); reject(reason); });
      if (abort.signal.aborted) stop();
    });
  }
  const call = <T>(op: string, args: Record<string, unknown> = {}): Promise<T | undefined> => {
    if (!current()) return Promise.resolve(undefined);
    return wait(api.request<T>(op, { ...args, expectedAccountId: accountId }));
  };
  function stored(): AiPending | null {
    try {
      const text = storage.getItem(storageKey); if (!text) return null;
      if (text.length > 20_000) throw fail('ai_storage_unavailable');
      const value: unknown = JSON.parse(text);
      if (!record(value) || value.schema !== 1 || value.accountId !== accountId || !short(value.operationId, 128)
          || !['create', 'update', 'rotate', 'revoke'].includes(String(value.kind)) || !count(value.createdAt)
          || Object.keys(value).some(key => !['schema', 'accountId', 'operationId', 'kind', 'args', 'createdAt'].includes(key))) throw fail('ai_storage_unavailable');
      if (clock() - value.createdAt > pendingTtl || value.createdAt > clock() + 60_000) {
        storage.removeItem(storageKey); state.notice = 'Срок локальной проверки прежнего запроса истёк. Проверьте список ключей перед новым изменением.'; return null;
      }
      return { schema: 1, accountId, operationId: value.operationId, kind: value.kind as AiMutation,
        args: mutationArgs(value.kind as AiMutation, value.args), createdAt: value.createdAt };
    } catch { throw fail('ai_storage_unavailable'); }
  }
  async function locked<T>(work: () => T | Promise<T>): Promise<T> {
    if (!options.locks) throw fail('ai_storage_unavailable');
    return options.locks.request(storageKey, work);
  }
  async function loadAccount(): Promise<void> {
    if (!current()) return; const sequence = ++accountRead; state.loading = true; emit();
    try {
      const value = await call<unknown>('ai.account.get');
      if (!current() || sequence !== accountRead) return;
      state.account = readAiAccount(value, accountId); if (errorSource === 'account') { state.error = ''; errorSource = null; }
    } catch (reason) { if (current() && sequence === accountRead) { state.error = aiErrorLabel(reason); errorSource = 'account'; } }
    finally { if (current() && sequence === accountRead) { state.loading = false; emit(); } }
  }
  async function loadKeys(): Promise<boolean> {
    if (!current()) return false; const sequence = ++keysRead; state.keysLoading = true; emit();
    try {
      const response = await call<unknown>('ai.keys.list');
      if (!current() || sequence !== keysRead) return false;
      if (!record(response) || !Array.isArray(response.keys) || response.keys.length > 1000) throw fail('ai_invalid_response');
      state.keys = response.keys.map(readAiKey);
      if (errorSource === 'keys') { state.error = ''; errorSource = null; }
      const secretKey = state.secretKey && state.keys.find(item => item.id === state.secretKey?.id);
      if (state.secret && secretKey && ['revoked', 'expired'].includes(secretKey.status)) {
        secretGeneration++; state.secret = null; state.secretKey = null; state.notice = 'Этот ключ уже недоступен. Полное значение скрыто.';
      }
      return true;
    } catch (reason) { if (current() && sequence === keysRead) { state.error = aiErrorLabel(reason); errorSource = 'keys'; } return false; }
    finally { if (current() && sequence === keysRead) { state.keysLoading = false; emit(); } }
  }
  async function dispatch(pending: AiPending): Promise<void> {
    if (!current()) return;
    state.busy = true; state.error = ''; errorSource = null; state.notice = ''; state.secret = null; state.secretKey = null; emit();
    try {
      const response = await call<unknown>(`ai.keys.${pending.kind}`, { ...pending.args, operationId: pending.operationId });
      if (!current()) return;
      if (!record(response)) throw fail('ai_invalid_response');
      const key = readAiKey(response.key);
      if (pending.args.keyId !== undefined && key.id !== pending.args.keyId) throw fail('ai_invalid_response');
      const secret = response.secret;
      if ((pending.kind === 'create' || pending.kind === 'rotate') && secret !== null && secret !== undefined && (!short(secret, 512) || response.replayed === true)) throw fail('ai_invalid_response');
      await locked(() => { const previous = stored(); if (previous?.operationId === pending.operationId) storage.removeItem(storageKey); });
      if (!current()) return;
      state.pending = null;
      // A replay is a historical receipt. The preceding keys read remains the
      // current policy (it may already be paused/revoked by another device).
      if (response.replayed !== true) state.keys = [key, ...(state.keys ?? []).filter(item => item.id !== key.id)];
      state.secret = typeof secret === 'string' ? secret : null; state.secretKey = state.secret ? key : null; secretGeneration++;
      state.notice = state.secret ? '' : pending.kind === 'create' || pending.kind === 'rotate'
        ? 'Изменение подтверждено. Полное значение ключа уже недоступно. Чтобы получить новое, выберите «Заменить» у этого ключа.'
        : pending.kind === 'revoke' ? 'Ключ отозван.' : 'Настройки ключа сохранены.';
      void loadAccount(); void loadKeys();
    } catch (reason) {
      if (!current()) return;
      const code = record(reason) ? reason.code : '';
      if (['ai_invalid_request', 'ai_key_unavailable', 'ai_key_changed', 'ai_context_changed', 'ACTIVE_PROFILE_CHANGED'].includes(String(code))) {
        try { await locked(() => { if (stored()?.operationId === pending.operationId) storage.removeItem(storageKey); }); state.pending = null; } catch { /* Keep the durable request for readback. */ }
        if (current()) { state.error = aiErrorLabel(reason); errorSource = 'mutation'; }
        if (code === 'ai_key_changed') await loadKeys();
      } else {
        state.notice = 'Ответ на изменение не подтверждён. Проверяем прежний запрос; новый ключ не создаётся.';
        state.error = code === 'ai_storage_unavailable' ? 'Не удалось сохранить подтверждение. Проверим результат прежнего запроса.' : aiErrorLabel(reason);
        errorSource = 'mutation';
        await loadKeys();
      }
    } finally { if (current()) { state.busy = false; emit(); } }
  }
  async function mutate(kind: AiMutation, args: Record<string, unknown>): Promise<void> {
    if (!current() || state.busy) return;
    try {
      const safeArgs = mutationArgs(kind, args);
      const pending = await locked(() => {
        if (!current()) return null;
        const previous = stored(); if (previous) { state.pending = previous; throw fail('ai_pending_unresolved'); }
        const next: AiPending = { schema: 1, accountId, operationId: randomId(), kind, args: safeArgs, createdAt: clock() };
        storage.setItem(storageKey, JSON.stringify(next)); return next;
      });
      if (!current() || !pending) return;
      state.pending = pending; await dispatch(pending);
    } catch (reason) { if (current()) { state.error = aiErrorLabel(reason); errorSource = 'mutation'; emit(); } }
  }
  async function reconcile(): Promise<void> {
    if (!current() || state.busy) return;
    try {
      state.pending = stored(); emit();
      if (!state.pending || !await loadKeys() || !current()) return;
      const pending = state.pending;
      await locked(() => { const previous = stored(); if (!previous || JSON.stringify(previous) !== JSON.stringify(pending)) throw fail('ai_pending_unresolved'); });
      if (current()) await dispatch(pending);
    } catch (reason) { if (current()) { state.error = aiErrorLabel(reason); errorSource = 'mutation'; emit(); } }
  }
  return {
    read: (): Readonly<AiView> => state,
    async start(): Promise<void> { await Promise.all([loadAccount(), reconcile()]); },
    loadAccount, loadKeys, mutate, reconcile,
    forgetSecret(): void { secretGeneration++; state.secret = null; state.secretKey = null; emit(); },
    async copySecret(write: (value: string) => Promise<void>): Promise<void> {
      if (!current() || !state.secret) return; const generation = secretGeneration;
      try { await write(state.secret); if (current() && secretGeneration === generation) { if (errorSource === 'copy') { state.error = ''; errorSource = null; } state.notice = 'Ключ скопирован.'; emit(); } }
      catch { if (current() && secretGeneration === generation) { state.error = 'Не удалось скопировать ключ. Выделите его и скопируйте вручную.'; errorSource = 'copy'; emit(); } }
    },
    dispose(): void { disposed = true; abort.abort(); accountRead++; keysRead++; secretGeneration++; state.secret = null; state.secretKey = null; state.account = null; state.keys = null; state.pending = null; },
  };
}
