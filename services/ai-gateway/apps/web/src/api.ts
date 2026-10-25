import type { Dashboard } from './types';

let csrfToken: string | undefined;

const errorMessages: Record<string, string> = {
  INFERENCE_TIMEOUT: 'Модель не успела ответить. Попробуйте другую модель или включите «Авто».',
  UPSTREAM_TIMEOUT: 'Модель не успела ответить. Попробуйте другую модель или включите «Авто».',
  UPSTREAM_UNAVAILABLE: 'Модель временно недоступна. Попробуйте другую модель.',
  INSUFFICIENT_BALANCE: 'Недостаточно средств. Пополните демо-баланс и повторите запрос.',
  INSUFFICIENT_FUNDS: 'Недостаточно средств. Пополните демо-баланс и повторите запрос.',
  BUDGET_EXCEEDED: 'Достигнут бюджет ключа. Измените лимит или выберите другой ключ.',
  RATE_LIMIT_EXCEEDED: 'Слишком много запросов. Подождите немного и повторите.',
  RATE_LIMITED: 'Слишком много запросов. Подождите немного и повторите.',
  MODEL_NOT_FOUND: 'Модель больше не доступна. Выберите модель из текущего списка.',
  MODEL_UNAVAILABLE: 'Модель временно недоступна. Выберите другую модель.',
  KEY_REVOKED: 'Этот ключ отозван. Создайте новый ключ.',
  KEY_PAUSED: 'Ключ на паузе. Возобновите его в настройках.',
  IDEMPOTENCY_CONFLICT: 'Такой запрос уже обрабатывается. Обновите данные перед новой попыткой.',
  INVALID_REQUEST: 'Проверьте введённые значения.',
  VALIDATION_ERROR: 'Проверьте введённые значения.',
  SERVICE_DAILY_BUDGET_EXHAUSTED: 'Дневной бюджет демо-сервиса исчерпан. Попробуйте позже.',
  RPM_LIMIT: 'Достигнут лимит запросов в минуту. Подождите немного.',
  TPM_LIMIT: 'Достигнут лимит токенов в минуту. Подождите немного.',
  CONCURRENCY_LIMIT: 'У ключа уже есть активные запросы. Дождитесь их завершения.',
  DAILY_BUDGET_LIMIT: 'Дневной бюджет ключа исчерпан. Измените его в настройках.',
  MONTHLY_BUDGET_LIMIT: 'Месячный бюджет ключа исчерпан. Измените его в настройках.',
  TOTAL_BUDGET_LIMIT: 'Общий бюджет ключа исчерпан. Измените его в настройках.',
  REQUEST_BUDGET_LIMIT: 'Запрос превышает разрешённый бюджет. Уменьшите его объём.',
  OUTPUT_TOKEN_LIMIT: 'Уменьшите длину ответа в настройках запроса.',
  FORBIDDEN: 'Это действие недоступно в данном кабинете.',
};

export class ApiError extends Error {
  constructor(public code: string, message: string, public status = 0) { super(message); }
}

export async function request<T>(path: string, options: RequestInit = {}, timeout = 20_000): Promise<T> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeout);
  const abort = () => controller.abort();
  options.signal?.addEventListener('abort', abort, { once: true });
  if (options.signal?.aborted) controller.abort();
  try {
    const response = await fetch(path, {
      ...options,
      credentials: 'same-origin',
      cache: 'no-store',
      signal: controller.signal,
      headers: {
        Accept: 'application/json',
        ...(options.body ? { 'Content-Type': 'application/json' } : {}),
        ...(options.method && options.method !== 'GET' && csrfToken ? { 'X-CSRF-Token': csrfToken } : {}),
        ...options.headers,
      },
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const code = String(body?.error?.code || body?.code || 'REQUEST_FAILED').toUpperCase();
      const fallback = response.status === 401 ? 'Сессия кабинета завершилась. Обновите страницу.'
        : response.status === 429 ? 'Слишком много запросов. Подождите немного.'
          : response.status === 403 ? 'Это действие недоступно в данном кабинете.'
            : response.status === 409 ? 'Данные изменились. Обновите кабинет и повторите действие.'
              : response.status === 402 ? 'Недостаточно средств. Пополните демо-баланс.'
                : response.status >= 500 ? 'Сервис временно недоступен. Попробуйте ещё раз.'
                  : 'Не удалось выполнить действие. Проверьте введённые данные.';
      throw new ApiError(code, errorMessages[code] || fallback, response.status);
    }
    if (response.status === 204) return undefined as T;
    return await response.json() as T;
  } catch (error) {
    if (error instanceof ApiError) throw error;
    if (error instanceof DOMException && error.name === 'AbortError') throw new ApiError('TIMEOUT', 'Нет ответа от сервера. Проверьте соединение и повторите.');
    throw new ApiError('NETWORK', 'Не удалось связаться с сервером. Проверьте подключение к интернету.');
  } finally {
    clearTimeout(timer);
    options.signal?.removeEventListener('abort', abort);
  }
}

let sessionPromise: Promise<unknown> | undefined;
export function ensureSession(): Promise<unknown> {
  if (!sessionPromise) sessionPromise = request<{ csrfToken?: string }>('/api/session', { method: 'POST' }).then((session) => {
    csrfToken = session.csrfToken;
    return session;
  }).catch((error) => {
    sessionPromise = undefined;
    throw error;
  });
  return sessionPromise;
}

export async function getDashboard(days: number, signal?: AbortSignal): Promise<Dashboard> {
  await ensureSession();
  try { return await request<Dashboard>(`/api/dashboard?days=${days}`, { signal }); }
  catch (error) {
    if (error instanceof ApiError && error.status === 401) {
      sessionPromise = undefined;
      await ensureSession();
      return request<Dashboard>(`/api/dashboard?days=${days}`, { signal });
    }
    throw error;
  }
}

export function mutation<T>(path: string, method: string, body?: unknown, idempotencyKey?: string): Promise<T> {
  return request<T>(path, {
    method,
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    ...(idempotencyKey ? { headers: { 'Idempotency-Key': idempotencyKey } } : {}),
  });
}

export async function installerFile(model: string, autoFallback: boolean): Promise<Blob> {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), 25_000);
  try {
    const response = await fetch('/api/installations', {
      method: 'POST', credentials: 'same-origin', cache: 'no-store', signal: controller.signal,
      headers: { 'Content-Type': 'application/json', ...(csrfToken ? { 'X-CSRF-Token': csrfToken } : {}) },
      body: JSON.stringify({ model, autoFallback }),
    });
    if (!response.ok) throw new ApiError('INSTALLER_FAILED', response.status === 401 ? 'Обновите страницу и повторите загрузку.' : response.status === 429 ? 'Слишком много установщиков. Подождите немного.' : 'Не удалось подготовить установщик. Повторите попытку.', response.status);
    return await response.blob();
  } catch (error) {
    if (error instanceof ApiError) throw error;
    throw new ApiError('NETWORK', 'Не удалось скачать установщик. Проверьте подключение к интернету.');
  } finally { clearTimeout(timer); }
}
