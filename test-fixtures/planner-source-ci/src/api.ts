import { useCallback, useEffect, useRef, useState } from 'react';
import type { PlannerSnapshot } from '../shared/types';

export interface Invitation {
  userId: string;
  workspaceId: string;
  email: string;
  token: string;
  expiresAt: string;
}
export type PlannerMutationResult = PlannerSnapshot & {
  invitation?: Invitation;
  importWarnings?: string[];
};

export class ApiError extends Error {
  status: number;
  code: string;
  details: unknown;
  constructor(status: number, message: string, code = '', details?: unknown) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}
export const isEmbedded = () => window.location.pathname === '/embed';
export const apiPath = (path: string) =>
  isEmbedded() && path.startsWith('/api/') ? path.replace('/api/', '/api/embed/') : path;

export async function request<T>(path: string, method = 'GET', body?: unknown): Promise<T> {
  const isForm = body instanceof FormData;
  const response = await fetch(apiPath(path), {
    method,
    credentials: 'same-origin',
    headers: body && !isForm ? { 'Content-Type': 'application/json' } : undefined,
    body: body === undefined ? undefined : isForm ? (body as FormData) : JSON.stringify(body),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }));
    throw new ApiError(
      response.status,
      error.error || 'Не удалось выполнить действие',
      error.code,
      error.details,
    );
  }
  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}

export function usePlanner() {
  const [snapshot, setSnapshot] = useState<PlannerSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(0);
  const [loading, setLoading] = useState(true);
  const [connected, setConnected] = useState(true);
  const revision = useRef(-1);
  const refresh = useCallback(async (background = false) => {
    try {
      const next = await request<PlannerSnapshot>('/api/state');
      revision.current = next.revision;
      setSnapshot(next);
      setConnected(true);
      if (!background) setError(null);
    } catch (e) {
      setConnected(false);
      if (isEmbedded() && e instanceof ApiError && [401, 403].includes(e.status)) setSnapshot(null);
      if (!background) setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, []);
  useEffect(() => {
    void refresh();
    const timer = window.setInterval(() => {
      void refresh(true);
    }, 20000);
    return () => clearInterval(timer);
  }, [refresh]);
  useEffect(() => {
    if (!isEmbedded()) return;
    const completed = (event: MessageEvent) => {
      if (event.origin === location.origin && event.data?.type === 'planner-embed-ready')
        void refresh();
    };
    window.addEventListener('message', completed);
    return () => window.removeEventListener('message', completed);
  }, [refresh]);
  useEffect(() => {
    if (!snapshot?.user.id || isEmbedded()) return;
    const stream = new EventSource('/api/events');
    let pending: number | undefined;
    const update = () => {
      if (pending) clearTimeout(pending);
      pending = window.setTimeout(() => {
        void refresh(true);
      }, 120);
    };
    stream.addEventListener('change', update);
    return () => {
      stream.close();
      if (pending) clearTimeout(pending);
    };
  }, [snapshot?.user.id, refresh]);
  const mutate = useCallback(
    async (path: string, method = 'POST', body?: unknown) => {
      setBusy((n) => n + 1);
      setError(null);
      try {
        const result = await request<PlannerMutationResult>(path, method, body);
        const { invitation, importWarnings, ...next } = result;
        revision.current = next.revision;
        setSnapshot(next);
        setConnected(true);
        return {
          ...next,
          ...(invitation ? { invitation } : {}),
          ...(importWarnings ? { importWarnings } : {}),
        };
      } catch (e) {
        const err = e as ApiError;
        if (isEmbedded() && [401, 403].includes(err.status) && err.code !== 'embed_route_denied')
          setSnapshot(null);
        setError(
          err.status === 409
            ? 'Объект уже изменён. Обновите данные и проверьте свой вариант ещё раз.'
            : err.status === 403
              ? 'Для этого действия недостаточно прав.'
              : err.message,
        );
        if (err.status === 409) await refresh(true);
        throw err;
      } finally {
        setBusy((n) => n - 1);
      }
    },
    [refresh],
  );
  return { snapshot, busy: busy > 0, loading, connected, error, setError, refresh, mutate };
}

export type Mutate = ReturnType<typeof usePlanner>['mutate'];
