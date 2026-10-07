import {
  createServer as createHttpServer,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http';
import { readFile, stat } from 'node:fs/promises';
import { resolve, dirname, extname, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { randomBytes, createHash, timingSafeEqual } from 'node:crypto';
import { AsyncLocalStorage } from 'node:async_hooks';
import { PlannerStore, uid } from './store.ts';
import { assertPlannerRpStartup } from './soty-rp-format.ts';
import { AgentKeys } from './agent-auth.ts';
import { PlannerAgentBackend } from './agent-backend.ts';
import { readWorkItemProof, workItemProofProfile, proofHash } from './agent-receipts.ts';
import {
  createPlannerEmbed,
  embedApiAllowed,
  type PlannerEmbedConfiguration,
} from './soty-embed.ts';
import { handlePlannerMcp } from './mcp.ts';
import { PlannerScheduler, validateWebhookUrl } from './scheduler.ts';
import { importContent, exportContent } from './imports.ts';
import {
  ApiError,
  object,
  parse,
  text,
  id,
  dependencySchema,
  resourceSchema,
  ruleSchema,
  typeSchema,
  timezone,
  entitySchema,
  templateSchema,
} from './validation.ts';
import {
  validateDependency,
  applyTemplate,
  suggestFromText,
  evaluateSignals,
} from '../shared/engine.ts';
import type {
  Entity,
  PlannerSnapshot,
  User,
  Role,
  Source,
  TypeDefinition,
  Dependency,
  Resource,
  Rule,
} from '../shared/types.ts';

const projectRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const JSON_LIMIT = 2 * 1024 * 1024,
  FILE_LIMIT = 20 * 1024 * 1024;
const verifiedBridgeBodies = new WeakMap<IncomingMessage, Buffer>();
const json = (res: ServerResponse, status: number, value: unknown) => {
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(value));
};
async function bodyBuffer(req: IncomingMessage, max = JSON_LIMIT) {
  const verified = verifiedBridgeBodies.get(req);
  if (verified) {
    if (verified.length > max) throw new ApiError(413, 'Запрос слишком большой');
    return verified;
  }
  const declared = Number(req.headers['content-length'] ?? 0);
  if (declared > max) throw new ApiError(413, 'Запрос слишком большой');
  let size = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of req) {
    const data = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += data.length;
    if (size > max) throw new ApiError(413, 'Запрос слишком большой');
    chunks.push(data);
  }
  return Buffer.concat(chunks);
}
async function bodyJson(req: IncomingMessage) {
  const buffer = await bodyBuffer(req);
  if (!buffer.length) return {};
  if (!(req.headers['content-type'] ?? '').includes('application/json'))
    throw new ApiError(415, 'Ожидается application/json');
  try {
    return object(JSON.parse(buffer.toString('utf8')));
  } catch (e) {
    if (e instanceof ApiError) throw e;
    throw new ApiError(400, 'Некорректный JSON');
  }
}
function sessionCookie(req: IncomingMessage) {
  const cookie = req.headers.cookie ?? '';
  return (
    cookie
      .split(';')
      .map((v) => v.trim())
      .find((v) => v.startsWith('planner_session='))
      ?.slice('planner_session='.length) ?? ''
  );
}
function setSession(req: IncomingMessage, res: ServerResponse, token: string, maxAge = 604800) {
  const secure = (req.headers.origin ?? '').startsWith('https://') ? '; Secure' : '';
  res.setHeader(
    'Set-Cookie',
    `planner_session=${token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${maxAge}${secure}`,
  );
}
function expectedRevision(raw: Record<string, unknown>, state: PlannerSnapshot) {
  if (raw.revision !== undefined && Number(raw.revision) !== state.revision)
    throw new ApiError(409, 'Данные пространства изменились', 'version_conflict');
}
function multipart(buffer: Buffer, contentType: string) {
  const boundary = contentType
    .match(/boundary=(?:"([^"]+)"|([^;\s]+))/)
    ?.slice(1)
    .find(Boolean);
  if (!boundary || boundary.length > 200) throw new ApiError(400, 'Некорректная multipart граница');
  const marker = Buffer.from(`--${boundary}`);
  const parts: { name: string; filename?: string; mime: string; data: Buffer }[] = [];
  let cursor = buffer.indexOf(marker);
  if (cursor !== 0) throw new ApiError(400, 'Некорректный multipart');
  while (cursor >= 0) {
    cursor += marker.length;
    if (buffer.subarray(cursor, cursor + 2).toString() === '--') break;
    if (buffer.subarray(cursor, cursor + 2).toString() !== '\r\n')
      throw new ApiError(400, 'Некорректный multipart');
    cursor += 2;
    const headEnd = buffer.indexOf('\r\n\r\n', cursor);
    if (headEnd < 0 || headEnd - cursor > 8000)
      throw new ApiError(400, 'Некорректные заголовки файла');
    const headers = buffer.subarray(cursor, headEnd).toString('utf8');
    const name = headers.match(/name="([^"]+)"/)?.[1];
    const filename = headers.match(/filename="([^"]*)"/)?.[1];
    if (!name) throw new ApiError(400, 'Нет имени части multipart');
    const next = buffer.indexOf(Buffer.from(`\r\n--${boundary}`), headEnd + 4);
    if (next < 0) throw new ApiError(400, 'Multipart не завершён');
    parts.push({
      name,
      filename,
      mime: headers.match(/content-type:\s*([^\r\n]+)/i)?.[1] ?? 'application/octet-stream',
      data: buffer.subarray(headEnd + 4, next),
    });
    if (parts.length > 10) throw new ApiError(400, 'Слишком много частей multipart');
    cursor = next + 2;
  }
  return parts;
}
export interface ServerOptions {
  dbPath?: string;
  host?: string;
  port?: number;
  development?: boolean;
  scheduler?: boolean;
  embed?: PlannerEmbedConfiguration;
}
export async function createPlannerServer(options: ServerOptions = {}) {
  const host = options.host ?? '127.0.0.1';
  const port = options.port ?? 4317;
  const localBinding = ['127.0.0.1', '::1', 'localhost'].includes(host);
  const databasePath = options.dbPath ?? resolve(projectRoot, 'data', 'planner.sqlite');
  assertPlannerRpStartup(databasePath);
  const store = new PlannerStore(databasePath);
  const scheduler = new PlannerScheduler(store);
  const agentKeys = new AgentKeys(store);
  const embeddedUsers = new AsyncLocalStorage<User>();
  let embed: ReturnType<typeof createPlannerEmbed> | undefined;
  try {
    embed = options.embed ? createPlannerEmbed(store, options.embed) : undefined;
  } catch (error) {
    store.close();
    throw error;
  }
  store.db.exec(
    'CREATE TABLE IF NOT EXISTS webhook_ingest (key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, received_at TEXT NOT NULL)',
  );
  const columns = store.db.prepare('PRAGMA table_info(webhook_connections)').all() as {
    name: string;
  }[];
  if (!columns.some((c) => c.name === 'created_by'))
    store.db.exec('ALTER TABLE webhook_connections ADD COLUMN created_by TEXT');
  let vite: Awaited<ReturnType<(typeof import('vite'))['createServer']>> | undefined;
  if (options.development) {
    const { createServer } = await import('vite');
    vite = await createServer({
      root: projectRoot,
      server: { middlewareMode: true, hmr: false },
      appType: 'spa',
    });
  }
  const activeEvents = new Set<ServerResponse>();
  function validHost(req: IncomingMessage) {
    try {
      if (embed) return embed.isEmbed(req) || embed.isNative(req);
      const hostname = new URL(`http://${req.headers.host ?? ''}`).hostname;
      return !localBinding || ['127.0.0.1', 'localhost', '[::1]'].includes(hostname);
    } catch {
      return false;
    }
  }
  function localAllowed(req: IncomingMessage) {
    return (
      !embed?.isEmbed(req) &&
      localBinding &&
      ['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(req.socket.remoteAddress ?? '') &&
      validHost(req)
    );
  }
  function userFor(req: IncomingMessage, required = true): User | undefined {
    const embeddedUser = embeddedUsers.getStore();
    if (embeddedUser) return embeddedUser;
    const token = sessionCookie(req);
    const user = token ? store.session(token) : undefined;
    if (user) return user;
    if (localAllowed(req)) return store.localUser();
    if (required) throw new ApiError(401, 'Войдите в аккаунт', 'unauthenticated');
    return undefined;
  }
  function checkOrigin(req: IncomingMessage) {
    if (['GET', 'HEAD', 'OPTIONS'].includes(req.method ?? 'GET')) return;
    const origin = req.headers.origin;
    if (!origin) throw new ApiError(403, 'Для изменения требуется Origin приложения', 'csrf');
    let originUrl: URL;
    try {
      originUrl = new URL(origin);
    } catch {
      throw new ApiError(403, 'Некорректный Origin', 'csrf');
    }
    if (originUrl.host !== req.headers.host || !['http:', 'https:'].includes(originUrl.protocol))
      throw new ApiError(403, 'Запрос из другого источника запрещён', 'csrf');
    const site = req.headers['sec-fetch-site'];
    if (site && site !== 'same-origin' && site !== 'none')
      throw new ApiError(403, 'Cross-site запрос запрещён', 'csrf');
  }
  function snapshotAfter(user: User) {
    if (!store.isWorkspaceScoped()) scheduler.tick();
    return store.snapshot(user);
  }
  function agentIdentity(req: IncomingMessage) {
    const authorization = req.headers.authorization;
    if (authorization) {
      if (!authorization.startsWith('Bearer '))
        throw new ApiError(401, 'Ожидается Bearer ключ MCP', 'unauthenticated');
      return agentKeys.authenticate(authorization.slice(7));
    }
    return { user: userFor(req)!, scope: {} };
  }
  function agentOrigin(req: IncomingMessage) {
    if (req.headers.origin) {
      let origin: URL;
      try {
        origin = new URL(req.headers.origin);
      } catch {
        throw new ApiError(403, 'Некорректный Origin', 'csrf');
      }
      if (!['http:', 'https:'].includes(origin.protocol) || origin.host !== req.headers.host)
        throw new ApiError(403, 'MCP запрос из другого источника запрещён', 'csrf');
    }
    const site = req.headers['sec-fetch-site'];
    if (site && !['same-origin', 'none'].includes(String(site)))
      throw new ApiError(403, 'Cross-site MCP запрос запрещён', 'csrf');
  }
  async function api(req: IncomingMessage, res: ServerResponse, url: URL) {
    const method = req.method ?? 'GET',
      parts = url.pathname
        .split('/')
        .filter(Boolean)
        .map((v) => {
          try {
            return decodeURIComponent(v);
          } catch {
            throw new ApiError(400, 'Некорректный URL');
          }
        });
    if (parts[1] === 'webhooks' && method === 'POST') {
      const workspaceId = parse(id, parts[2]);
      const connection = store.db
        .prepare('SELECT token_hash,created_by FROM webhook_connections WHERE workspace_id=?')
        .get(workspaceId) as { token_hash: string | null; created_by: string | null } | undefined;
      const token = (req.headers.authorization ?? '').replace(/^Bearer /, '');
      const digest = createHash('sha256').update(token).digest();
      if (
        !connection?.token_hash ||
        !token ||
        !timingSafeEqual(digest, Buffer.from(connection.token_hash, 'hex'))
      )
        throw new ApiError(401, 'Недействительный ключ webhook', 'unauthenticated');
      const actor = connection.created_by ? store.user(connection.created_by) : undefined;
      if (!actor || !['owner', 'editor'].includes(store.role(actor.id, workspaceId) ?? ''))
        throw new ApiError(403, 'Доступ webhook отключён');
      const raw = await bodyJson(req);
      const requestId = text(req.headers['idempotency-key'] ?? raw.requestId);
      if (!requestId || requestId.length > 160) throw new ApiError(400, 'Нужен Idempotency-Key');
      if (!Array.isArray(raw.entities) || raw.entities.length < 1 || raw.entities.length > 100)
        throw new ApiError(400, 'Нужны от 1 до 100 entities');
      const fingerprint = createHash('sha256').update(JSON.stringify(raw)).digest('hex');
      const ingestKey = `${workspaceId}:${requestId}`;
      const prior = store.db
        .prepare('SELECT fingerprint FROM webhook_ingest WHERE key=?')
        .get(ingestKey) as { fingerprint: string } | undefined;
      if (prior) {
        if (prior.fingerprint !== fingerprint)
          throw new ApiError(409, 'Ключ уже использован для другого запроса');
        return json(res, 200, { ok: true, duplicate: true });
      }
      store.change(
        actor,
        workspaceId,
        'webhook-ingest',
        'Получены данные подключённого источника',
        (state) => {
          const changedIds: string[] = [];
          for (const item of raw.entities as unknown[]) {
            const incoming = object(item);
            const externalId = text(incoming.externalId);
            if (!externalId || externalId.length > 500)
              throw new ApiError(400, 'Каждому объекту нужен externalId');
            const existing = state.entities.find(
              (e) =>
                e.workspaceId === workspaceId &&
                e.source.kind === 'webhook' &&
                e.source.externalId === externalId,
            );
            const receivedAt = new Date().toISOString();
            const source: Source = {
              kind: 'webhook',
              label: 'Webhook',
              externalId,
              observedAt: text(incoming.observedAt, receivedAt),
              receivedAt,
              staleAfterMinutes:
                typeof incoming.staleAfterMinutes === 'number'
                  ? incoming.staleAfterMinutes
                  : undefined,
            };
            if (existing) {
              if (incoming.version !== undefined && Number(incoming.version) !== existing.version)
                throw new ApiError(409, 'Объект изменился', 'version_conflict');
              const allowed = [
                'title',
                'description',
                'plan',
                'actual',
                'forecast',
                'status',
                'dueAt',
                'fields',
                'tags',
              ];
              const patch = Object.fromEntries(
                allowed.filter((k) => incoming[k] !== undefined).map((k) => [k, incoming[k]]),
              );
              const before = structuredClone(existing);
              const next = parse(entitySchema, {
                ...existing,
                ...patch,
                ...('forecast' in patch
                  ? { forecastProvenance: patch.forecast ? 'manual' : undefined }
                  : {}),
                source,
                updatedAt: receivedAt,
                version: existing.version + 1,
              }) as Entity;
              store.validateEntityReferences(next, state);
              state.entities[state.entities.indexOf(existing)] = next;
              store.audit(
                state,
                actor.id,
                workspaceId,
                'webhook-entity-update',
                'Обновление источника',
                { entityId: next.id, before, after: next },
              );
              changedIds.push(next.id);
            } else {
              const next = store.makeEntity(
                { ...incoming, workspaceId, source, ownerId: actor.id, parentId: null },
                state,
                actor,
              );
              state.entities.push(next);
              changedIds.push(next.id);
            }
          }
          store.db
            .prepare('INSERT INTO webhook_ingest(key,fingerprint,received_at) VALUES(?,?,?)')
            .run(ingestKey, fingerprint, new Date().toISOString());
          store.db
            .prepare('UPDATE webhook_connections SET status=?,updated_at=? WHERE workspace_id=?')
            .run('inbound-received', new Date().toISOString(), workspaceId);
          return { after: { changedIds, requestId } };
        },
      );
      scheduler.tick();
      return json(res, 200, { ok: true, duplicate: false });
    }
    checkOrigin(req);
    if (parts[1] === 'auth') {
      if (parts[2] === 'me' && method === 'GET') {
        const user = userFor(req)!;
        return json(res, 200, {
          user,
          mode: user.local ? 'local' : 'authenticated',
          localModeAvailable: localAllowed(req),
        });
      }
      if (parts[2] === 'register' && method === 'POST') {
        const result = store.register(await bodyJson(req), userFor(req, false));
        setSession(req, res, result.token);
        return json(res, 201, snapshotAfter(result.user));
      }
      if (parts[2] === 'login' && method === 'POST') {
        const result = store.login(await bodyJson(req), req.socket.remoteAddress ?? '');
        setSession(req, res, result.token);
        return json(res, 200, snapshotAfter(result.user));
      }
      if (parts[2] === 'logout' && method === 'POST') {
        store.logout(sessionCookie(req));
        setSession(req, res, '', 0);
        return json(res, 200, { ok: true });
      }
      throw new ApiError(404, 'Маршрут авторизации не найден');
    }
    const user = userFor(req)!;
    if (parts[1] === 'state' && method === 'GET') return json(res, 200, store.snapshot(user));
    if (parts[1] === 'events' && method === 'GET') {
      if (activeEvents.size >= 100) throw new ApiError(503, 'Слишком много соединений событий');
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });
      res.write(
        `event: ready\ndata: ${JSON.stringify({ revision: store.snapshot(user).revision })}\n\n`,
      );
      activeEvents.add(res);
      const listener = (revision: number) =>
        res.write(`event: change\ndata: ${JSON.stringify({ revision })}\n\n`);
      store.on('change', listener);
      const heartbeat = setInterval(() => res.write(': heartbeat\n\n'), 20000);
      heartbeat.unref();
      req.on('close', () => {
        clearInterval(heartbeat);
        store.off('change', listener);
        activeEvents.delete(res);
      });
      return;
    }
    if (parts[1] === 'history' && method === 'GET')
      return json(res, 200, store.history(user, url.searchParams.get('at') ?? ''));
    if (parts[1] === 'audit' && method === 'GET') {
      const snapshot = store.snapshot(user);
      const workspaceId = url.searchParams.get('workspaceId');
      if (workspaceId) store.requireRole(user, workspaceId);
      return json(
        res,
        200,
        snapshot.audit.filter((a) => !workspaceId || a.workspaceId === workspaceId),
      );
    }
    if (parts[1] === 'search' && method === 'GET') {
      const workspaceId = url.searchParams.get('workspaceId');
      if (workspaceId) store.requireRole(user, workspaceId);
      const q = (url.searchParams.get('q') ?? '').toLocaleLowerCase('ru');
      return json(
        res,
        200,
        store
          .snapshot(user)
          .entities.filter(
            (e) =>
              (!workspaceId || e.workspaceId === workspaceId) &&
              `${e.title} ${e.description} ${e.tags.join(' ')} ${JSON.stringify(e.fields)}`
                .toLocaleLowerCase('ru')
                .includes(q),
          ),
      );
    }
    if (parts[1] === 'export' && method === 'GET') {
      const workspaceId = parse(id, url.searchParams.get('workspaceId'));
      const output = exportContent(
        store.snapshot(user),
        workspaceId,
        url.searchParams.get('format') ?? 'json',
      );
      res.writeHead(200, {
        'Content-Type': output.mime,
        'Content-Disposition': `attachment; filename="timeline.${output.extension}"`,
        'Cache-Control': 'no-store',
        'X-Planner-Warnings': encodeURIComponent(JSON.stringify(output.warnings)),
      });
      res.end(output.content);
      return;
    }
    if (parts[1] === 'import' && method === 'POST') {
      const result = importContent(store, user, await bodyJson(req));
      return json(res, 200, { ...snapshotAfter(user), importWarnings: result.importWarnings });
    }
    if (parts[1] === 'settings' && ['PATCH', 'POST'].includes(method)) {
      store.updateSettings(user, await bodyJson(req));
      return json(res, 200, snapshotAfter(user));
    }
    if (parts[1] === 'entities') {
      if (method === 'POST' && !parts[2]) {
        store.createEntity(user, await bodyJson(req));
        return json(res, 201, snapshotAfter(user));
      }
      if (method === 'PATCH' && parts[2]) {
        store.patchEntity(user, parts[2], await bodyJson(req));
        return json(res, 200, snapshotAfter(user));
      }
      if (method === 'DELETE' && parts[2]) {
        const raw = await bodyJson(req);
        store.deleteEntity(user, parts[2], raw.version ?? url.searchParams.get('version'));
        return json(res, 200, snapshotAfter(user));
      }
    }
    if (
      ['dependencies', 'types', 'resources', 'rules'].includes(parts[1]) &&
      ['POST', 'PATCH', 'DELETE'].includes(method)
    ) {
      const raw = await bodyJson(req);
      const collection = parts[1] as 'dependencies' | 'types' | 'resources' | 'rules';
      const current = parts[2]
        ? store.read()[collection].find((v) => v.id === parts[2])
        : undefined;
      if (parts[2] && !current) throw new ApiError(404, 'Объект настройки не найден');
      const workspaceId = parse(id, current?.workspaceId ?? raw.workspaceId);
      const action =
        { dependencies: 'dependency', types: 'type', resources: 'resource', rules: 'rule' }[
          collection
        ] +
        '-' +
        (method === 'POST' ? 'create' : method === 'DELETE' ? 'delete' : 'update');
      store.change(user, workspaceId, action, text(raw.reason), (state) => {
        expectedRevision(raw, state);
        const list = state[collection] as any[];
        const record = current ? list.find((v) => v.id === current.id) : undefined;
        const before = record ? structuredClone(record) : null;
        if (current && !current.workspaceId)
          throw new ApiError(403, 'Встроенное определение сохраняется неизменным');
        if (method === 'DELETE') {
          if (
            collection === 'types' &&
            (state.entities.some((e) => e.typeId === record.id) ||
              state.templates.some((t) => t.items.some((i) => i.typeId === record.id)) ||
              state.rules.some((r) => r.typeId === record.id))
          )
            throw new ApiError(409, 'Тип используется объектами');
          if (
            collection === 'resources' &&
            (state.entities.some((e) => e.allocations.some((a) => a.resourceId === record.id)) ||
              state.templates.some((t) =>
                t.items.some((i) => i.allocations?.some((a) => a.resourceId === record.id)),
              ))
          )
            throw new ApiError(409, 'Ресурс используется объектами');
          list.splice(list.indexOf(record), 1);
          return { before };
        }
        const patch = raw.patch
          ? object(raw.patch)
          : Object.fromEntries(
              Object.entries(raw).filter(([k]) => !['revision', 'reason'].includes(k)),
            );
        if (method === 'PATCH' && ['id', 'workspaceId', 'builtin'].some((k) => k in patch))
          throw new ApiError(400, 'Защищённое поле');
        const value = {
          ...(record ?? {}),
          ...patch,
          id: record?.id ?? uid(collection.slice(0, -1)),
          workspaceId,
        };
        let next: Dependency | TypeDefinition | Resource | Rule;
        if (collection === 'dependencies') {
          next = parse(dependencySchema, value);
          const errors = validateDependency(next as Dependency, {
            ...state,
            dependencies: state.dependencies.filter((d) => d.id !== next.id),
          });
          if (errors.length) throw new ApiError(400, 'Недопустимая связь', 'validation', errors);
        } else if (collection === 'types') {
          next = parse(typeSchema, { ...value, builtin: false });
          if (
            new Set((next as TypeDefinition).fields.map((f) => f.id)).size !==
            (next as TypeDefinition).fields.length
          )
            throw new ApiError(400, 'Повтор имени поля');
        } else if (collection === 'resources') next = parse(resourceSchema, value);
        else {
          next = parse(ruleSchema, value);
          const rule = next as Rule;
          if (
            rule.typeId &&
            !state.types.some(
              (t) => t.id === rule.typeId && (!t.workspaceId || t.workspaceId === workspaceId),
            )
          )
            throw new ApiError(400, 'Тип правила недоступен');
          if (
            rule.ownerId &&
            !state.memberships.some(
              (m) => m.workspaceId === workspaceId && m.userId === rule.ownerId,
            )
          )
            throw new ApiError(400, 'Ответственный правила недоступен');
        }
        if (record) list[list.indexOf(record)] = next;
        else list.push(next);
        if (collection === 'types')
          for (const entity of state.entities.filter((e) => e.typeId === next.id))
            store.validateEntityReferences(entity, state);
        return { before, after: next };
      });
      return json(res, method === 'POST' ? 201 : 200, snapshotAfter(user));
    }
    if (parts[1] === 'workspaces') {
      if (method === 'POST' && !parts[2]) {
        store.createWorkspace(user, await bodyJson(req));
        return json(res, 201, snapshotAfter(user));
      }
      if (parts[2] && ['PATCH', 'DELETE'].includes(method)) {
        const raw = await bodyJson(req);
        store.change(
          user,
          parts[2],
          method === 'DELETE' ? 'workspace-delete' : 'workspace-update',
          text(raw.reason),
          (state) => {
            expectedRevision(raw, state);
            const workspace = state.workspaces.find((w) => w.id === parts[2]);
            if (!workspace) throw new ApiError(404, 'Пространство не найдено');
            const before = structuredClone(workspace);
            if (method === 'DELETE') {
              if (raw.confirm !== true) throw new ApiError(400, 'Для удаления нужно confirm:true');
              for (const field of [
                'entities',
                'dependencies',
                'resources',
                'rules',
                'signals',
                'notifications',
                'comments',
                'scenarios',
                'memberships',
              ] as const)
                (state[field] as any[]) = (state[field] as any[]).filter(
                  (v) => v.workspaceId !== workspace.id,
                );
              state.types = state.types.filter((t) => t.workspaceId !== workspace.id);
              state.templates = state.templates.filter((t) => t.workspaceId !== workspace.id);
              state.workspaces = state.workspaces.filter((w) => w.id !== workspace.id);
              state.users = state.users.filter(
                (u) => !u.pending || state.memberships.some((m) => m.userId === u.id),
              );
              store.db.prepare('DELETE FROM files WHERE workspace_id=?').run(workspace.id);
              store.db
                .prepare('DELETE FROM webhook_connections WHERE workspace_id=?')
                .run(workspace.id);
              store.db.prepare('DELETE FROM invitations WHERE workspace_id=?').run(workspace.id);
              return { before };
            }
            const patch = raw.patch ? object(raw.patch) : raw;
            if (patch.name !== undefined) {
              const name = text(patch.name).trim();
              if (!name || name.length > 200) throw new ApiError(400, 'Некорректное название');
              workspace.name = name;
            }
            if (patch.description !== undefined)
              workspace.description = text(patch.description).slice(0, 8000);
            if (patch.timezone !== undefined) workspace.timezone = parse(timezone, patch.timezone);
            if (patch.mode !== undefined) {
              if (!['personal', 'team'].includes(text(patch.mode)))
                throw new ApiError(400, 'Некорректный режим');
              workspace.mode = patch.mode as 'team' | 'personal';
            }
            return { before, after: workspace };
          },
          ['owner'],
        );
        return json(res, 200, snapshotAfter(user));
      }
    }
    if (parts[1] === 'memberships' && ['POST', 'PATCH', 'DELETE'].includes(method)) {
      const raw = await bodyJson(req);
      if (parts[2] && parts[3]) {
        raw.workspaceId = parts[2];
        raw.userId = parts[3];
      }
      if (!raw.workspaceId) raw.workspaceId = url.searchParams.get('workspaceId');
      if (!raw.userId && method === 'DELETE') raw.userId = url.searchParams.get('userId');
      const result = store.membership(
        user,
        raw,
        method === 'POST' ? 'create' : method === 'PATCH' ? 'update' : 'delete',
      );
      return json(res, 200, {
        ...snapshotAfter(user),
        ...('invitation' in result ? { invitation: result.invitation } : {}),
      });
    }
    if (parts[1] === 'invitations' && method === 'POST') {
      const raw = await bodyJson(req);
      if (parts[2] === 'accept') {
        store.acceptInvitation(user, raw);
        return json(res, 200, snapshotAfter(user));
      }
      const workspaceId = parse(id, raw.workspaceId),
        userId = parse(id, raw.userId);
      return json(res, 201, store.invitation(user, workspaceId, userId));
    }
    if (parts[1] === 'comments' && ['POST', 'DELETE'].includes(method)) {
      const raw = await bodyJson(req);
      const old = parts[2] ? store.read().comments.find((c) => c.id === parts[2]) : undefined;
      if (parts[2] && !old) throw new ApiError(404, 'Комментарий не найден');
      const entity = store.read().entities.find((e) => e.id === (old?.entityId ?? raw.entityId));
      if (!entity) throw new ApiError(404, 'Объект не найден');
      store.change(
        user,
        entity.workspaceId,
        method === 'POST' ? 'comment-create' : 'comment-delete',
        '',
        (state) => {
          if (method === 'DELETE') {
            const comment = state.comments.find((c) => c.id === old!.id)!;
            if (
              comment.userId !== user.id &&
              store.role(user.id, entity.workspaceId, state) !== 'owner'
            )
              throw new ApiError(403, 'Можно удалить свой комментарий');
            state.comments = state.comments.filter((c) => c.id !== comment.id);
            return { entityId: entity.id, before: comment };
          }
          const value = text(raw.text).trim();
          if (!value || value.length > 10000)
            throw new ApiError(400, 'Нужен текст до 10000 символов');
          const comment = {
            id: uid('comment'),
            workspaceId: entity.workspaceId,
            entityId: entity.id,
            userId: user.id,
            text: value,
            createdAt: new Date().toISOString(),
          };
          state.comments.push(comment);
          return { entityId: entity.id, after: comment };
        },
        ['owner', 'editor', 'approver'],
      );
      return json(res, 200, snapshotAfter(user));
    }
    if (parts[1] === 'templates' && !parts[2] && method === 'POST') {
      const raw = await bodyJson(req);
      const workspaceId = parse(id, raw.workspaceId);
      store.change(user, workspaceId, 'template-create', 'Сохранение шаблона', (state) => {
        const template = parse(templateSchema, {
          ...raw,
          id: uid('template'),
          description: raw.description ?? '',
          dependencies: raw.dependencies ?? [],
        });
        const ws = state.workspaces.find((w) => w.id === workspaceId)!;
        let instance: ReturnType<typeof applyTemplate>;
        try {
          instance = applyTemplate(template, {
            workspaceId,
            anchorDate: template.anchorDate ?? new Date().toISOString(),
            timezone: ws.timezone,
          });
        } catch (error) {
          throw new ApiError(400, (error as Error).message, 'invalid_template');
        }
        const validationState = {
          ...state,
          entities: [...state.entities, ...instance.entities],
          dependencies: [...state.dependencies, ...instance.dependencies],
        };
        for (const entity of instance.entities) {
          parse(entitySchema, entity);
          store.validateEntityReferences(entity, validationState);
        }
        for (const dependency of instance.dependencies) {
          const errors = validateDependency(dependency, {
            ...validationState,
            dependencies: validationState.dependencies.filter((d) => d.id !== dependency.id),
          });
          if (errors.length)
            throw new ApiError(400, 'Недопустимая связь шаблона', 'invalid_template', errors);
        }
        state.templates.push(template);
        return { after: { id: template.id, name: template.name } };
      });
      return json(res, 201, snapshotAfter(user));
    }
    if (parts[1] === 'templates' && parts[2] && parts[3] === 'apply' && method === 'POST') {
      const raw = await bodyJson(req);
      const workspaceId = parse(id, raw.workspaceId);
      store.change(user, workspaceId, 'template-apply', 'Применение шаблона', (state) => {
        const template = state.templates.find(
          (t) => t.id === parts[2] && (!t.workspaceId || t.workspaceId === workspaceId),
        );
        if (!template) throw new ApiError(404, 'Шаблон недоступен');
        const anchorDate = text(raw.start ?? raw.anchorDate);
        if (!Number.isFinite(Date.parse(anchorDate))) throw new ApiError(400, 'Нужна дата начала');
        const ws = state.workspaces.find((w) => w.id === workspaceId)!;
        const result = applyTemplate(template, {
          workspaceId,
          anchorDate,
          title: text(raw.title) || undefined,
          ownerId: user.id,
          timezone: ws.timezone,
        });
        state.entities.push(...result.entities);
        for (const entity of result.entities) {
          parse(entitySchema, entity);
          store.validateEntityReferences(entity, state);
        }
        for (const dependency of result.dependencies) {
          const errors = validateDependency(dependency, state);
          if (errors.length) throw new ApiError(400, 'Ошибки связей шаблона', 'validation', errors);
          state.dependencies.push(dependency);
        }
        return { after: { templateId: template.id, entityIds: result.entities.map((e) => e.id) } };
      });
      return json(res, 201, snapshotAfter(user));
    }
    if (parts[1] === 'assistant' && method === 'POST') {
      const raw = await bodyJson(req);
      const workspaceId = parse(id, raw.workspaceId);
      if (parts[2] === 'apply') {
        store.change(
          user,
          workspaceId,
          'assistant-apply',
          'Применено предложение локального помощника',
          (state) => {
            if (!Array.isArray(raw.drafts) || raw.drafts.length < 1 || raw.drafts.length > 100)
              throw new ApiError(400, 'Нужны drafts');
            const created = raw.drafts.map((d) =>
              store.makeEntity(
                { ...object(d), source: undefined, workspaceId, parentId: null },
                state,
                user,
              ),
            );
            state.entities.push(...created);
            return { after: { entityIds: created.map((e) => e.id) } };
          },
        );
        return json(res, 201, snapshotAfter(user));
      }
      store.requireRole(user, workspaceId);
      const prompt = text(raw.text).trim();
      if (!prompt || prompt.length > 20000)
        throw new ApiError(400, 'Нужен запрос до 20000 символов');
      return json(
        res,
        200,
        suggestFromText(
          prompt,
          store.snapshot(user),
          workspaceId,
          text(raw.start) || new Date().toISOString(),
        ),
      );
    }
    if (parts[1] === 'scenarios' && method === 'POST') {
      const raw = await bodyJson(req);
      if (parts[2] === 'preview') return json(res, 200, store.preview(user, raw));
      if (parts[2] && ['submit', 'approve', 'reject'].includes(parts[3])) {
        store.decideScenario(
          user,
          parts[2],
          parts[3] as 'submit' | 'approve' | 'reject',
          text(raw.reason),
        );
        return json(res, 200, snapshotAfter(user));
      }
      if (!parts[2]) {
        store.createScenario(user, raw);
        return json(res, 201, snapshotAfter(user));
      }
    }
    if (parts[1] === 'signals' && parts[2] && method === 'POST') {
      const action = parts[3],
        raw = await bodyJson(req);
      const signal = store.read().signals.find((s) => s.id === parts[2]);
      if (!signal) throw new ApiError(404, 'Сигнал не найден');
      if (!['ack', 'resolve', 'accept-risk', 'snooze'].includes(action))
        throw new ApiError(404, 'Неизвестное действие сигнала');
      store.change(
        user,
        signal.workspaceId,
        `signal-${action}`,
        text(raw.reason),
        (state) => {
          const current = state.signals.find((s) => s.id === signal.id)!;
          const before = structuredClone(current);
          const now = new Date().toISOString();
          if (action === 'snooze') {
            const until = text(raw.until ?? raw.snoozedUntil);
            if (
              !Number.isFinite(Date.parse(until)) ||
              Date.parse(until) <= Date.now() ||
              Date.parse(until) > Date.now() + 30 * 86400000
            )
              throw new ApiError(400, 'Нужно время отсрочки в пределах 30 дней');
            store.db
              .prepare(
                'INSERT OR REPLACE INTO signal_controls(signal_id,snoozed_until) VALUES(?,?)',
              )
              .run(current.id, new Date(until).toISOString());
            for (const n of state.notifications.filter(
              (n) => n.signalId === current.id && ['pending', 'failed'].includes(n.state),
            )) {
              n.state = 'snoozed';
              n.snoozedUntil = until;
            }
            return {
              entityId: current.entityId,
              before,
              after: { ...current, snoozedUntil: until },
            };
          }
          if (['resolved', 'accepted-risk'].includes(current.state) && action === 'ack')
            throw new ApiError(409, 'Сигнал уже обработан');
          if (action === 'resolve') {
            const active = evaluateSignals(state, now).some(
              (s) => s.dedupeKey === current.dedupeKey,
            );
            const pendingApproval =
              current.kind === 'approval' &&
              state.scenarios.some(
                (s) => `approval:${s.id}` === current.dedupeKey && s.state === 'pending',
              );
            if (active || pendingApproval)
              throw new ApiError(
                409,
                'Условие сигнала сохраняется. Измените объект или примите риск',
                'condition_active',
              );
            current.state = 'resolved';
            current.resolvedAt = now;
          }
          if (action === 'ack') {
            current.state = 'acknowledged';
            current.acknowledgedBy = user.id;
          }
          if (action === 'accept-risk') {
            if (!text(raw.reason).trim())
              throw new ApiError(400, 'Для принятия риска нужна причина');
            current.state = 'accepted-risk';
          }
          current.updatedAt = now;
          return { entityId: current.entityId, before, after: current };
        },
        action === 'accept-risk' ? ['owner', 'approver'] : ['owner', 'editor', 'approver'],
      );
      return json(res, 200, snapshotAfter(user));
    }
    if (parts[1] === 'notifications' && parts[2] && parts[3] === 'delivered' && method === 'POST') {
      const raw = await bodyJson(req);
      store.transaction((state) => {
        const n = state.notifications.find((n) => n.id === parts[2] && n.userId === user.id);
        if (!n) throw new ApiError(404, 'Уведомление не найдено');
        store.requireRole(user, n.workspaceId, undefined, state);
        if (n.channel !== 'browser') throw new ApiError(400, 'Этот канал подтверждает сервер');
        if (n.state === 'delivered') return false;
        if (Date.parse(n.scheduledAt) > Date.now()) throw new ApiError(409, 'Уведомление отложено');
        if (!['delivered', 'failed'].includes(text(raw.state, 'delivered')))
          throw new ApiError(400, 'Некорректный результат доставки');
        n.state = raw.state === 'failed' ? 'failed' : 'delivered';
        n.attempts++;
        if (n.state === 'delivered') n.deliveredAt = new Date().toISOString();
      });
      return json(res, 200, store.snapshot(user));
    }
    if (parts[1] === 'files') {
      if (method === 'POST' && !parts[2]) {
        const partsData = multipart(
          await bodyBuffer(req, FILE_LIMIT + 100000),
          req.headers['content-type'] ?? '',
        );
        const workspaceId = parse(
          id,
          partsData.find((p) => p.name === 'workspaceId')?.data.toString('utf8'),
        );
        const entityId =
          partsData.find((p) => p.name === 'entityId')?.data.toString('utf8') || null;
        const file = partsData.find((p) => p.name === 'file' && p.filename);
        if (!file || file.data.length > FILE_LIMIT) throw new ApiError(400, 'Нужен файл до 20 МБ');
        let fileId = '';
        store.change(user, workspaceId, 'file-upload', 'Загрузка файла', (state) => {
          if (
            entityId &&
            !state.entities.some((e) => e.id === entityId && e.workspaceId === workspaceId)
          )
            throw new ApiError(400, 'Объект файла недоступен');
          fileId = uid('file');
          const filename = file.filename!.replace(/[\r\n\x00]/g, '').slice(0, 300);
          const mime = /^[a-z0-9.+-]+\/[a-z0-9.+-]+$/i.test(file.mime)
            ? file.mime
            : 'application/octet-stream';
          store.db
            .prepare(
              'INSERT INTO files(id,workspace_id,entity_id,name,mime,size,data,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)',
            )
            .run(
              fileId,
              workspaceId,
              entityId,
              filename,
              mime,
              file.data.length,
              file.data,
              user.id,
              new Date().toISOString(),
            );
          return { entityId, after: { fileId, name: filename, size: file.data.length } };
        });
        return json(res, 201, {
          id: fileId,
          url: `/api/files/${fileId}`,
          label: file.filename,
          size: file.data.length,
          mime: file.mime,
        });
      }
      if (parts[2] && ['GET', 'HEAD', 'DELETE'].includes(method)) {
        const file = store.db.prepare('SELECT * FROM files WHERE id=?').get(parts[2]) as
          | {
              id: string;
              workspace_id: string;
              entity_id: string | null;
              name: string;
              mime: string;
              size: number;
              data: Uint8Array;
            }
          | undefined;
        if (!file) throw new ApiError(404, 'Файл не найден');
        store.requireRole(user, file.workspace_id);
        if (method === 'DELETE') {
          store.change(user, file.workspace_id, 'file-delete', 'Удаление файла', (state) => {
            store.db.prepare('DELETE FROM files WHERE id=?').run(file.id);
            for (const entity of state.entities.filter(
              (e) =>
                e.workspaceId === file.workspace_id && e.links.some((l) => l.fileId === file.id),
            )) {
              entity.links = entity.links.filter((l) => l.fileId !== file.id);
              entity.version++;
              entity.updatedAt = new Date().toISOString();
            }
            return {
              entityId: file.entity_id,
              before: { fileId: file.id, name: file.name, size: file.size },
            };
          });
          return json(res, 200, snapshotAfter(user));
        }
        res.writeHead(200, {
          'Content-Type': 'application/octet-stream',
          'Content-Length': file.size,
          'Content-Disposition': `attachment; filename="attachment"; filename*=UTF-8''${encodeURIComponent(file.name)}`,
          'Cache-Control': 'private, no-store',
        });
        res.end(method === 'HEAD' ? undefined : Buffer.from(file.data));
        return;
      }
    }
    if (parts[1] === 'connections') {
      if (method === 'GET') {
        const workspaceId = parse(id, url.searchParams.get('workspaceId'));
        store.requireRole(user, workspaceId, ['owner']);
        const row = store.db
          .prepare(
            'SELECT endpoint,enabled,status,updated_at,token_hash FROM webhook_connections WHERE workspace_id=?',
          )
          .get(workspaceId) as
          | {
              endpoint: string;
              enabled: number;
              status: string;
              updated_at: string;
              token_hash: string | null;
            }
          | undefined;
        return json(res, 200, {
          workspaceId,
          provider: 'generic-webhook',
          status: row?.status ?? 'not-configured',
          outbound: { enabled: !!row?.enabled, endpoint: row?.endpoint ?? '' },
          inbound: { configured: !!row?.token_hash, url: `/api/webhooks/${workspaceId}` },
          updatedAt: row?.updated_at ?? null,
        });
      }
    }
    throw new ApiError(404, 'API маршрут не найден', 'not_found');
  }
  const server = createHttpServer(async (req, res) => {
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'same-origin');
    res.setHeader('X-Frame-Options', 'DENY');
    try {
      if (!validHost(req)) throw new ApiError(403, 'Недопустимый Host', 'invalid_host');
      const url = new URL(req.url ?? '/', `http://${req.headers.host ?? '127.0.0.1'}`);
      if (embed) {
        if (
          embed.isEmbed(req) &&
          options.embed?.bridge?.verifyReady &&
          req.method === 'HEAD' &&
          url.pathname === '/api/embed/transport-ready' &&
          !url.search
        ) {
          let headers;
          try {
            headers = options.embed.bridge.verifyReady(req);
          } catch {
            throw new ApiError(403, 'Подключение не подтверждено', 'embed_transport_denied');
          }
          if (
            !store
              .read()
              .workspaces.some((workspace) => workspace.id === options.embed!.workspaceId)
          )
            throw new ApiError(
              503,
              'Выбранное пространство недоступно',
              'embed_workspace_unavailable',
            );
          res.writeHead(204, { ...headers, 'Cache-Control': 'no-store' });
          res.end();
          return;
        }
        if (embed.isEmbed(req) && options.embed?.bridge) {
          const buffer = await bodyBuffer(req, 1048576);
          try {
            await options.embed.bridge.verifyRequest(req, buffer);
          } catch (error) {
            const value = error as { status?: number; code?: string };
            throw new ApiError(
              value.status ?? 403,
              'Подключение или доступ изменился',
              value.code ?? 'embed_transport_denied',
            );
          }
          verifiedBridgeBodies.set(req, buffer);
        }
        if (
          await embed.handle(
            req,
            res,
            url,
            () => userFor(req)!,
            () => bodyBuffer(req),
          )
        )
          return;
        if (embed.isEmbed(req)) {
          // Existing stored file links keep their source-owned path. They use
          // this BFF session/fence, never native cookies or implicit owner.
          if (
            /^\/api\/files\/[^/]+$/.test(url.pathname) &&
            ['GET', 'HEAD'].includes(req.method ?? 'GET')
          ) {
            const session = await embed.authorize(req);
            await store.withWorkspaceScope(session.user.id, session.workspaceId, () =>
              embeddedUsers.run(session.user, () => api(req, res, url)),
            );
            return;
          }
          if (url.pathname.startsWith('/api/embed/')) {
            const legacy = new URL(url.href);
            legacy.pathname = legacy.pathname.replace('/api/embed/', '/api/');
            if (!embedApiAllowed(req.method ?? 'GET', legacy.pathname))
              throw new ApiError(
                403,
                'Это действие доступно в отдельном Планировщике',
                'embed_route_denied',
              );
            const session = await embed.authorize(req);
            await store.withWorkspaceScope(session.user.id, session.workspaceId, () =>
              embeddedUsers.run(session.user, () => api(req, res, legacy)),
            );
            return;
          }
          if (url.pathname.startsWith('/api/') || url.pathname === '/mcp') {
            const agent = [
              '/mcp',
              '/api/agent/tools',
              '/api/agent/call',
              '/api/agent/receipts/profile',
              '/api/agent/receipts/lookup',
            ].includes(url.pathname);
            if (!agent || !req.headers.authorization?.startsWith('Bearer '))
              throw new ApiError(
                403,
                'Локальный вход не действует внутри Сот',
                'embed_route_denied',
              );
          } else if (url.pathname === '/embed') {
            embed.framePolicy(res);
            try {
              await embed.authorize(req);
            } catch (error) {
              if (error instanceof ApiError && [401, 403].includes(error.status)) {
                embed.loginPage(req, res);
                return;
              }
              throw error;
            }
          } else if (url.pathname === '/') {
            res.writeHead(302, { Location: '/embed', 'Cache-Control': 'no-store' });
            res.end();
            return;
          } else if (!['GET', 'HEAD'].includes(req.method ?? 'GET')) {
            throw new ApiError(405, 'Метод недопустим');
          }
        } else if (url.pathname === '/embed' || url.pathname.startsWith('/api/embed/')) {
          throw new ApiError(
            403,
            'Встроенный вход использует отдельный адрес',
            'embed_origin_invalid',
          );
        }
      }
      if (url.pathname === '/health') {
        json(res, 200, {
          ok: true,
          storage: 'sqlite',
          version: '1.2.0',
          mcp: true,
          mode: localBinding ? 'local' : 'authenticated',
          serverTime: new Date().toISOString(),
        });
        return;
      }
      if (url.pathname === '/mcp') {
        agentOrigin(req);
        const identity = agentIdentity(req);
        const backend = new PlannerAgentBackend(store, identity.user, identity.scope);
        await handlePlannerMcp(
          req,
          res,
          req.method === 'POST' ? await bodyJson(req) : undefined,
          backend,
        );
        return;
      }
      if (url.pathname.startsWith('/api/agent/receipts/')) {
        agentOrigin(req);
        if (!req.headers.authorization?.startsWith('Bearer '))
          throw new ApiError(401, 'Для результата требуется Bearer ключ', 'unauthenticated');
        const identity = agentKeys.authenticate(req.headers.authorization.slice(7));
        const backend = new PlannerAgentBackend(store, identity.user, identity.scope);
        void backend;
        if (url.pathname === '/api/agent/receipts/profile' && req.method === 'GET')
          return json(res, 200, {
            ...workItemProofProfile,
            authority: {
              sourceActorId: identity.user.id,
              keyId: identity.scope.keyId,
              workspaceIds: identity.scope.workspaceIds,
              readOnly: !!identity.scope.readOnly,
              scopeDigest: proofHash(identity.scope),
            },
          });
        if (url.pathname === '/api/agent/receipts/lookup' && req.method === 'POST')
          return json(
            res,
            200,
            readWorkItemProof(store, identity.user, identity.scope, await bodyJson(req)),
          );
        throw new ApiError(405, 'Метод недопустим');
      }
      if (url.pathname.startsWith('/api/agent/keys')) {
        checkOrigin(req);
        const user = userFor(req)!;
        const parts = url.pathname.split('/');
        if (req.method === 'GET' && url.pathname === '/api/agent/keys')
          return json(res, 200, {
            keys: agentKeys.list(user),
            connection: {
              path: '/mcp',
              stdio: {
                command: process.execPath,
                args: [
                  '--import',
                  pathToFileURL(resolve(projectRoot, 'node_modules', 'tsx', 'dist', 'loader.mjs'))
                    .href,
                  resolve(projectRoot, 'server', 'mcp-stdio.ts'),
                ],
              },
            },
          });
        if (req.method === 'POST' && url.pathname === '/api/agent/keys')
          return json(res, 201, agentKeys.create(user, await bodyJson(req)));
        if (req.method === 'DELETE' && parts.length === 5 && parts[4])
          return json(res, 200, agentKeys.revoke(user, parse(id, parts[4])));
        throw new ApiError(405, 'Метод недопустим');
      }
      if (url.pathname === '/api/agent/tools' || url.pathname === '/api/agent/call') {
        agentOrigin(req);
        const identity = agentIdentity(req);
        const backend = new PlannerAgentBackend(store, identity.user, identity.scope);
        if (url.pathname === '/api/agent/tools' && req.method === 'GET')
          return json(res, 200, { tools: backend.tools });
        if (url.pathname === '/api/agent/call' && req.method === 'POST') {
          const raw = await bodyJson(req);
          return json(res, 200, backend.call(text(raw.name), raw.arguments ?? {}));
        }
        throw new ApiError(405, 'Метод недопустим');
      }
      if (
        url.pathname.startsWith('/api/connections') &&
        ['POST', 'PATCH', 'DELETE'].includes(req.method ?? '')
      ) {
        checkOrigin(req);
        const user = userFor(req)!;
        const raw = await bodyJson(req);
        const workspaceId = parse(id, raw.workspaceId);
        store.requireRole(user, workspaceId, ['owner']);
        if (req.method === 'DELETE') {
          store.change(
            user,
            workspaceId,
            'connection-delete',
            'Отключение webhook',
            () => {
              store.db
                .prepare('DELETE FROM webhook_connections WHERE workspace_id=?')
                .run(workspaceId);
              return { after: { provider: 'generic-webhook', status: 'not-configured' } };
            },
            ['owner'],
          );
          return json(res, 200, { ok: true, status: 'not-configured' });
        }
        const endpoint = text(raw.endpoint ?? raw.url);
        if (endpoint) await validateWebhookUrl(endpoint);
        let token: string | undefined;
        store.change(
          user,
          workspaceId,
          'connection-configure',
          'Настройка универсального webhook',
          () => {
            const existing = store.db
              .prepare('SELECT token_hash FROM webhook_connections WHERE workspace_id=?')
              .get(workspaceId) as { token_hash: string | null } | undefined;
            let tokenHash = existing?.token_hash ?? null;
            if (!tokenHash || raw.regenerateToken === true) {
              token = randomBytes(32).toString('base64url');
              tokenHash = createHash('sha256').update(token).digest('hex');
            }
            store.db
              .prepare(
                'INSERT OR REPLACE INTO webhook_connections(workspace_id,endpoint,token_hash,enabled,status,updated_at,created_by) VALUES(?,?,?,?,?,?,?)',
              )
              .run(
                workspaceId,
                endpoint,
                tokenHash,
                endpoint && raw.enabled === true ? 1 : 0,
                'configured',
                new Date().toISOString(),
                user.id,
              );
            return {
              after: {
                provider: 'generic-webhook',
                configured: true,
                outboundEnabled: !!endpoint && raw.enabled === true,
              },
            };
          },
          ['owner'],
        );
        return json(res, 200, {
          workspaceId,
          status: 'configured',
          inboundUrl: `/api/webhooks/${workspaceId}`,
          ...(token ? { token } : {}),
        });
      }
      if (url.pathname.startsWith('/api/')) {
        await api(req, res, url);
        return;
      }
      if (vite) {
        vite.middlewares(req, res, (e: unknown) => {
          if (e) json(res, 500, { error: 'Ошибка сервера разработки', code: 'development_error' });
        });
        return;
      }
      if (!['GET', 'HEAD'].includes(req.method ?? 'GET'))
        throw new ApiError(405, 'Метод недопустим');
      const dist = resolve(projectRoot, 'dist');
      let pathname: string;
      try {
        pathname = decodeURIComponent(url.pathname);
      } catch {
        throw new ApiError(400, 'Некорректный URL');
      }
      let file = resolve(dist, '.' + pathname);
      if (!file.startsWith(dist + sep) && file !== dist)
        throw new ApiError(403, 'Недопустимый путь');
      try {
        if (!(await stat(file)).isFile()) file = resolve(dist, 'index.html');
      } catch {
        file = resolve(dist, 'index.html');
      }
      let bytes: Buffer;
      try {
        bytes = await readFile(file);
      } catch {
        json(res, 503, {
          error: 'Сборка отсутствует. Выполните npm run build',
          code: 'build_missing',
        });
        return;
      }
      const mime: Record<string, string> = {
        '.html': 'text/html; charset=utf-8',
        '.js': 'text/javascript; charset=utf-8',
        '.css': 'text/css; charset=utf-8',
        '.svg': 'image/svg+xml',
        '.png': 'image/png',
        '.ico': 'image/x-icon',
        '.woff2': 'font/woff2',
        '.json': 'application/json',
      };
      res.writeHead(200, {
        'Content-Type': mime[extname(file)] ?? 'application/octet-stream',
        'Cache-Control': extname(file) === '.html' ? 'no-cache' : 'public, max-age=3600',
      });
      res.end(req.method === 'HEAD' ? undefined : bytes);
    } catch (error) {
      const e =
        error instanceof ApiError
          ? error
          : new ApiError(500, 'Не удалось выполнить действие', 'server_error');
      if (!res.headersSent)
        json(res, e.status, {
          error: e.message,
          code: e.code,
          ...(e.details ? { details: e.details } : {}),
        });
      else res.end();
    }
  });
  if (options.scheduler !== false) scheduler.start();
  return {
    server,
    store,
    scheduler,
    listen: () =>
      new Promise<number>((resolvePort, reject) => {
        server.once('error', reject);
        server.listen(port, host, () => {
          server.off('error', reject);
          const address = server.address();
          resolvePort(typeof address === 'object' && address ? address.port : port);
        });
      }),
    close: async () => {
      scheduler.stop();
      for (const res of activeEvents) res.end();
      await new Promise<void>((resolveClose) => server.close(() => resolveClose()));
      server.closeAllConnections();
      if (vite) await vite.close();
      store.close();
    },
  };
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const app = await createPlannerServer({
    dbPath: process.env.PLANNER_DB_PATH ? resolve(process.env.PLANNER_DB_PATH) : undefined,
    host: process.env.PLANNER_HOST ?? '127.0.0.1',
    port: Number(process.env.PLANNER_PORT ?? 4317),
    development: !process.argv.includes('--production'),
  });
  const port = await app.listen();
  console.log(
    `Timeline planner: http://${process.env.PLANNER_HOST ?? '127.0.0.1'}:${port} (${process.env.PLANNER_HOST && process.env.PLANNER_HOST !== '127.0.0.1' ? 'authentication required' : 'local mode'})`,
  );
  let closing = false;
  const close = async () => {
    if (closing) return;
    closing = true;
    await app.close();
    process.exit(0);
  };
  process.on('SIGINT', () => void close());
  process.on('SIGTERM', () => void close());
}
