import { DatabaseSync } from 'node:sqlite';
import { createHash, randomBytes, randomUUID, scryptSync, timingSafeEqual } from 'node:crypto';
import { dirname } from 'node:path';
import { mkdirSync } from 'node:fs';
import { EventEmitter } from 'node:events';
import { AsyncLocalStorage } from 'node:async_hooks';
import { createSeed } from '../shared/seed.ts';
import { validateEntity, validateDependency, deriveForecast } from '../shared/engine.ts';
import type {
  PlannerSnapshot,
  User,
  Role,
  Entity,
  Workspace,
  PlanChange,
  AuditEntry,
  PlannerSettings,
  Scenario,
  Source,
} from '../shared/types.ts';
import {
  ApiError,
  entitySchema,
  parse,
  object,
  text,
  blankRange,
  range,
  preferencesSchema,
  id as idSchema,
  role as roleSchema,
  timezone,
  iso,
} from './validation.ts';

export type ChangeRecord = { before?: unknown; after?: unknown; entityId?: string | null };
export interface Invitation {
  userId: string;
  workspaceId: string;
  email: string;
  token: string;
  expiresAt: string;
}
type InvitationRecord = {
  user_id: string;
  workspace_id: string;
  expires_at: string;
  created_by: string;
};
const clone = <T>(v: T): T => structuredClone(v);
const keyHash = (v: string) => createHash('sha256').update(v).digest('hex');
export const uid = (prefix: string) => `${prefix}-${randomUUID()}`;
const safeUser = (u: User): User => ({
  id: u.id,
  displayName: u.displayName,
  ...(u.email ? { email: u.email } : {}),
  ...(u.local ? { local: true } : {}),
  ...(u.pending ? { pending: true } : {}),
});

export class PlannerStore extends EventEmitter {
  private readonly workspaceFence = new AsyncLocalStorage<{
    userId: string;
    workspaceId: string;
  }>();
  withWorkspaceScope<T>(userId: string, workspaceId: string, callback: () => T): T {
    if (this.workspaceFence.getStore())
      throw new ApiError(403, 'Повторная область доступа запрещена', 'scope_violation');
    const user = this.user(userId);
    if (!user) throw new ApiError(401, 'Аккаунт недоступен', 'unauthenticated');
    this.requireRole(user, workspaceId);
    return this.workspaceFence.run(Object.freeze({ userId, workspaceId }), callback);
  }
  isWorkspaceScoped() {
    return this.workspaceFence.getStore() !== undefined;
  }
  readonly db: DatabaseSync;
  readonly localUserId: string;
  constructor(path: string, seed?: PlannerSnapshot) {
    super();
    if (path !== ':memory:') mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    this.db.exec(`PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), document TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS history (revision INTEGER PRIMARY KEY, created_at TEXT NOT NULL, document TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS history_time ON history(created_at);
      CREATE TABLE IF NOT EXISTS credentials (user_id TEXT PRIMARY KEY, email TEXT NOT NULL UNIQUE, salt TEXT NOT NULL, password_hash TEXT NOT NULL, created_at TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS sessions (token_hash TEXT PRIMARY KEY, user_id TEXT NOT NULL, expires_at TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS preferences (user_id TEXT PRIMARY KEY, document TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS files (id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, entity_id TEXT, name TEXT NOT NULL, mime TEXT NOT NULL, size INTEGER NOT NULL, data BLOB NOT NULL, created_by TEXT NOT NULL, created_at TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS rule_runs (key TEXT PRIMARY KEY, rule_id TEXT NOT NULL, entity_id TEXT NOT NULL, fired_at TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS notification_keys (key TEXT PRIMARY KEY, notification_id TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS signal_controls (signal_id TEXT PRIMARY KEY, snoozed_until TEXT);
      CREATE TABLE IF NOT EXISTS scenario_context (scenario_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS webhook_connections (workspace_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, token_hash TEXT, enabled INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, updated_at TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS login_limits (key TEXT PRIMARY KEY, attempts INTEGER NOT NULL, reset_at TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS invitations (user_id TEXT PRIMARY KEY, workspace_id TEXT NOT NULL, token_hash TEXT NOT NULL, expires_at TEXT NOT NULL, created_by TEXT NOT NULL);
    `);
    if (!this.db.prepare('SELECT id FROM state WHERE id=1').get()) {
      const initial = clone(seed ?? createSeed(new Date().toISOString()));
      initial.revision = Math.max(initial.revision, 1);
      this.db.prepare('INSERT INTO state(id,document) VALUES(1,?)').run(JSON.stringify(initial));
      this.db
        .prepare('INSERT INTO history(revision,created_at,document) VALUES(?,?,?)')
        .run(initial.revision, new Date().toISOString(), JSON.stringify(initial));
    }
    const invitationColumns = this.db.prepare('PRAGMA table_info(invitations)').all() as {
      name: string;
    }[];
    if (!invitationColumns.some((c) => c.name === 'workspace_id'))
      this.db.exec('ALTER TABLE invitations ADD COLUMN workspace_id TEXT');
    const current = this.read();
    const oldInvitations = this.db
      .prepare('SELECT user_id,workspace_id FROM invitations')
      .all() as { user_id: string; workspace_id: string | null }[];
    let upgraded = false;
    for (const invitation of oldInvitations) {
      const target = current.users.find((u) => u.id === invitation.user_id);
      const memberships = current.memberships.filter((m) => m.userId === invitation.user_id);
      if (
        !target ||
        memberships.length !== 1 ||
        this.db.prepare('SELECT user_id FROM credentials WHERE user_id=?').get(invitation.user_id)
      ) {
        this.db.prepare('DELETE FROM invitations WHERE user_id=?').run(invitation.user_id);
        continue;
      }
      if (!invitation.workspace_id)
        this.db
          .prepare('UPDATE invitations SET workspace_id=? WHERE user_id=?')
          .run(memberships[0].workspaceId, invitation.user_id);
      if (!target.pending) {
        target.pending = true;
        upgraded = true;
      }
    }
    if (upgraded)
      this.db.prepare('UPDATE state SET document=? WHERE id=1').run(JSON.stringify(current));
    this.localUserId = current.users.find((u) => u.local)?.id ?? current.user.id;
  }
  close() {
    this.db.close();
    this.removeAllListeners();
  }
  read(): PlannerSnapshot {
    const row = this.db.prepare('SELECT document FROM state WHERE id=1').get() as {
      document: string;
    };
    return JSON.parse(row.document);
  }
  user(id: string) {
    return this.read().users.find((u) => u.id === id);
  }
  localUser() {
    return this.user(this.localUserId)!;
  }
  role(userId: string, workspaceId: string, state = this.read()): Role | undefined {
    const fence = this.workspaceFence.getStore();
    if (fence && (fence.userId !== userId || fence.workspaceId !== workspaceId)) return undefined;
    return state.memberships.find((m) => m.userId === userId && m.workspaceId === workspaceId)
      ?.role;
  }
  requireRole(
    user: User,
    workspaceId: string,
    roles: Role[] = ['owner', 'editor', 'approver', 'viewer'],
    state = this.read(),
  ) {
    const ownRole = this.role(user.id, workspaceId, state);
    if (!ownRole || !roles.includes(ownRole))
      throw new ApiError(403, 'Недостаточно прав в этом пространстве', 'forbidden');
    return ownRole;
  }
  preferences(userId: string, state = this.read()): PlannerSettings {
    const row = this.db.prepare('SELECT document FROM preferences WHERE user_id=?').get(userId) as
      { document: string } | undefined;
    return row ? JSON.parse(row.document) : clone(state.settings);
  }
  snapshot(user: User, source = this.read(), current = this.read()): PlannerSnapshot {
    const fence = this.workspaceFence.getStore();
    if (fence && fence.userId !== user.id)
      throw new ApiError(403, 'Другой аккаунт недоступен', 'scope_violation');
    const allowed = new Set(
      current.memberships
        .filter((m) => m.userId === user.id && (!fence || m.workspaceId === fence.workspaceId))
        .map((m) => m.workspaceId),
    );
    const ws = source.workspaces.filter((w) => allowed.has(w.id));
    const wsIds = new Set(ws.map((w) => w.id));
    const memberships = source.memberships.filter((m) => wsIds.has(m.workspaceId));
    const userIds = new Set([user.id, ...memberships.map((m) => m.userId)]);
    const entities = source.entities.filter((e) => wsIds.has(e.workspaceId));
    const entityIds = new Set(entities.map((e) => e.id));
    const ownerWs = new Set(
      ws.filter((w) => this.role(user.id, w.id, current) === 'owner').map((w) => w.id),
    );
    return {
      ...clone(source),
      user: safeUser(user),
      users: source.users.filter((u) => userIds.has(u.id)).map(safeUser),
      memberships,
      workspaces: ws,
      entities,
      dependencies: source.dependencies.filter(
        (d) => wsIds.has(d.workspaceId) && entityIds.has(d.fromId) && entityIds.has(d.toId),
      ),
      types: source.types.filter((t) => !t.workspaceId || wsIds.has(t.workspaceId)),
      resources: source.resources.filter((r) => wsIds.has(r.workspaceId)),
      templates: source.templates.filter((t) => !t.workspaceId || wsIds.has(t.workspaceId)),
      rules: source.rules.filter((r) => wsIds.has(r.workspaceId)),
      signals: source.signals.filter((s) => wsIds.has(s.workspaceId)),
      notifications: source.notifications.filter(
        (n) => wsIds.has(n.workspaceId) && n.userId === user.id,
      ),
      comments: source.comments.filter((c) => wsIds.has(c.workspaceId)),
      scenarios: source.scenarios.filter((s) => wsIds.has(s.workspaceId)),
      audit: source.audit
        .filter((a) => wsIds.has(a.workspaceId))
        .map((a) =>
          ownerWs.has(a.workspaceId)
            ? a
            : {
                ...a,
                before: a.action === 'membership' ? null : a.before,
                after: a.action === 'membership' ? null : a.after,
              },
        ),
      settings: this.preferences(user.id, current),
      serverTime: new Date().toISOString(),
    };
  }
  transaction(fn: (state: PlannerSnapshot) => boolean | void, now = new Date().toISOString()) {
    this.db.exec('BEGIN IMMEDIATE');
    let changed = false;
    try {
      const state = this.read();
      changed = fn(state) !== false;
      if (changed) {
        state.revision += 1;
        state.serverTime = now;
        this.db.prepare('UPDATE state SET document=? WHERE id=1').run(JSON.stringify(state));
        this.db
          .prepare('INSERT INTO history(revision,created_at,document) VALUES(?,?,?)')
          .run(state.revision, now, JSON.stringify(state));
        this.db.prepare('DELETE FROM history WHERE revision < ?').run(state.revision - 2000);
      }
      this.db.exec('COMMIT');
    } catch (e) {
      this.db.exec('ROLLBACK');
      throw e;
    }
    if (changed) this.emit('change', this.read().revision);
  }
  audit(
    state: PlannerSnapshot,
    userId: string,
    workspaceId: string,
    action: string,
    reason: string,
    record: ChangeRecord = {},
    now = new Date().toISOString(),
  ) {
    const entry: AuditEntry = {
      id: uid('audit'),
      workspaceId,
      entityId: record.entityId ?? null,
      userId,
      action,
      reason: reason.slice(0, 8000),
      createdAt: now,
      before: clone(record.before ?? null),
      after: clone(record.after ?? null),
    };
    state.audit.push(entry);
  }
  change(
    user: User,
    workspaceId: string,
    action: string,
    reason: string,
    fn: (state: PlannerSnapshot) => ChangeRecord | void,
    roles: Role[] = ['owner', 'editor'],
  ) {
    this.transaction((state) => {
      this.requireRole(user, workspaceId, roles, state);
      const record = fn(state);
      this.audit(state, user.id, workspaceId, action, reason, record ?? {});
      this.invalidateScenarios(state);
      if (/^(dependency|resource|type)-/.test(action))
        for (const scenario of state.scenarios)
          if (scenario.workspaceId === workspaceId && ['draft', 'pending'].includes(scenario.state))
            scenario.state = 'stale';
    });
    return this.snapshot(user);
  }
  history(user: User, at: string) {
    if (!Number.isFinite(Date.parse(at))) throw new ApiError(400, 'Некорректная дата');
    const row = this.db
      .prepare(
        'SELECT document FROM history WHERE created_at <= ? ORDER BY created_at DESC,revision DESC LIMIT 1',
      )
      .get(new Date(at).toISOString()) as { document: string } | undefined;
    if (!row)
      throw new ApiError(404, 'Нет сохранённого состояния на эту дату', 'history_unavailable');
    return this.snapshot(user, JSON.parse(row.document));
  }
  issueSession(userId: string) {
    const token = randomBytes(32).toString('base64url');
    const expires = new Date(Date.now() + 7 * 86400000).toISOString();
    this.db
      .prepare('INSERT INTO sessions(token_hash,user_id,expires_at) VALUES(?,?,?)')
      .run(keyHash(token), userId, expires);
    this.db.prepare('DELETE FROM sessions WHERE expires_at < ?').run(new Date().toISOString());
    return token;
  }
  session(token: string): User | undefined {
    const row = this.db
      .prepare('SELECT user_id FROM sessions WHERE token_hash=? AND expires_at > ?')
      .get(keyHash(token), new Date().toISOString()) as { user_id: string } | undefined;
    return row ? this.user(row.user_id) : undefined;
  }
  logout(token: string) {
    this.db.prepare('DELETE FROM sessions WHERE token_hash=?').run(keyHash(token));
  }
  private validInvitation(token: string, email: string, state: PlannerSnapshot): InvitationRecord {
    const invitation =
      token.length <= 256
        ? (this.db
            .prepare(
              'SELECT user_id,workspace_id,expires_at,created_by FROM invitations WHERE token_hash=?',
            )
            .get(keyHash(token)) as InvitationRecord | undefined)
        : undefined;
    const target = invitation ? state.users.find((u) => u.id === invitation.user_id) : undefined;
    if (
      !invitation ||
      !target?.pending ||
      target.email?.toLowerCase() !== email ||
      Date.parse(invitation.expires_at) <= Date.now() ||
      !state.memberships.some(
        (m) => m.userId === target.id && m.workspaceId === invitation.workspace_id,
      ) ||
      state.memberships.some(
        (m) => m.userId === target.id && m.workspaceId !== invitation.workspace_id,
      ) ||
      this.role(invitation.created_by, invitation.workspace_id, state) !== 'owner'
    ) {
      throw new ApiError(
        403,
        'Нужен действующий код приглашения владельца для этого адреса',
        'invitation_required',
      );
    }
    return invitation;
  }
  private mintInvitation(user: User, target: User, workspaceId: string): Invitation {
    const token = randomBytes(32).toString('base64url');
    const expiresAt = new Date(Date.now() + 7 * 86400000).toISOString();
    this.db
      .prepare(
        'INSERT OR REPLACE INTO invitations(user_id,workspace_id,token_hash,expires_at,created_by) VALUES(?,?,?,?,?)',
      )
      .run(target.id, workspaceId, keyHash(token), expiresAt, user.id);
    return { userId: target.id, workspaceId, email: target.email!, token, expiresAt };
  }
  register(input: Record<string, unknown>, claimActor?: User) {
    const email = text(input.email).trim().toLowerCase();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || email.length > 256)
      throw new ApiError(400, 'Нужен корректный email');
    const password = text(input.password);
    if (password.length < 10 || password.length > 256)
      throw new ApiError(400, 'Пароль должен содержать от 10 до 256 символов');
    const displayName = text(input.displayName).trim().slice(0, 160);
    if (!displayName) throw new ApiError(400, 'Нужно имя');
    if (this.db.prepare('SELECT user_id FROM credentials WHERE email=?').get(email))
      throw new ApiError(409, 'Аккаунт уже существует', 'account_exists');
    const salt = randomBytes(16).toString('hex');
    const hash = scryptSync(password, salt, 64, {
      N: 16384,
      r: 8,
      p: 1,
      maxmem: 64 * 1024 * 1024,
    }).toString('hex');
    let created!: User;
    this.transaction((state) => {
      const pending = state.users.some((u) => u.pending && u.email?.toLowerCase() === email);
      const supplied = text(input.invitationToken);
      const invitation =
        pending || supplied ? this.validInvitation(supplied, email, state) : undefined;
      const invited = invitation ? state.users.find((u) => u.id === invitation.user_id) : undefined;
      created = invited ?? { id: uid('user'), displayName, email };
      created.displayName = displayName;
      delete created.pending;
      if (!invited) state.users.push(created);
      this.db
        .prepare(
          'INSERT INTO credentials(user_id,email,salt,password_hash,created_at) VALUES(?,?,?,?,?)',
        )
        .run(created.id, email, salt, hash, new Date().toISOString());
      this.db.prepare('DELETE FROM invitations WHERE user_id=?').run(created.id);
      if (
        input.claimLocal === true &&
        claimActor?.local &&
        !this.db.prepare('SELECT user_id FROM credentials WHERE user_id<>? LIMIT 1').get(created.id)
      ) {
        const localWs = state.memberships
          .filter((m) => m.userId === claimActor.id && m.role === 'owner')
          .map((m) => m.workspaceId);
        for (const workspaceId of localWs) {
          if (
            !state.memberships.some((m) => m.userId === created.id && m.workspaceId === workspaceId)
          )
            state.memberships.push({ workspaceId, userId: created.id, role: 'owner' });
          this.audit(
            state,
            created.id,
            workspaceId,
            'membership',
            'Локальное пространство подключено к аккаунту',
            { after: { userId: created.id, role: 'owner' } },
          );
        }
      }
      if (!state.memberships.some((m) => m.userId === created.id)) {
        const workspace: Workspace = {
          id: uid('workspace'),
          name: 'Моё пространство',
          mode: 'personal',
          timezone: 'Asia/Yekaterinburg',
          createdAt: new Date().toISOString(),
        };
        state.workspaces.push(workspace);
        state.memberships.push({ workspaceId: workspace.id, userId: created.id, role: 'owner' });
        this.audit(state, created.id, workspace.id, 'workspace-create', 'Регистрация', {
          after: workspace,
        });
      }
    });
    return { user: created, token: this.issueSession(created.id) };
  }
  acceptInvitation(user: User, raw: Record<string, unknown>) {
    const credential = this.db
      .prepare('SELECT email FROM credentials WHERE user_id=?')
      .get(user.id) as { email: string } | undefined;
    if (!credential || user.local)
      throw new ApiError(401, 'Войдите в зарегистрированный аккаунт', 'unauthenticated');
    this.transaction((state) => {
      const invitation = this.validInvitation(text(raw.invitationToken), credential.email, state);
      const targetId = invitation.user_id,
        workspaceId = invitation.workspace_id;
      const pendingMembership = state.memberships.find(
        (m) => m.userId === targetId && m.workspaceId === workspaceId,
      )!;
      const prior = state.memberships.find(
        (m) => m.userId === user.id && m.workspaceId === workspaceId,
      );
      if (prior) throw new ApiError(409, 'Аккаунт уже входит в это пространство', 'already_member');
      pendingMembership.userId = user.id;
      const now = new Date().toISOString();
      for (const entity of state.entities.filter(
        (e) =>
          e.workspaceId === workspaceId &&
          (e.ownerId === targetId || e.participantIds.includes(targetId)),
      )) {
        const before = clone(entity);
        if (entity.ownerId === targetId) entity.ownerId = user.id;
        entity.participantIds = [
          ...new Set(entity.participantIds.map((id) => (id === targetId ? user.id : id))),
        ];
        entity.version++;
        entity.updatedAt = now;
        this.audit(
          state,
          user.id,
          workspaceId,
          'entity-invitation-accepted',
          'Принято приглашение ответственного',
          { entityId: entity.id, before, after: entity },
          now,
        );
      }
      for (const rule of state.rules.filter(
        (r) => r.workspaceId === workspaceId && r.ownerId === targetId,
      ))
        rule.ownerId = user.id;
      for (const signal of state.signals.filter(
        (s) => s.workspaceId === workspaceId && s.responsibleUserId === targetId,
      ))
        signal.responsibleUserId = user.id;
      state.notifications = state.notifications.filter(
        (n) => !(n.workspaceId === workspaceId && n.userId === targetId),
      );
      state.users = state.users.filter((u) => u.id !== targetId);
      this.db.prepare('DELETE FROM invitations WHERE user_id=?').run(targetId);
      this.audit(
        state,
        user.id,
        workspaceId,
        'invitation-accepted',
        'Принято приглашение владельца',
        {
          before: { workspaceId, userId: targetId, role: pendingMembership.role },
          after: pendingMembership,
        },
        now,
      );
      this.invalidateScenarios(state);
    });
    return this.snapshot(user);
  }
  login(input: Record<string, unknown>, address: string) {
    const email = text(input.email).trim().toLowerCase().slice(0, 256);
    const key = keyHash(`${address}|${email}`);
    const limits = this.db
      .prepare('SELECT attempts,reset_at FROM login_limits WHERE key=?')
      .get(key) as { attempts: number; reset_at: string } | undefined;
    if (limits && limits.attempts >= 8 && Date.parse(limits.reset_at) > Date.now())
      throw new ApiError(429, 'Слишком много попыток. Повторите через 15 минут', 'rate_limited');
    const row = this.db
      .prepare('SELECT user_id,salt,password_hash FROM credentials WHERE email=?')
      .get(email) as { user_id: string; salt: string; password_hash: string } | undefined;
    const candidate = scryptSync(
      text(input.password).slice(0, 256),
      row?.salt ?? 'unknown-user-fixed-salt',
      64,
      { N: 16384, r: 8, p: 1, maxmem: 64 * 1024 * 1024 },
    );
    if (!row || !timingSafeEqual(candidate, Buffer.from(row.password_hash, 'hex'))) {
      const attempts = limits && Date.parse(limits.reset_at) > Date.now() ? limits.attempts + 1 : 1;
      this.db
        .prepare('INSERT OR REPLACE INTO login_limits(key,attempts,reset_at) VALUES(?,?,?)')
        .run(key, attempts, new Date(Date.now() + 15 * 60000).toISOString());
      throw new ApiError(401, 'Неверный email или пароль', 'invalid_credentials');
    }
    this.db.prepare('DELETE FROM login_limits WHERE key=?').run(key);
    const user = this.user(row.user_id)!;
    return { user, token: this.issueSession(user.id) };
  }
  makeEntity(
    raw: Record<string, unknown>,
    state: PlannerSnapshot,
    actor: User,
    imported = false,
  ): Entity {
    const workspaceId = parse(idSchema, raw.workspaceId);
    const ws = state.workspaces.find((w) => w.id === workspaceId);
    if (!ws) throw new ApiError(404, 'Пространство не найдено');
    const typeId = text(
      raw.typeId,
      state.types.find(
        (t) =>
          (!t.workspaceId || t.workspaceId === workspaceId) &&
          (!raw.kind || t.kind === raw.kind) &&
          !t.fields.some((f) => f.required),
      )?.id,
    );
    const definition = state.types.find(
      (t) => t.id === typeId && (!t.workspaceId || t.workspaceId === workspaceId),
    );
    if (!definition) throw new ApiError(400, 'Неизвестный тип объекта');
    const now = new Date().toISOString();
    const plan = raw.plan ?? blankRange(ws.timezone);
    const manualSource: Source = {
      kind: imported ? 'import' : 'manual',
      label: imported ? 'Импорт' : 'Создано вручную',
      observedAt: now,
      receivedAt: now,
    };
    const entity = parse(entitySchema, {
      id: uid('entity'),
      workspaceId,
      typeId,
      kind: raw.kind ?? definition.kind,
      title: raw.title,
      description: raw.description ?? '',
      parentId: raw.parentId ?? null,
      ownerId: raw.ownerId === undefined ? actor.id : raw.ownerId,
      participantIds: raw.participantIds ?? [],
      status: raw.status ?? 'planned',
      plan,
      baseline: imported && raw.baseline ? raw.baseline : plan,
      actual: raw.actual ?? null,
      forecast: raw.forecast ?? null,
      forecastProvenance: raw.forecast
        ? imported && raw.forecastProvenance === 'derived'
          ? 'derived'
          : 'manual'
        : undefined,
      dueAt: raw.dueAt ?? null,
      tags: raw.tags ?? [],
      fields: raw.fields ?? {},
      links: raw.links ?? [],
      allocations: raw.allocations ?? [],
      recurrence: raw.recurrence ?? null,
      source: raw.source ?? manualSource,
      createdAt: now,
      updatedAt: now,
      version: 1,
    }) as Entity;
    this.validateEntityReferences(entity, state);
    return entity;
  }
  validateEntityReferences(entity: Entity, state: PlannerSnapshot) {
    const errors = validateEntity(entity, state);
    const memberIds = new Set(
      state.memberships.filter((m) => m.workspaceId === entity.workspaceId).map((m) => m.userId),
    );
    if (entity.ownerId && !memberIds.has(entity.ownerId))
      errors.push('Ответственный не входит в пространство');
    if (entity.participantIds.some((id) => !memberIds.has(id)))
      errors.push('Участник не входит в пространство');
    if (
      entity.allocations.some(
        (a) =>
          !state.resources.some(
            (r) => r.id === a.resourceId && r.workspaceId === entity.workspaceId,
          ),
      )
    )
      errors.push('Ресурс из другого пространства или отсутствует');
    if (
      entity.parentId &&
      !state.entities.some((e) => e.id === entity.parentId && e.workspaceId === entity.workspaceId)
    )
      errors.push('Родитель из другого пространства или отсутствует');
    for (const link of entity.links)
      if (link.kind === 'file') {
        if (!link.fileId || link.url !== `/api/files/${link.fileId}`) {
          errors.push('Вложение должно ссылаться на загруженный файл');
          continue;
        }
        const file = this.db
          .prepare('SELECT workspace_id FROM files WHERE id=?')
          .get(link.fileId) as { workspace_id: string } | undefined;
        if (!file || file.workspace_id !== entity.workspaceId) errors.push('Недоступный файл');
      }
    let cursor = entity.parentId;
    const visited = new Set([entity.id]);
    while (cursor) {
      if (visited.has(cursor)) {
        errors.push('Цикл вложенности');
        break;
      }
      visited.add(cursor);
      cursor = state.entities.find((e) => e.id === cursor)?.parentId ?? null;
    }
    if (errors.length)
      throw new ApiError(400, 'Объект не прошёл проверку', 'invalid_entity', errors);
  }
  createEntity(user: User, raw: Record<string, unknown>) {
    const workspaceId = parse(idSchema, raw.workspaceId);
    return this.change(user, workspaceId, 'entity-create', text(raw.reason), (state) => {
      if (raw.source && object(raw.source).kind !== 'manual')
        throw new ApiError(
          400,
          'Ручной объект не может представляться подключённым источником',
          'protected_source',
        );
      const entity = this.makeEntity(raw, state, user);
      state.entities.push(entity);
      return { entityId: entity.id, after: entity };
    });
  }
  patchEntity(user: User, entityId: string, raw: Record<string, unknown>) {
    const current = this.read().entities.find((e) => e.id === entityId);
    if (!current) throw new ApiError(404, 'Объект не найдено');
    return this.change(user, current.workspaceId, 'entity-update', text(raw.reason), (state) => {
      const entity = state.entities.find((e) => e.id === entityId)!;
      if (Number(raw.version ?? raw.baseVersion) !== entity.version)
        throw new ApiError(409, 'Объект изменился. Обновите данные', 'version_conflict', {
          entityId,
          version: entity.version,
        });
      const patch = raw.patch
        ? object(raw.patch)
        : Object.fromEntries(
            Object.entries(raw).filter(([k]) => !['version', 'baseVersion', 'reason'].includes(k)),
          );
      if (
        [
          'id',
          'workspaceId',
          'baseline',
          'createdAt',
          'updatedAt',
          'version',
          'source',
          'forecastProvenance',
        ].some((k) => k in patch)
      )
        throw new ApiError(
          400,
          'Это поле меняется только специальным действием',
          'protected_field',
        );
      if (entity.status === 'done' && (patch.plan || patch.actual) && !text(raw.reason).trim())
        throw new ApiError(400, 'Для исправления завершённого объекта нужна причина');
      const before = clone(entity);
      const updated = parse(entitySchema, {
        ...entity,
        ...patch,
        ...('forecast' in patch
          ? { forecastProvenance: patch.forecast ? 'manual' : undefined }
          : {}),
        updatedAt: new Date().toISOString(),
        version: entity.version + 1,
      }) as Entity;
      this.validateEntityReferences(updated, state);
      state.entities[state.entities.findIndex((e) => e.id === entityId)] = updated;
      return { entityId, before, after: updated };
    });
  }
  deleteEntity(user: User, entityId: string, version: unknown) {
    const current = this.read().entities.find((e) => e.id === entityId);
    if (!current) throw new ApiError(404, 'Объект не найден');
    return this.change(user, current.workspaceId, 'entity-delete', 'Удаление объекта', (state) => {
      const entity = state.entities.find((e) => e.id === entityId)!;
      if (Number(version) !== entity.version)
        throw new ApiError(409, 'Объект изменился', 'version_conflict');
      state.entities = state.entities.filter((e) => e.id !== entityId);
      const attached = this.db.prepare('SELECT id FROM files WHERE entity_id=?').all(entityId) as {
        id: string;
      }[];
      for (const file of attached) {
        const remaining = state.entities.find((e) =>
          e.links.some((link) => link.kind === 'file' && link.fileId === file.id),
        );
        if (remaining)
          this.db.prepare('UPDATE files SET entity_id=? WHERE id=?').run(remaining.id, file.id);
        else this.db.prepare('DELETE FROM files WHERE id=?').run(file.id);
      }
      for (const child of state.entities.filter((e) => e.parentId === entityId)) {
        const before = clone(child);
        child.parentId = null;
        child.version++;
        child.updatedAt = new Date().toISOString();
        this.audit(
          state,
          user.id,
          child.workspaceId,
          'entity-reference-cleanup',
          'Удалён родитель',
          { entityId: child.id, before, after: child },
        );
      }
      state.dependencies = state.dependencies.filter(
        (d) => d.fromId !== entityId && d.toId !== entityId,
      );
      state.comments = state.comments.filter((c) => c.entityId !== entityId);
      const removedSignals = new Set(
        state.signals.filter((s) => s.entityId === entityId).map((s) => s.id),
      );
      state.signals = state.signals.filter((s) => s.entityId !== entityId);
      state.notifications = state.notifications.filter((n) => !removedSignals.has(n.signalId));
      return { entityId, before: entity };
    });
  }
  invalidateScenarios(state: PlannerSnapshot) {
    for (const scenario of state.scenarios)
      if (['draft', 'pending'].includes(scenario.state)) {
        const context = this.db
          .prepare('SELECT fingerprint FROM scenario_context WHERE scenario_id=?')
          .get(scenario.id) as { fingerprint: string } | undefined;
        if (
          Object.entries(scenario.baseVersions).some(
            ([id, version]) => state.entities.find((e) => e.id === id)?.version !== version,
          ) ||
          !context ||
          context.fingerprint !== this.scenarioContext(state, scenario.workspaceId)
        )
          scenario.state = 'stale';
      }
  }
  scenarioContext(state: PlannerSnapshot, workspaceId: string) {
    return keyHash(
      JSON.stringify({
        workspace: state.workspaces.find((w) => w.id === workspaceId),
        entities: state.entities
          .filter((e) => e.workspaceId === workspaceId)
          .map((e) => ({ id: e.id, version: e.version }))
          .sort((a, b) => a.id.localeCompare(b.id)),
        dependencies: state.dependencies
          .filter((d) => d.workspaceId === workspaceId)
          .sort((a, b) => a.id.localeCompare(b.id)),
        resources: state.resources
          .filter((r) => r.workspaceId === workspaceId)
          .sort((a, b) => a.id.localeCompare(b.id)),
        types: state.types
          .filter((t) => !t.workspaceId || t.workspaceId === workspaceId)
          .sort((a, b) => a.id.localeCompare(b.id)),
      }),
    );
  }
  preview(user: User, raw: Record<string, unknown>) {
    const workspaceId = parse(idSchema, raw.workspaceId);
    const state = this.read();
    this.requireRole(user, workspaceId, ['owner', 'editor', 'approver', 'viewer'], state);
    const changes = this.parseChanges(raw.changes, workspaceId, state);
    return deriveForecast(this.snapshot(user, state), changes, workspaceId);
  }
  parseChanges(value: unknown, workspaceId: string, state: PlannerSnapshot): PlanChange[] {
    if (!Array.isArray(value) || !value.length || value.length > 1000)
      throw new ApiError(400, 'Нужны изменения сроков');
    const changes = value.map((v) => {
      const item = object(v);
      const entityId = parse(idSchema, item.entityId);
      const entity = state.entities.find((e) => e.id === entityId && e.workspaceId === workspaceId);
      if (!entity) throw new ApiError(400, 'Объект изменения недоступен');
      if (entity.status === 'done' || entity.actual?.end)
        throw new ApiError(400, 'Завершённый факт не переносится сценарием');
      const plan = parse(range, item.plan);
      if (entity.actual?.start && plan.start !== entity.plan.start)
        throw new ApiError(400, 'Зафиксированное начало работы не переносится сценарием');
      return { entityId, plan } as PlanChange;
    });
    if (new Set(changes.map((c) => c.entityId)).size !== changes.length)
      throw new ApiError(400, 'Повтор объекта в изменениях');
    return changes;
  }
  createScenario(
    user: User,
    raw: Record<string, unknown>,
    hooks?: {
      before?: (state: PlannerSnapshot) => void;
      after?: (state: PlannerSnapshot, scenario: Scenario) => void;
    },
  ) {
    const workspaceId = parse(idSchema, raw.workspaceId);
    return this.change(user, workspaceId, 'scenario-create', text(raw.reason), (state) => {
      hooks?.before?.(state);
      const changes = this.parseChanges(raw.changes, workspaceId, state);
      const preview = deriveForecast(this.snapshot(user, state), changes, workspaceId);
      if (preview.conflicts.some((c) => ['cycle', 'invalid-time'].includes(c.kind)))
        throw new ApiError(
          400,
          'Сценарий содержит недопустимые изменения',
          'invalid_scenario',
          preview.conflicts,
        );
      const baseVersions = Object.fromEntries(
        state.entities.filter((e) => e.workspaceId === workspaceId).map((e) => [e.id, e.version]),
      );
      const scenario: Scenario = {
        id: uid('scenario'),
        workspaceId,
        name: text(raw.name, 'Вариант плана').slice(0, 300),
        reason: text(raw.reason).slice(0, 8000),
        requestedBy: user.id,
        createdAt: new Date().toISOString(),
        state: 'draft',
        baseVersions,
        changes: preview.changes,
        preview,
      };
      this.db
        .prepare('INSERT INTO scenario_context(scenario_id,fingerprint) VALUES(?,?)')
        .run(scenario.id, this.scenarioContext(state, workspaceId));
      state.scenarios.push(scenario);
      hooks?.after?.(state, scenario);
      return { after: scenario };
    });
  }
  decideScenario(
    user: User,
    scenarioId: string,
    action: 'submit' | 'approve' | 'reject',
    reason = '',
    hooks?: {
      before?: (state: PlannerSnapshot) => void;
      after?: (state: PlannerSnapshot, scenario: Scenario) => void;
    },
  ) {
    const existing = this.read().scenarios.find((s) => s.id === scenarioId);
    if (!existing) throw new ApiError(404, 'Сценарий не найден');
    const roles: Role[] = action === 'submit' ? ['owner', 'editor'] : ['owner', 'approver'];
    return this.change(
      user,
      existing.workspaceId,
      `scenario-${action}`,
      reason,
      (state) => {
        hooks?.before?.(state);
        const scenario = state.scenarios.find((s) => s.id === scenarioId)!;
        const context = this.db
          .prepare('SELECT fingerprint FROM scenario_context WHERE scenario_id=?')
          .get(scenario.id) as { fingerprint: string } | undefined;
        if (
          scenario.state === 'stale' ||
          !context ||
          context.fingerprint !== this.scenarioContext(state, scenario.workspaceId) ||
          Object.entries(scenario.baseVersions).some(
            ([id, v]) => state.entities.find((e) => e.id === id)?.version !== v,
          )
        )
          throw new ApiError(
            409,
            'Исходные данные сценария изменились. Создайте новый вариант',
            'stale_scenario',
          );
        const before = clone(scenario);
        if (action === 'submit') {
          if (scenario.state !== 'draft') throw new ApiError(409, 'Этот сценарий уже отправлен');
          scenario.state = 'pending';
        } else {
          if (scenario.state !== 'pending')
            throw new ApiError(409, 'Сценарий должен быть отправлен на согласование');
          if (action === 'approve') {
            const recalculated = deriveForecast(
              this.snapshot(user, state),
              scenario.changes,
              scenario.workspaceId,
            );
            if (
              JSON.stringify(recalculated.changes) !== JSON.stringify(scenario.preview.changes) ||
              JSON.stringify(recalculated.conflicts) !== JSON.stringify(scenario.preview.conflicts)
            )
              throw new ApiError(
                409,
                'Последствия сценария изменились. Создайте актуальный вариант',
                'stale_scenario',
              );
            for (const change of scenario.changes) {
              const entity = state.entities.find(
                (e) => e.id === change.entityId && e.workspaceId === scenario.workspaceId,
              );
              if (!entity || entity.status === 'done' || entity.actual?.end)
                throw new ApiError(409, 'Состояние объекта изменилось');
              const previous = clone(entity);
              entity.plan = parse(range, change.plan);
              entity.forecast = null;
              delete entity.forecastProvenance;
              entity.updatedAt = new Date().toISOString();
              entity.version++;
              this.validateEntityReferences(entity, state);
              this.audit(
                state,
                user.id,
                scenario.workspaceId,
                'entity-plan-approved',
                scenario.reason,
                { entityId: entity.id, before: previous, after: entity },
              );
            }
            scenario.state = 'approved';
          } else scenario.state = 'rejected';
          scenario.decidedBy = user.id;
          scenario.decidedAt = new Date().toISOString();
        }
        hooks?.after?.(state, scenario);
        return { before, after: scenario };
      },
      roles,
    );
  }
  updateSettings(user: User, raw: Record<string, unknown>) {
    this.transaction((state) => {
      const previous = this.preferences(user.id, state);
      const notificationInput = raw.notifications ? object(raw.notifications) : raw;
      const notifications = parse(preferencesSchema, {
        ...previous.notifications,
        ...notificationInput,
      });
      const value = {
        ...previous,
        notifications,
        lastSeenAt:
          raw.lastSeenAt === undefined
            ? previous.lastSeenAt
            : raw.lastSeenAt === null
              ? null
              : new Date(parse(iso, raw.lastSeenAt)).toISOString(),
      };
      this.db
        .prepare('INSERT OR REPLACE INTO preferences(user_id,document) VALUES(?,?)')
        .run(user.id, JSON.stringify(value));
    });
    return this.snapshot(user);
  }
  createWorkspace(user: User, raw: Record<string, unknown>) {
    this.transaction((state) => {
      const name = text(raw.name).trim();
      if (!name || name.length > 200) throw new ApiError(400, 'Нужно название пространства');
      const mode = raw.mode === 'team' ? 'team' : 'personal';
      const tz = parse(timezone, raw.timezone ?? 'Asia/Yekaterinburg');
      const workspace: Workspace = {
        id: uid('workspace'),
        name,
        mode,
        timezone: tz,
        createdAt: new Date().toISOString(),
        description: text(raw.description).slice(0, 8000),
      };
      state.workspaces.push(workspace);
      state.memberships.push({ workspaceId: workspace.id, userId: user.id, role: 'owner' });
      this.audit(state, user.id, workspace.id, 'workspace-create', '', { after: workspace });
    });
    return this.snapshot(user);
  }
  membership(user: User, raw: Record<string, unknown>, action: 'create' | 'update' | 'delete') {
    const workspaceId = parse(idSchema, raw.workspaceId);
    let invitation: Invitation | undefined;
    const snapshot = this.change(
      user,
      workspaceId,
      'membership',
      text(raw.reason),
      (state) => {
        let userId = text(raw.userId);
        const email = text(raw.email).trim().toLowerCase();
        if (action === 'create' && !userId) {
          if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || email.length > 256)
            throw new ApiError(400, 'Нужен email участника');
          if (
            state.memberships.some(
              (m) =>
                m.workspaceId === workspaceId &&
                state.users.find((u) => u.id === m.userId)?.email?.toLowerCase() === email,
            )
          )
            throw new ApiError(409, 'Участник или приглашение уже добавлены', 'already_member');
          const target: User = {
            id: uid('user'),
            email,
            displayName: text(raw.displayName, email.split('@')[0]).trim().slice(0, 160) || email,
            pending: true,
          };
          state.users.push(target);
          userId = target.id;
        }
        if (!state.users.some((u) => u.id === userId))
          throw new ApiError(400, 'Пользователь не найден');
        if (
          action === 'create' &&
          state.users.find((u) => u.id === userId)?.pending &&
          state.memberships.some((m) => m.userId === userId && m.workspaceId !== workspaceId)
        )
          throw new ApiError(
            409,
            'У ожидающего участника отдельное приглашение для каждого пространства',
            'pending_invitation',
          );
        const prior = state.memberships.find(
          (m) => m.workspaceId === workspaceId && m.userId === userId,
        );
        if (action === 'create' && prior) throw new ApiError(409, 'Участник уже добавлен');
        if (action !== 'create' && !prior) throw new ApiError(404, 'Участник не найден');
        const nextRole = action === 'delete' ? null : parse(roleSchema, raw.role ?? 'viewer');
        if (
          prior?.role === 'owner' &&
          !state.users.find((u) => u.id === userId)?.pending &&
          nextRole !== 'owner' &&
          state.memberships.filter(
            (m) =>
              m.workspaceId === workspaceId &&
              m.role === 'owner' &&
              !state.users.find((u) => u.id === m.userId)?.pending,
          ).length === 1
        )
          throw new ApiError(409, 'В пространстве должен остаться владелец');
        const before = prior ? clone(prior) : null;
        if (action === 'delete') {
          state.memberships = state.memberships.filter((m) => m !== prior);
          for (const entity of state.entities.filter(
            (e) =>
              e.workspaceId === workspaceId &&
              (e.ownerId === userId || e.participantIds.includes(userId)),
          )) {
            const before = clone(entity);
            if (entity.ownerId === userId) entity.ownerId = null;
            entity.participantIds = entity.participantIds.filter((id) => id !== userId);
            entity.version++;
            entity.updatedAt = new Date().toISOString();
            this.audit(
              state,
              user.id,
              workspaceId,
              'entity-membership-removed',
              'Участник удалён из пространства',
              { entityId: entity.id, before, after: entity },
            );
          }
          for (const rule of state.rules.filter(
            (r) => r.workspaceId === workspaceId && r.ownerId === userId,
          ))
            delete rule.ownerId;
          for (const signal of state.signals.filter(
            (s) => s.workspaceId === workspaceId && s.responsibleUserId === userId,
          ))
            signal.responsibleUserId = null;
          state.notifications = state.notifications.filter(
            (n) => !(n.workspaceId === workspaceId && n.userId === userId),
          );
          this.db
            .prepare('DELETE FROM invitations WHERE user_id=? AND workspace_id=?')
            .run(userId, workspaceId);
          if (
            state.users.find((u) => u.id === userId)?.pending &&
            !state.memberships.some((m) => m.userId === userId)
          )
            state.users = state.users.filter((u) => u.id !== userId);
        } else if (prior) prior.role = nextRole!;
        else state.memberships.push({ workspaceId, userId, role: nextRole! });
        if (action === 'create') {
          const target = state.users.find((u) => u.id === userId)!;
          if (target.pending && target.email)
            invitation = this.mintInvitation(user, target, workspaceId);
        }
        return {
          before,
          after: action === 'delete' ? null : { workspaceId, userId, role: nextRole },
        };
      },
      ['owner'],
    );
    return { ...snapshot, ...(invitation ? { invitation } : {}) };
  }
  invitation(user: User, workspaceId: string, userId: string) {
    let result!: Invitation;
    this.change(
      user,
      workspaceId,
      'invitation-create',
      'Создан одноразовый код приглашения',
      (state) => {
        const target = state.users.find((u) => u.id === userId);
        if (
          !target?.email ||
          !state.memberships.some((m) => m.workspaceId === workspaceId && m.userId === userId)
        )
          throw new ApiError(404, 'Приглашаемый участник не найден');
        if (
          !target.pending ||
          this.db.prepare('SELECT user_id FROM credentials WHERE user_id=?').get(userId)
        )
          throw new ApiError(409, 'Участник уже имеет аккаунт');
        if (state.memberships.some((m) => m.userId === userId && m.workspaceId !== workspaceId))
          throw new ApiError(
            409,
            'У ожидающего участника отдельное приглашение для каждого пространства',
            'pending_invitation',
          );
        result = this.mintInvitation(user, target, workspaceId);
        return { after: { userId, workspaceId, expiresAt: result.expiresAt } };
      },
      ['owner'],
    );
    return result;
  }
}
