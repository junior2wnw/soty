import { DateTime } from 'luxon';
import { lookup } from 'node:dns/promises';
import { request } from 'node:https';
import { isIP } from 'node:net';
import { createHash } from 'node:crypto';
import {
  evaluateSignals,
  deriveForecast,
  expandRecurrence,
  computeResourceConflicts,
} from '../shared/engine.ts';
import type {
  Entity,
  Notification,
  NotificationPreferences,
  PlannerSnapshot,
  Signal,
} from '../shared/types.ts';
import { PlannerStore, uid } from './store.ts';
import { ApiError } from './validation.ts';

export function quietUntil(now: string, prefs: NotificationPreferences): string | null {
  if (prefs.quietStart === prefs.quietEnd) return null;
  const local = DateTime.fromISO(now, { zone: prefs.timezone });
  if (!local.isValid) return null;
  const toMinutes = (v: string) => Number(v.slice(0, 2)) * 60 + Number(v.slice(3));
  const start = toMinutes(prefs.quietStart),
    end = toMinutes(prefs.quietEnd),
    minute = local.hour * 60 + local.minute;
  const quiet = start < end ? minute >= start && minute < end : minute >= start || minute < end;
  if (!quiet) return null;
  let until = local.set({
    hour: Math.floor(end / 60),
    minute: end % 60,
    second: 0,
    millisecond: 0,
  });
  if (until.toMillis() <= local.toMillis()) until = until.plus({ days: 1 });
  return until.toUTC().toISO();
}
export function publicAddress(address: string): boolean {
  const family = isIP(address);
  if (!family) return false;
  if (family === 6) {
    // Only global unicast. IPv4-mapped, NAT64 and transition addresses can hide a local destination.
    const a = address.toLowerCase(),
      groups = a.split(':');
    const first = parseInt(groups[0], 16),
      second = parseInt(groups[1], 16);
    return (
      first >= 0x2000 &&
      first < 0x4000 &&
      first !== 0x2002 &&
      !(first === 0x2001 && [0, 2, 0x10, 0x20, 0xdb8].includes(second))
    );
  }
  const octets = address.split('.').map(Number);
  if (octets.length !== 4 || octets.some((v) => v < 0 || v > 255 || !Number.isInteger(v)))
    return false;
  const [a, b] = octets;
  return !(
    a === 0 ||
    a === 10 ||
    a === 127 ||
    a >= 224 ||
    (a === 169 && b === 254) ||
    (a === 172 && b >= 16 && b <= 31) ||
    (a === 192 && b === 168) ||
    (a === 100 && b >= 64 && b <= 127) ||
    (a === 198 && (b === 18 || b === 19)) ||
    (a === 192 && b === 0) ||
    (a === 198 && b === 51) ||
    (a === 203 && b === 0)
  );
}
export async function validateWebhookUrl(endpoint: string) {
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    throw new ApiError(400, 'Некорректный URL webhook');
  }
  if (
    url.protocol !== 'https:' ||
    url.username ||
    url.password ||
    (url.port && url.port !== '443') ||
    url.hash
  )
    throw new ApiError(400, 'Webhook должен использовать HTTPS без логина в URL');
  if (
    url.hostname === 'localhost' ||
    url.hostname.endsWith('.localhost') ||
    url.hostname.endsWith('.local')
  )
    throw new ApiError(400, 'Локальный адрес webhook недопустим');
  const addresses = await lookup(url.hostname.replace(/^\[|\]$/g, ''), { all: true }).catch(() => {
    throw new ApiError(400, 'Адрес webhook не удалось разрешить');
  });
  if (!addresses.length || addresses.some((a) => !publicAddress(a.address)))
    throw new ApiError(400, 'Webhook должен иметь публичный адрес');
  return { url, address: addresses[0] };
}
async function sendWebhook(endpoint: string, payload: unknown) {
  const { url, address } = await validateWebhookUrl(endpoint);
  const body = Buffer.from(JSON.stringify(payload));
  await new Promise<void>((resolve, reject) => {
    const req = request(
      url,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': body.length,
          'User-Agent': 'UniversalPlanner/1.0',
        },
        lookup: ((_host: any, _opts: any, cb: any) =>
          cb(null, address.address, address.family)) as any,
        timeout: 10000,
      },
      (res) => {
        res.resume();
        if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) resolve();
        else reject(new Error(`HTTP ${res.statusCode ?? 0}`));
      },
    );
    req.on('timeout', () => req.destroy(new Error('Webhook timeout')));
    req.on('error', () => reject(new Error('Webhook connection failed')));
    req.end(body);
  });
}
function digest(signal: Signal) {
  return createHash('sha256')
    .update(
      JSON.stringify([
        signal.title,
        signal.description,
        signal.severity,
        signal.dueAt,
        signal.state,
        signal.createdAt,
      ]),
    )
    .digest('hex')
    .slice(0, 20);
}

export class PlannerScheduler {
  private timer: ReturnType<typeof setInterval> | undefined;
  private delivering = false;
  constructor(readonly store: PlannerStore) {}
  start(interval = 15000) {
    this.tick();
    this.timer = setInterval(() => {
      try {
        this.tick();
        void this.deliverWebhooks();
      } catch {
        /* persisted state survives a failed tick; next tick retries */
      }
    }, interval);
    this.timer.unref();
  }
  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }
  tick(now = new Date().toISOString()) {
    this.store.transaction((state) => {
      let changed = false;
      // Derived forecast is kept separate from approved plan, immutable baseline and recorded facts.
      for (const workspace of state.workspaces) {
        const preview = deriveForecast(state, [], workspace.id);
        const projected = new Map(preview.changes.map((c) => [c.entityId, c.plan]));
        for (const entity of state.entities.filter((e) => e.workspaceId === workspace.id)) {
          // A manual forecast remains the owner's estimate, even if dependencies disagree.
          if (entity.forecast && entity.forecastProvenance !== 'derived') continue;
          const forecast = projected.get(entity.id);
          if (
            entity.status === 'done' ||
            entity.status === 'cancelled' ||
            !forecast ||
            JSON.stringify(forecast) === JSON.stringify(entity.plan)
          ) {
            if (entity.forecastProvenance === 'derived') {
              entity.forecast = null;
              delete entity.forecastProvenance;
              changed = true;
            }
          } else if (
            JSON.stringify(entity.forecast) !== JSON.stringify(forecast) ||
            entity.forecastProvenance !== 'derived'
          ) {
            entity.forecast = structuredClone(forecast);
            entity.forecastProvenance = 'derived';
            changed = true;
          }
        }
      }
      if (this.applyFollowups(state, now)) changed = true;
      this.store.invalidateScenarios(state);
      const evaluated = [
        ...evaluateSignals(
          { ...state, rules: state.rules.filter((r) => r.action === 'signal') },
          now,
        ),
        ...this.afterDoneSignals(state, now),
      ];
      const activeKeys = new Set(evaluated.map((s) => s.dedupeKey));
      for (const next of evaluated) {
        const index = state.signals.findIndex((s) => s.dedupeKey === next.dedupeKey);
        if (index < 0) {
          state.signals.push(next);
          changed = true;
        } else {
          const before = state.signals[index];
          const updated = { ...next, id: before.id, createdAt: before.createdAt };
          if (before.state === 'acknowledged' || before.state === 'accepted-risk') {
            updated.state = before.state;
            updated.acknowledgedBy = before.acknowledgedBy;
            updated.updatedAt = before.updatedAt;
          } else if (before.state === 'resolved') {
            updated.state = 'open';
            updated.createdAt = now;
            updated.updatedAt = now;
            delete updated.resolvedAt;
          } else if (
            JSON.stringify({ ...before, updatedAt: null }) ===
            JSON.stringify({ ...updated, updatedAt: null })
          ) {
            updated.updatedAt = before.updatedAt;
          }
          if (JSON.stringify(before) !== JSON.stringify(updated)) {
            state.signals[index] = updated;
            changed = true;
          }
        }
      }
      for (const signal of state.signals) {
        if (
          !['resolved', 'accepted-risk'].includes(signal.state) &&
          signal.kind !== 'approval' &&
          !activeKeys.has(signal.dedupeKey)
        ) {
          signal.state = 'resolved';
          signal.resolvedAt = now;
          signal.updatedAt = now;
          changed = true;
        }
      }
      for (const scenario of state.scenarios.filter((s) => s.state === 'pending')) {
        const dedupeKey = `approval:${scenario.id}`;
        if (!state.signals.some((s) => s.dedupeKey === dedupeKey)) {
          const entityId = scenario.changes[0]?.entityId;
          if (entityId) {
            state.signals.push({
              id: uid('signal'),
              workspaceId: scenario.workspaceId,
              entityId,
              kind: 'approval',
              severity: 'info',
              title: `Согласование: ${scenario.name}`,
              description: scenario.reason || 'Проверьте предлагаемые изменения плана',
              affectedIds: scenario.preview.affectedIds,
              responsibleUserId:
                state.memberships.find(
                  (m) => m.workspaceId === scenario.workspaceId && m.role === 'approver',
                )?.userId ??
                state.memberships.find(
                  (m) => m.workspaceId === scenario.workspaceId && m.role === 'owner',
                )?.userId ??
                null,
              dueAt: null,
              state: 'open',
              createdAt: now,
              updatedAt: now,
              dedupeKey,
            });
            changed = true;
          }
        }
      }
      for (const signal of state.signals.filter(
        (s) => s.kind === 'approval' && s.state !== 'resolved',
      )) {
        const scenario = state.scenarios.find((s) => `approval:${s.id}` === signal.dedupeKey);
        if (!scenario || scenario.state !== 'pending') {
          signal.state = 'resolved';
          signal.resolvedAt = now;
          signal.updatedAt = now;
          changed = true;
        }
      }
      const stoppedSignals = new Set(
        state.signals
          .filter((s) => ['resolved', 'accepted-risk'].includes(s.state))
          .map((s) => s.id),
      );
      const retained = state.notifications.filter(
        (n) => n.state === 'delivered' || !stoppedSignals.has(n.signalId),
      );
      if (retained.length !== state.notifications.length) {
        state.notifications = retained;
        changed = true;
      }
      if (this.scheduleNotifications(state, now)) changed = true;
      for (const n of state.notifications) {
        if (
          n.channel === 'in-app' &&
          n.state === 'pending' &&
          Date.parse(n.scheduledAt) <= Date.parse(now)
        ) {
          n.state = 'delivered';
          n.deliveredAt = now;
          n.attempts++;
          changed = true;
        }
        if (
          n.state === 'snoozed' &&
          n.snoozedUntil &&
          Date.parse(n.snoozedUntil) <= Date.parse(now)
        ) {
          n.state = 'pending';
          n.scheduledAt = now;
          changed = true;
        }
      }
      return changed;
    }, now);
  }
  private afterDoneSignals(state: PlannerSnapshot, now: string): Signal[] {
    const signals: Signal[] = [];
    for (const rule of state.rules.filter(
      (r) => r.enabled && r.action === 'signal' && r.trigger === 'after-done',
    ))
      for (const entity of state.entities.filter(
        (e) =>
          e.workspaceId === rule.workspaceId &&
          e.status === 'done' &&
          (!rule.typeId || e.typeId === rule.typeId),
      )) {
        const doneAt = entity.actual?.end ?? entity.actual?.start ?? entity.updatedAt;
        if (Date.parse(doneAt) + rule.leadMinutes * 60000 > Date.parse(now)) continue;
        const dedupeKey = `after-done|${rule.workspaceId}|${entity.id}|${rule.id}|${doneAt}`;
        const previous = state.signals.find((s) => s.dedupeKey === dedupeKey);
        if (previous?.state === 'resolved') continue;
        signals.push({
          id: previous?.id ?? uid('signal'),
          workspaceId: rule.workspaceId,
          entityId: entity.id,
          kind: 'reminder',
          severity: 'info',
          title: rule.name,
          description: `«${entity.title}» завершено. Требуется следующий шаг по правилу «${rule.name}».`,
          affectedIds: [entity.id],
          responsibleUserId: rule.ownerId ?? entity.ownerId,
          dueAt: null,
          state: previous?.state ?? 'open',
          createdAt: previous?.createdAt ?? now,
          updatedAt: previous?.updatedAt ?? now,
          dedupeKey,
        });
      }
    return signals;
  }
  private applyFollowups(state: PlannerSnapshot, now: string) {
    let changed = false;
    let created = 0;
    const nowMs = Date.parse(now);
    const originals = [...state.entities];
    const resourceConflicts = computeResourceConflicts(state);
    for (const rule of state.rules.filter((r) => r.enabled && r.action === 'create-followup')) {
      const actor = state.users.find(
        (u) =>
          u.id ===
          (rule.ownerId ??
            state.memberships.find((m) => m.workspaceId === rule.workspaceId && m.role === 'owner')
              ?.userId),
      );
      if (!actor || !this.store.role(actor.id, rule.workspaceId, state)) continue;
      for (const entity of originals.filter(
        (e) =>
          e.workspaceId === rule.workspaceId &&
          (!rule.typeId || e.typeId === rule.typeId) &&
          e.status !== 'cancelled',
      )) {
        let targets: string[] = [];
        if (rule.trigger === 'after-done' && entity.status === 'done') {
          const doneAt = entity.actual?.end ?? entity.actual?.start ?? entity.updatedAt;
          if (Date.parse(doneAt) + rule.leadMinutes * 60000 <= nowMs) targets = [doneAt];
        }
        if (
          rule.trigger === 'overdue' &&
          entity.status !== 'done' &&
          entity.dueAt &&
          Date.parse(entity.dueAt) < nowMs
        )
          targets = [entity.dueAt];
        if (
          rule.trigger === 'before-due' &&
          entity.status !== 'done' &&
          entity.dueAt &&
          Date.parse(entity.dueAt) - rule.leadMinutes * 60000 <= nowMs &&
          nowMs <= Date.parse(entity.dueAt)
        )
          targets = [entity.dueAt];
        if (rule.trigger === 'before-start' && entity.status !== 'done') {
          const candidates = entity.recurrence
            ? expandRecurrence(
                entity,
                new Date(nowMs - 60000).toISOString(),
                new Date(nowMs + Math.max(rule.leadMinutes, 1) * 60000).toISOString(),
              )
                .map((o) => o.start)
                .filter((at): at is string => at !== null)
            : entity.plan.start
              ? [entity.plan.start]
              : [];
          targets = candidates.filter(
            (at) => Date.parse(at) - rule.leadMinutes * 60000 <= nowMs && Date.parse(at) >= nowMs,
          );
        }
        if (
          rule.trigger === 'resource-conflict' &&
          resourceConflicts.some((c) => c.entityIds.includes(entity.id))
        )
          targets = [entity.plan.start ?? entity.plan.precise?.start ?? 'undated'];
        for (const target of targets) {
          const key = `${rule.id}|${entity.id}|${rule.trigger}|${target}`;
          const inserted = this.store.db
            .prepare(
              'INSERT OR IGNORE INTO rule_runs(key,rule_id,entity_id,fired_at) VALUES(?,?,?,?)',
            )
            .run(key, rule.id, entity.id, now);
          if (!Number(inserted.changes)) continue;
          if (created++ >= 100) throw new ApiError(503, 'Лимит выполнения правил за цикл');
          const draft = {
            workspaceId: entity.workspaceId,
            typeId: entity.typeId,
            kind: 'point',
            title: rule.followupTitle || `Следующий шаг: ${entity.title}`,
            description: `Создано правилом «${rule.name}» после события «${entity.title}».`,
            parentId: entity.id,
            ownerId: actor.id,
            plan: { start: now, end: null, timezone: entity.plan.timezone, precision: 'exact' },
            fields: {},
            status: 'draft',
          };
          // Follow-up uses a generic point type so custom required business fields do not block automation.
          draft.typeId =
            state.types.find(
              (t) =>
                t.kind === 'point' &&
                !t.fields.some((f) => f.required) &&
                (!t.workspaceId || t.workspaceId === entity.workspaceId),
            )?.id ?? draft.typeId;
          const next = this.store.makeEntity(draft, state, actor);
          state.entities.push(next);
          this.store.audit(
            state,
            actor.id,
            entity.workspaceId,
            'rule-followup',
            rule.name,
            { entityId: next.id, after: next },
            now,
          );
          changed = true;
        }
      }
    }
    return changed;
  }
  private scheduleNotifications(state: PlannerSnapshot, now: string) {
    let changed = false;
    const nowMs = Date.parse(now);
    for (const signal of state.signals.filter((s) => ['open', 'acknowledged'].includes(s.state))) {
      // Undated notes/drafts are valid content. Their contextual hint is not an actionable paging condition.
      if (signal.kind === 'missing-date' && signal.severity === 'info') continue;
      const control = this.store.db
        .prepare('SELECT snoozed_until FROM signal_controls WHERE signal_id=?')
        .get(signal.id) as { snoozed_until: string | null } | undefined;
      if (control?.snoozed_until && Date.parse(control.snoozed_until) > nowMs) continue;
      const members = state.memberships.filter(
        (m) =>
          m.workspaceId === signal.workspaceId &&
          !state.users.find((u) => u.id === m.userId)?.pending,
      );
      const owner = members.find((m) => m.role === 'owner')?.userId;
      const responsible = members.some((m) => m.userId === signal.responsibleUserId)
        ? signal.responsibleUserId
        : owner;
      if (!responsible) continue;
      const prefs = this.store.preferences(responsible, state).notifications;
      const unresolvedSince =
        signal.state === 'acknowledged'
          ? Date.parse(signal.updatedAt)
          : Date.parse(signal.createdAt);
      const escalated = nowMs - unresolvedSince >= prefs.escalationMinutes * 60000;
      if (signal.state === 'acknowledged' && !escalated) continue;
      const recipientIds = new Set([
        responsible,
        ...(escalated
          ? members.filter((m) => m.role === 'owner' || m.role === 'approver').map((m) => m.userId)
          : []),
      ]);
      for (const userId of recipientIds) {
        const ownPrefs = this.store.preferences(userId, state).notifications;
        const channels: Notification['channel'][] = ['in-app'];
        if (ownPrefs.browserEnabled) channels.push('browser');
        const connection = this.store.db
          .prepare('SELECT enabled FROM webhook_connections WHERE workspace_id=?')
          .get(signal.workspaceId) as { enabled: number } | undefined;
        if (connection?.enabled && userId === responsible) channels.push('webhook');
        for (const channel of channels) {
          const signature = `${signal.id}:${userId}:${channel}:${digest(signal)}:${escalated ? 'escalated' : 'direct'}`;
          const pending = state.notifications.find(
            (n) =>
              n.signalId === signal.id &&
              n.userId === userId &&
              n.channel === channel &&
              (['pending', 'snoozed'].includes(n.state) ||
                (n.state === 'failed' && n.attempts < 5)),
          );
          if (pending) continue;
          const last = state.notifications
            .filter((n) => n.signalId === signal.id && n.userId === userId && n.channel === channel)
            .sort(
              (a, b) =>
                Date.parse(b.deliveredAt ?? b.scheduledAt) -
                Date.parse(a.deliveredAt ?? a.scheduledAt),
            )[0];
          if (
            last &&
            Date.parse(last.deliveredAt ?? last.scheduledAt) + ownPrefs.repeatMinutes * 60000 >
              nowMs
          ) {
            const known = this.store.db
              .prepare('SELECT key FROM notification_keys WHERE key LIKE ? LIMIT 1')
              .get(signature + '%');
            if (known) continue;
          }
          const cycle = Math.floor(nowMs / (ownPrefs.repeatMinutes * 60000));
          const key = `${signature}:${cycle}`;
          const notification: Notification = {
            id: uid('notification'),
            workspaceId: signal.workspaceId,
            signalId: signal.id,
            userId,
            channel,
            state: 'pending',
            scheduledAt: channel === 'in-app' ? now : (quietUntil(now, ownPrefs) ?? now),
            attempts: 0,
            summary: `${escalated ? 'Требуется решение: ' : ''}${signal.title}. ${signal.description}`,
          };
          const insert = this.store.db
            .prepare('INSERT OR IGNORE INTO notification_keys(key,notification_id) VALUES(?,?)')
            .run(key, notification.id);
          if (!Number(insert.changes)) continue;
          state.notifications.push(notification);
          changed = true;
        }
      }
    }
    return changed;
  }
  async deliverWebhooks(now = new Date().toISOString()) {
    if (this.delivering) return;
    this.delivering = true;
    try {
      const candidates = this.store
        .read()
        .notifications.filter(
          (n) =>
            n.channel === 'webhook' &&
            ['pending', 'failed'].includes(n.state) &&
            n.attempts < 5 &&
            Date.parse(n.scheduledAt) <= Date.parse(now),
        )
        .slice(0, 20);
      for (const item of candidates) {
        const connection = this.store.db
          .prepare('SELECT endpoint,enabled FROM webhook_connections WHERE workspace_id=?')
          .get(item.workspaceId) as { endpoint: string; enabled: number } | undefined;
        if (!connection?.enabled) continue;
        const state = this.store.read();
        const signal = state.signals.find((s) => s.id === item.signalId);
        if (!signal || ['resolved', 'accepted-risk'].includes(signal.state)) continue;
        // payload is scoped to this workspace and does not include credentials or unrelated objects.
        let delivered = false;
        try {
          await sendWebhook(connection.endpoint, {
            id: item.id,
            workspaceId: item.workspaceId,
            signal,
            summary: item.summary,
            occurredAt: signal.createdAt,
          });
          delivered = true;
        } catch {
          delivered = false;
        }
        this.store.transaction((current) => {
          const n = current.notifications.find((n) => n.id === item.id);
          if (!n) return false;
          n.attempts++;
          n.state = delivered ? 'delivered' : 'failed';
          if (delivered) n.deliveredAt = now;
          else
            n.scheduledAt = new Date(
              Date.parse(now) + Math.min(60, 2 ** n.attempts) * 60000,
            ).toISOString();
          this.store.db
            .prepare('UPDATE webhook_connections SET status=?,updated_at=? WHERE workspace_id=?')
            .run(delivered ? 'delivered' : 'delivery-failed', now, item.workspaceId);
          return true;
        }, now);
      }
    } finally {
      this.delivering = false;
    }
  }
}
