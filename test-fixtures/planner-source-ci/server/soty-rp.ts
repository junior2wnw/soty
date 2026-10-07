import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import type { PlannerStore } from './store.ts';
import { ApiError } from './validation.ts';
import {
  createSourceRpSessionService,
  sourceRpCipherBinding,
  SOURCE_RP_LIMITS,
  type SourceRpMarker,
  type SourceRpHead,
  type SourceRpStoragePort,
  type SourceRpProtocol,
} from './source-rp/index.mjs';
import { installPlannerRpFormat } from './soty-rp-format.ts';

const hash = (value: string) => createHash('sha256').update(value).digest('hex');
export type RootContext = {
  reference: { id: string; version: number; digest: string };
  rootPrincipal: { accountId: string; deviceId: string };
  humanPrincipal: {
    issuer: string;
    subject: string;
    clientId: string;
    clientProfileDigest: string;
    clientGeneration: number;
  };
  expiresAt: number;
};
type Anchor = {
  marker: SourceRpMarker;
  userId: string;
  workspaceId: string;
  rootLocator: string;
  linkCreatedAt: number;
  grantCreatedAt: number;
};
type Authority = {
  anchorCipher: string;
  generation: number;
  userId: string;
  linkCreatedAt: number;
  grantCreatedAt: number;
};
const need = (ok: unknown, code = 'embed_session_required', status = 401) => {
  if (!ok) throw new ApiError(status, 'Войдите своим профилем Сот заново', code);
};
export interface PlannerRpOptions {
  profileDigest: string;
  bindingDigest: string;
  issuer: string;
  clientId: string;
  workspaceId: string;
  key: Buffer;
  allowMigration: boolean;
  protocol: SourceRpProtocol;
  clock?: () => number;
}
/** Source-owned native session/link/grant + encrypted CAS. Root metadata is only a locator. */
export function createPlannerRpSessions(store: PlannerStore, options: PlannerRpOptions) {
  installPlannerRpFormat(store.db, options.allowMigration);
  const now = options.clock ?? Date.now,
    key = Buffer.from(options.key),
    keyId = 'planner-rp-' + hash(key.toString('base64url')).slice(0, 24);
  need(
    key.length === 32 &&
      /^[a-f0-9]{64}$/.test(options.profileDigest) &&
      /^[a-f0-9]{64}$/.test(options.bindingDigest),
    'embed_configuration_invalid',
    503,
  );
  async function encrypt(model: string, binding: string, value: unknown) {
    const iv = randomBytes(12),
      cipher = createCipheriv('aes-256-gcm', key, iv);
    cipher.setAAD(Buffer.from(JSON.stringify([model, binding, keyId])));
    return Buffer.concat([
      iv,
      cipher.update(JSON.stringify(value)),
      cipher.final(),
      cipher.getAuthTag(),
    ]).toString('base64url');
  }
  async function decrypt<T>(
    model: string,
    binding: string,
    value: string,
    expectedKey: string,
  ): Promise<T> {
    need(expectedKey === keyId, 'embed_storage_key_unavailable', 503);
    try {
      const bytes = Buffer.from(value, 'base64url'),
        cipher = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
      cipher.setAAD(Buffer.from(JSON.stringify([model, binding, keyId])));
      cipher.setAuthTag(bytes.subarray(-16));
      return JSON.parse(
        Buffer.concat([cipher.update(bytes.subarray(12, -16)), cipher.final()]).toString(),
      );
    } catch {
      throw new ApiError(
        503,
        'Сохранённый вход недоступен. Войдите заново',
        'embed_storage_key_unavailable',
      );
    }
  }
  const anchorBinding = (id: string) =>
    JSON.stringify([
      'planner.source-rp-anchor.v2',
      id,
      options.profileDigest,
      options.bindingDigest,
    ]);
  const readHead = (id: string): SourceRpHead | null => {
    const row = store.db
      .prepare('SELECT document FROM planner_soty_rp_heads WHERE session_hash=?')
      .get(id) as { document: string } | undefined;
    return row ? JSON.parse(row.document) : null;
  };
  const writeHead = (head: SourceRpHead) =>
    store.db
      .prepare('UPDATE planner_soty_rp_heads SET document=? WHERE session_hash=?')
      .run(JSON.stringify(head), head.sessionIdHash);
  function native(marker: SourceRpMarker, expected?: Authority): Authority {
    const row = store.db
      .prepare('SELECT * FROM planner_soty_rp_anchors WHERE session_hash=?')
      .get(marker.sessionIdHash) as
      | {
          payload_cipher: string;
          generation: number;
          active: number;
          profile_digest: string;
          binding_digest: string;
          expires_at: number;
          created_at: number;
          key_id: string;
        }
      | undefined;
    need(
      row &&
        row.active === 1 &&
        row.expires_at > now() &&
        row.expires_at === marker.sessionExpiresAt &&
        row.created_at === marker.createdAt &&
        row.profile_digest === marker.profileDigest &&
        row.binding_digest === marker.bindingDigest,
      'embed_session_required',
    );
    need(row!.key_id === keyId, 'embed_storage_key_unavailable', 503);
    // Immutable Source link and actual selected grant remain native authority; Root account is not a Source account.
    const link = store.db
      .prepare(
        'SELECT user_id,created_at FROM planner_soty_links WHERE issuer=? AND subject=? AND workspace_id=?',
      )
      .get(marker.issuer, marker.subject, options.workspaceId) as
      { user_id: string; created_at: number } | undefined;
    const grant = store.db
      .prepare(
        'SELECT created_at FROM planner_soty_grants WHERE issuer=? AND subject=? AND workspace_id=? AND binding_digest=?',
      )
      .get(marker.issuer, marker.subject, options.workspaceId, options.bindingDigest) as
      { created_at: number } | undefined;
    need(link && grant, 'embed_link_required', 403);
    const user = store.user(link!.user_id);
    need(user, 'embed_account_unavailable');
    store.requireRole(user!, options.workspaceId);
    const snapshot = {
      anchorCipher: row!.payload_cipher,
      generation: row!.generation,
      userId: link!.user_id,
      linkCreatedAt: link!.created_at,
      grantCreatedAt: grant!.created_at,
    };
    if (expected)
      need(JSON.stringify(snapshot) === JSON.stringify(expected), 'embed_session_changed');
    return snapshot;
  }
  const storagePort: SourceRpStoragePort<Authority> = {
    async read(id) {
      return readHead(id);
    },
    async captureSourceAuthority(marker) {
      return native(marker);
    },
    async assertSourceAuthority(marker, snapshot) {
      native(marker, snapshot);
    },
    async claim({ marker, authority, expected, claimId, claimedAt }) {
      let done = false;
      store.transaction(() => {
        native(marker, authority);
        const head = readHead(marker.sessionIdHash);
        if (head && head.state === 'idle' && JSON.stringify(head) === JSON.stringify(expected)) {
          writeHead({ ...head, state: 'refreshing', claimId, claimedAt, updatedAt: claimedAt });
          done = true;
        }
        return false;
      });
      return done;
    },
    async finish({
      marker,
      authority,
      expected,
      claimId,
      proofCipher,
      accessExpiresAt,
      updatedAt,
    }) {
      let done = false;
      store.transaction(() => {
        native(marker, authority);
        const head = readHead(marker.sessionIdHash);
        if (
          head?.state === 'refreshing' &&
          head.revision === expected.revision &&
          head.claimId === claimId &&
          head.proofCipher === expected.proofCipher &&
          head.profileDigest === expected.profileDigest &&
          head.bindingDigest === expected.bindingDigest
        ) {
          writeHead({
            ...head,
            state: 'idle',
            revision: head.revision + 1,
            proofCipher,
            accessExpiresAt,
            claimId: null,
            claimedAt: null,
            lastAttemptId: claimId,
            updatedAt,
          });
          done = true;
        }
        return false;
      });
      return done;
    },
    async block({ expected, claimId, state, updatedAt }) {
      store.transaction(() => {
        const head = readHead(expected.sessionIdHash);
        if (
          head?.state === 'refreshing' &&
          head.revision === expected.revision &&
          head.claimId === claimId
        )
          writeHead({ ...head, state, updatedAt });
        return false;
      });
    },
    async compactExpired({ now: at, limit }) {
      store.transaction(() => {
        store.db
          .prepare(
            'DELETE FROM planner_soty_rebind_receipts WHERE request_hash IN(SELECT request_hash FROM planner_soty_rebind_receipts WHERE expires_at<=? LIMIT ?)',
          )
          .run(at, limit);
        store.db
          .prepare(
            'DELETE FROM planner_soty_rp_anchors WHERE session_hash IN(SELECT session_hash FROM planner_soty_rp_anchors WHERE expires_at<=? ORDER BY expires_at LIMIT ?)',
          )
          .run(at, limit);
        return false;
      });
    },
  };
  const service = createSourceRpSessionService({
    storagePort,
    protocol: options.protocol,
    profileDigest: options.profileDigest,
    keyId,
    encrypt,
    decrypt,
    clock: now,
  });
  function locator(context: RootContext) {
    need(
      context &&
        context.humanPrincipal.issuer === options.issuer &&
        context.humanPrincipal.clientId === options.clientId &&
        context.reference &&
        context.rootPrincipal.accountId &&
        context.rootPrincipal.deviceId &&
        context.expiresAt > now(),
      'embed_profile_changed',
    );
    return hash(
      JSON.stringify([
        'planner.source-rp-locator.v2',
        options.profileDigest,
        options.bindingDigest,
        context.rootPrincipal.accountId,
        context.rootPrincipal.deviceId,
        context.humanPrincipal.issuer,
        context.humanPrincipal.subject,
        context.humanPrincipal.clientId,
        context.humanPrincipal.clientProfileDigest,
        context.humanPrincipal.clientGeneration,
      ]),
    );
  }
  async function anchor(id: string) {
    const row = store.db
      .prepare(
        'SELECT payload_cipher,key_id FROM planner_soty_rp_anchors WHERE session_hash=? AND active=1 AND expires_at>?',
      )
      .get(id, now()) as { payload_cipher: string; key_id: string } | undefined;
    need(row, 'embed_session_required');
    const value = await decrypt<Anchor>(
      'PlannerRpAnchor',
      anchorBinding(id),
      row!.payload_cipher,
      row!.key_id,
    );
    need(
      value.marker.sessionIdHash === id && value.workspaceId === options.workspaceId,
      'embed_profile_changed',
    );
    const current = native(value.marker);
    need(
      current.userId === value.userId &&
        current.linkCreatedAt === value.linkCreatedAt &&
        current.grantCreatedAt === value.grantCreatedAt,
      'embed_link_required',
      403,
    );
    return value;
  }
  async function prove(marker: SourceRpMarker) {
    const current = await service.currentProof(marker),
      head = readHead(marker.sessionIdHash);
    need(head, 'embed_session_required');
    const token = await decrypt<{ accessToken: string }>(
      'SourceRpRenewal',
      sourceRpCipherBinding(marker, head!.revision),
      head!.proofCipher,
      head!.keyId,
    );
    await current.assertCurrent();
    return { current, accessToken: token.accessToken };
  }
  return Object.freeze({
    service,
    locator,
    async create({
      proof,
      userId,
      context,
      loginStartedAt,
    }: {
      proof: {
        issuer: string;
        subject: string;
        accessToken: string;
        expiresAt: number;
        refreshToken: string;
        nonce: string;
      };
      userId: string;
      context: RootContext;
      loginStartedAt: number;
    }) {
      await service.compactExpired();
      const rootLocator = locator(context);
      need(
        proof.issuer === options.issuer && proof.subject === context.humanPrincipal.subject,
        'embed_profile_changed',
      );
      const createdAt = loginStartedAt,
        sessionExpiresAt = createdAt + SOURCE_RP_LIMITS.seconds * 1000;
      need(
        Number.isSafeInteger(createdAt) && createdAt > 0 && sessionExpiresAt > now(),
        'embed_session_required',
      );
      const marker: SourceRpMarker = {
        sessionIdHash: hash(randomBytes(32).toString('base64url')),
        profileDigest: options.profileDigest,
        bindingDigest: options.bindingDigest,
        issuer: proof.issuer,
        subject: proof.subject,
        createdAt,
        sessionExpiresAt,
      };
      const link = store.db
        .prepare(
          'SELECT user_id,created_at FROM planner_soty_links WHERE issuer=? AND subject=? AND workspace_id=?',
        )
        .get(proof.issuer, proof.subject, options.workspaceId) as
        { user_id: string; created_at: number } | undefined;
      const grant = store.db
        .prepare(
          'SELECT created_at FROM planner_soty_grants WHERE issuer=? AND subject=? AND workspace_id=? AND binding_digest=?',
        )
        .get(proof.issuer, proof.subject, options.workspaceId, options.bindingDigest) as
        { created_at: number } | undefined;
      need(link?.user_id === userId && grant, 'embed_link_required', 403);
      const user = store.user(userId);
      need(user, 'embed_account_unavailable');
      store.requireRole(user!, options.workspaceId);
      const value: Anchor = {
        marker,
        userId,
        workspaceId: options.workspaceId,
        rootLocator,
        linkCreatedAt: link!.created_at,
        grantCreatedAt: grant!.created_at,
      };
      const cipher = await encrypt('PlannerRpAnchor', anchorBinding(marker.sessionIdHash), value),
        proofCipher = await encrypt('SourceRpRenewal', sourceRpCipherBinding(marker, 0), {
          accessToken: proof.accessToken,
          refreshToken: proof.refreshToken,
          nonce: proof.nonce,
        });
      store.transaction(() => {
        const current = store.user(userId);
        need(current, 'embed_account_unavailable');
        store.requireRole(current!, options.workspaceId);
        need(
          (store.db
            .prepare(
              'SELECT count(*) AS n FROM planner_soty_rp_anchors WHERE active=1 AND expires_at>?',
            )
            .get(now())!.n as number) < SOURCE_RP_LIMITS.heads,
          'embed_capacity',
          503,
        );
        need(
          (store.db
            .prepare(
              'SELECT count(*) AS n FROM planner_soty_rp_anchors WHERE locator_hash=? AND active=1 AND expires_at>?',
            )
            .get(rootLocator, now())!.n as number) < SOURCE_RP_LIMITS.perAccount,
          'embed_capacity',
          503,
        );
        const liveLink = store.db
          .prepare(
            'SELECT user_id,created_at FROM planner_soty_links WHERE issuer=? AND subject=? AND workspace_id=?',
          )
          .get(proof.issuer, proof.subject, options.workspaceId) as
          { user_id: string; created_at: number } | undefined;
        const liveGrant = store.db
          .prepare(
            'SELECT created_at FROM planner_soty_grants WHERE issuer=? AND subject=? AND workspace_id=? AND binding_digest=?',
          )
          .get(proof.issuer, proof.subject, options.workspaceId, options.bindingDigest) as
          { created_at: number } | undefined;
        need(
          liveLink?.user_id === value.userId &&
            liveLink.created_at === value.linkCreatedAt &&
            liveGrant?.created_at === value.grantCreatedAt,
          'embed_link_required',
          403,
        );
        store.db
          .prepare('INSERT INTO planner_soty_rp_anchors VALUES(?,?,?,?,?,?,?,?,1,1)')
          .run(
            marker.sessionIdHash,
            rootLocator,
            options.profileDigest,
            options.bindingDigest,
            cipher,
            keyId,
            sessionExpiresAt,
            createdAt,
          );
        const head: SourceRpHead = {
          sessionIdHash: marker.sessionIdHash,
          profileDigest: marker.profileDigest,
          bindingDigest: marker.bindingDigest,
          sessionExpiresAt,
          accessExpiresAt: Math.min(proof.expiresAt, sessionExpiresAt),
          revision: 0,
          state: 'idle',
          proofCipher,
          keyId,
          claimId: null,
          claimedAt: null,
          lastAttemptId: null,
          createdAt,
          updatedAt: createdAt,
        };
        store.db
          .prepare('INSERT INTO planner_soty_rp_heads VALUES(?,?)')
          .run(marker.sessionIdHash, JSON.stringify(head));
        return false;
      });
      return marker;
    },
    async current(marker: SourceRpMarker, context: RootContext) {
      const value = await anchor(marker.sessionIdHash);
      need(value.rootLocator === locator(context), 'embed_profile_changed');
      return prove(marker);
    },
    async resume(context: RootContext) {
      await service.compactExpired();
      const rootLocator = locator(context),
        row = store.db
          .prepare(
            'SELECT session_hash FROM planner_soty_rp_anchors WHERE locator_hash=? AND profile_digest=? AND binding_digest=? AND active=1 AND expires_at>? ORDER BY created_at DESC LIMIT 1',
          )
          .get(rootLocator, options.profileDigest, options.bindingDigest, now()) as
          { session_hash: string } | undefined;
      need(row, 'embed_session_required');
      const value = await anchor(row!.session_hash);
      need(value.rootLocator === rootLocator, 'embed_profile_changed');
      const proved = await prove(value.marker);
      return { ...value, ...proved };
    },
    async readReceipt(requestId: string, context: RootContext) {
      const rootLocator = locator(context),
        requestHash = hash(rootLocator + '\0' + requestId),
        row = store.db
          .prepare(
            'SELECT * FROM planner_soty_rebind_receipts WHERE request_hash=? AND expires_at>?',
          )
          .get(requestHash, now()) as
          | {
              intent_digest: string;
              session_hash: string;
              continuation_digest: string;
              payload_cipher: string;
              key_id: string;
            }
          | undefined;
      if (!row) return null;
      need(
        row.continuation_digest === hash(JSON.stringify(context.reference)),
        'embed_resume_request_conflict',
        409,
      );
      const value = await anchor(row.session_hash);
      need(value.rootLocator === rootLocator, 'embed_profile_changed');
      await prove(value.marker);
      const receipt = await decrypt<{ token: string; expiresAt: number }>(
        'PlannerRebindReceipt',
        requestHash + ':' + row.intent_digest,
        row.payload_cipher,
        row.key_id,
      );
      return { ...receipt, marker: value.marker, userId: value.userId };
    },
    state(marker: SourceRpMarker) {
      return readHead(marker.sessionIdHash);
    },
    async commitReceipt({
      requestId,
      context,
      marker,
      token,
      expiresAt,
    }: {
      requestId: string;
      context: RootContext;
      marker: SourceRpMarker;
      token: string;
      expiresAt: number;
    }) {
      await service.compactExpired();
      const rootLocator = locator(context),
        requestHash = hash(rootLocator + '\0' + requestId),
        continuationDigest = hash(JSON.stringify(context.reference)),
        intentDigest = hash(
          JSON.stringify([rootLocator, marker.sessionIdHash, continuationDigest]),
        );
      const cipher = await encrypt('PlannerRebindReceipt', requestHash + ':' + intentDigest, {
        token,
        expiresAt,
      });
      let same = false;
      store.transaction(() => {
        native(marker);
        const previous = store.db
          .prepare('SELECT intent_digest FROM planner_soty_rebind_receipts WHERE request_hash=?')
          .get(requestHash) as { intent_digest: string } | undefined;
        if (previous) {
          need(previous.intent_digest === intentDigest, 'embed_resume_request_conflict', 409);
          same = true;
          return false;
        }
        need(
          (store.db
            .prepare('SELECT count(*) AS n FROM planner_soty_rebind_receipts WHERE expires_at>?')
            .get(now())!.n as number) < 1024,
          'embed_capacity',
          503,
        );
        store.db
          .prepare('INSERT INTO planner_soty_rebind_receipts VALUES(?,?,?,?,?,?,?,?)')
          .run(
            requestHash,
            intentDigest,
            marker.sessionIdHash,
            continuationDigest,
            cipher,
            keyId,
            expiresAt,
            now(),
          );
        return false;
      });
      return { same };
    },
  });
}
