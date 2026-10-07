import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto';
import type { IncomingMessage, ServerResponse } from 'node:http';
import type { PlannerStore } from './store.ts';
import type { User } from '../shared/types.ts';
import { ApiError } from './validation.ts';
import { createSotyBffProtocol, type SotyBffProfile } from './soty-protocol.ts';
import { startEmbedLogin } from '../shared/embed-login.mjs';
import { createPlannerRpSessions, type RootContext } from './soty-rp.ts';
import { inspectPlannerRpFormat } from './soty-rp-format.ts';
import type { SourceRpMarker } from './source-rp/index.mjs';

export interface PlannerEmbedConfiguration {
  nativeOrigin: string;
  embedOrigin: string;
  parentOrigin: string;
  workspaceId: string;
  profile: SotyBffProfile;
  /** Approved host key; never browser/descriptor/Host-derived configuration. */
  sessionKey: string;
  /** Explicit installed format/admission. A manifest cannot enable durable renewal. */
  renewal?: { admissionEnabled: boolean; allowMigration: boolean };
  /** Trusted connector binding to the current Root profile. An HTTP header,
   * email, display name, app grant or localhost address cannot provide it. */
  currentSotySubject(
    request: IncomingMessage,
    expected?: { proof: Proof; continuation?: LaunchContinuation },
  ): Promise<{ issuer: string; subject: string } | null>;
  /** Optional closed authenticated connector profile. Default mode does not
   * receive requests/identity through an unsigned header or generic proxy. */
  bridge?: {
    /** Stable approved app/resource/semantic-profile consent pin, not a launch ID. */
    consentDigest: string;
    verifyReady?(request: IncomingMessage): Record<string, string>;
    verifyRequest(request: IncomingMessage, body: Buffer): void | Promise<void>;
    continuation(request: IncomingMessage): LaunchContinuation;
    /** Fresh authenticated channel read; not a Human login or Source permission. */
    assertCurrent?(request: IncomingMessage): Promise<void>;
    /** Private verifier result for this exact MAC-authenticated request. */
    context?(request: IncomingMessage): RootContext;
  };
}
type LaunchContinuation = { id: string; version: number; digest: string };
type Proof = {
  issuer: string;
  subject: string;
  accessToken: string;
  expiresAt: number;
  refreshToken?: string;
  nonce?: string;
  loginStartedAt?: number;
  rpMarker?: SourceRpMarker;
  continuation?: LaunchContinuation;
  bindingDigest?: string;
};
type Session = Proof & { userId: string; workspaceId: string };
type Intent = {
  verifier: string;
  state: string;
  nonce: string;
  expiresAt: number;
  loginStartedAt?: number;
  continuation?: LaunchContinuation;
  bindingDigest?: string;
  prepareCsrfHash?: string;
};
type Preparation = {
  csrf: string;
  expiresAt: number;
  continuation?: LaunchContinuation;
  bindingDigest: string;
};
type Consent = Proof & { workspaceId: string };
const hash = (value: string) => createHash('sha256').update(value).digest('hex');
function need(ok: unknown, code = 'embed_invalid', status = 400): asserts ok {
  if (!ok) throw new ApiError(status, 'Подключение недоступно', code);
}
const escape = (text: string) =>
  text.replace(
    /[&<>"']/g,
    (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]!,
  );
function approvedOrigin(value: string) {
  const url = new URL(value);
  need(
    url.origin === value &&
      !url.username &&
      !url.password &&
      (url.protocol === 'https:' ||
        (url.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname))),
    'embed_configuration_invalid',
    503,
  );
  return url;
}
const cookie = (req: IncomingMessage, name: string) => {
  const values = (req.headers.cookie ?? '')
    .split(';')
    .map((v) => v.trim())
    .filter((v) => v.startsWith(name + '='));
  return values.length === 1 ? values[0].slice(name.length + 1) : '';
};
function setCookie(res: ServerResponse, origin: string, name: string, token: string, maxAge = 300) {
  res.setHeader(
    'Set-Cookie',
    `${name}=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAge}${origin.startsWith('https:') ? '; Secure' : ''}`,
  );
}
function html(
  res: ServerResponse,
  title: string,
  content: string,
  script = '',
  formOrigin = '',
  referrerPolicy: 'no-referrer' | 'origin' = 'no-referrer',
) {
  // HTML parsing normalizes CRLF/CR. Hash the exact normalized script that the
  // browser executes, including helpers checked out on Windows. Keep strict CSP.
  script = script.replace(/\r\n?/g, '\n');
  res.setHeader('Referrer-Policy', referrerPolicy);
  res.setHeader(
    'Content-Security-Policy',
    (res.getHeader('Content-Security-Policy') ?? '').toString() +
      `; default-src 'none'; connect-src 'self'; style-src 'unsafe-inline'; script-src ${script ? `'sha256-${createHash('sha256').update(script).digest('base64')}'` : "'none'"}; base-uri 'none'; form-action 'self' ${formOrigin}`,
  );
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(
    `<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escape(title)}</title><style>body{font:17px system-ui;line-height:1.6;margin:0;padding:24px;color:#14201c;background:#f6f8f7}main{max-width:620px;margin:8vh auto}a,button{display:inline-block;box-sizing:border-box;min-height:44px;padding:10px 16px;border:1px solid #24775b;border-radius:12px;background:#fff;color:#145238;font:inherit;cursor:pointer}a:focus-visible,button:focus-visible{outline:3px solid #24775b;outline-offset:3px}small{display:block;margin-top:16px}</style><main><h1>${escape(title)}</h1>${content}</main>${script ? `<script>${script}</script>` : ''}</html>`,
  );
}
async function form(req: IncomingMessage) {
  need(
    /^application\/x-www-form-urlencoded(?:;|$)/i.test(req.headers['content-type'] ?? ''),
    'embed_invalid',
  );
  let text = '',
    bytes = 0;
  for await (const part of req) {
    bytes += part.length;
    need(bytes <= 4096);
    text += part.toString();
  }
  const fields = new URLSearchParams(text);
  need([...fields.keys()].length === 1 && fields.has('intent'));
  return fields.get('intent')!;
}

/** The same source-owned UI/store stays authoritative. This BFF admits one
 * explicit workspace association after independent Soty and native proofs. */
export function createPlannerEmbed(
  store: PlannerStore,
  input: PlannerEmbedConfiguration,
  protocol = createSotyBffProtocol(input.profile),
) {
  const native = approvedOrigin(input.nativeOrigin),
    embedded = approvedOrigin(input.embedOrigin),
    parent = approvedOrigin(input.parentOrigin);
  need(
    native.origin !== embedded.origin &&
      native.hostname !== embedded.hostname &&
      parent.origin !== embedded.origin,
    'embed_origins_must_be_distinct',
    503,
  );
  need(
    typeof input.currentSotySubject === 'function' && /^[A-Za-z0-9_-]{43}$/.test(input.sessionKey),
    'embed_configuration_invalid',
    503,
  );
  need(
    input.profile.redirectUri === embedded.origin + '/api/embed/callback',
    'embed_callback_invalid',
    503,
  );
  if (input.bridge)
    need(/^[a-f0-9]{64}$/.test(input.bridge.consentDigest), 'embed_configuration_invalid', 503);
  const configuration = Object.freeze({
    ...input,
    profile: Object.freeze({ ...input.profile }),
    ...(input.bridge ? { bridge: Object.freeze({ ...input.bridge }) } : {}),
  });
  const bindingDigest =
    configuration.bridge?.consentDigest ??
    hash(
      JSON.stringify({
        schema: 'planner.source-selected-consent.v1',
        native: native.origin,
        embedded: embedded.origin,
        parent: parent.origin,
        workspaceId: input.workspaceId,
        issuer: input.profile.issuer,
        clientId: input.profile.clientId,
      }),
    );
  const key = Buffer.from(input.sessionKey, 'base64url');
  need(key.length === 32, 'embed_configuration_invalid', 503);
  const aad = Buffer.from(
    hash(
      JSON.stringify({
        native: native.origin,
        embedded: embedded.origin,
        parent: parent.origin,
        workspaceId: input.workspaceId,
        issuer: input.profile.issuer,
        clientId: input.profile.clientId,
        bindingDigest,
      }),
    ),
  );
  store.db
    .exec(`CREATE TABLE IF NOT EXISTS planner_soty_links (issuer TEXT NOT NULL,subject TEXT NOT NULL,workspace_id TEXT NOT NULL,user_id TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(issuer,subject,workspace_id));
    CREATE TABLE IF NOT EXISTS planner_soty_private (kind TEXT NOT NULL,key_hash TEXT NOT NULL,payload_cipher TEXT NOT NULL,expires_at INTEGER NOT NULL,PRIMARY KEY(kind,key_hash));
    CREATE TABLE IF NOT EXISTS planner_soty_grants (issuer TEXT NOT NULL,subject TEXT NOT NULL,workspace_id TEXT NOT NULL,binding_digest TEXT NOT NULL,created_at INTEGER NOT NULL,PRIMARY KEY(issuer,subject,workspace_id,binding_digest));`);
  const rpProfileDigest = hash(
    JSON.stringify([
      'planner.source-rp-profile.v2',
      configuration.profile.issuer,
      configuration.profile.clientId,
      configuration.profile.redirectUri,
      hash(configuration.profile.clientSecret),
      bindingDigest,
    ]),
  );
  const rpEnabled =
    configuration.renewal?.admissionEnabled === true || inspectPlannerRpFormat(store.db) === 2;
  if (rpEnabled)
    need(
      configuration.profile.renewalProfile === 'soty.human-rp-renewal.v1' &&
        configuration.bridge?.context &&
        configuration.bridge.assertCurrent,
      'embed_renewal_configuration_invalid',
      503,
    );
  const rpSessions = rpEnabled
    ? createPlannerRpSessions(store, {
        profileDigest: rpProfileDigest,
        bindingDigest,
        issuer: configuration.profile.issuer,
        clientId: configuration.profile.clientId,
        workspaceId: configuration.workspaceId,
        key,
        allowMigration: configuration.renewal?.allowMigration === true,
        protocol,
      })
    : null;
  function hasGrant(proof: Proof) {
    return !!store.db
      .prepare(
        'SELECT 1 FROM planner_soty_grants WHERE issuer=? AND subject=? AND workspace_id=? AND binding_digest=?',
      )
      .get(proof.issuer, proof.subject, configuration.workspaceId, bindingDigest);
  }
  function seal(value: unknown) {
    const iv = randomBytes(12),
      cipher = createCipheriv('aes-256-gcm', key, iv);
    cipher.setAAD(aad);
    return Buffer.concat([
      iv,
      cipher.update(JSON.stringify(value)),
      cipher.final(),
      cipher.getAuthTag(),
    ]).toString('base64url');
  }
  function unseal(value: string) {
    const bytes = Buffer.from(value, 'base64url');
    need(bytes.length >= 29);
    const decipher = createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
    decipher.setAAD(aad);
    decipher.setAuthTag(bytes.subarray(-16));
    return JSON.parse(
      Buffer.concat([decipher.update(bytes.subarray(12, -16)), decipher.final()]).toString(),
    );
  }
  function save<T extends { expiresAt: number }>(
    kind: string,
    value: T,
    storageExpiresAt = value.expiresAt,
  ) {
    store.db.prepare('DELETE FROM planner_soty_private WHERE expires_at<=?').run(Date.now());
    need(
      (store.db.prepare('SELECT count(*) AS n FROM planner_soty_private').get() as { n: number })
        .n < 256,
      'embed_capacity',
      503,
    );
    const token = randomBytes(32).toString('base64url');
    store.db
      .prepare(
        'INSERT INTO planner_soty_private(kind,key_hash,payload_cipher,expires_at) VALUES(?,?,?,?)',
      )
      .run(kind, hash(token), seal(value), storageExpiresAt);
    return token;
  }
  function load<T>(kind: string, token: string): T {
    need(/^[A-Za-z0-9_-]{43}$/.test(token), 'embed_session_required', 401);
    const row = store.db
      .prepare(
        'SELECT payload_cipher FROM planner_soty_private WHERE kind=? AND key_hash=? AND expires_at>?',
      )
      .get(kind, hash(token), Date.now()) as { payload_cipher: string } | undefined;
    need(row, 'embed_session_required', 401);
    try {
      return unseal(row.payload_cipher);
    } catch {
      throw new ApiError(401, 'Войдите заново', 'embed_session_required');
    }
  }
  function remove(kind: string, token: string) {
    store.db
      .prepare('DELETE FROM planner_soty_private WHERE kind=? AND key_hash=?')
      .run(kind, hash(token));
  }
  async function live(req: IncomingMessage, proof: Proof) {
    need(
      proof.issuer === configuration.profile.issuer && proof.expiresAt > Date.now(),
      'embed_proof_expired',
      401,
    );
    need(proof.bindingDigest === bindingDigest, 'embed_binding_changed', 401);
    let liveProof = proof;
    if (proof.rpMarker) {
      need(rpSessions && configuration.bridge?.context, 'embed_session_required', 401);
      try {
        const current = await rpSessions!.current(
          proof.rpMarker,
          configuration.bridge!.context!(req),
        );
        liveProof = {
          ...proof,
          accessToken: current.accessToken,
          expiresAt: current.current.expiresAt,
        };
      } catch (error) {
        throw new ApiError(
          (error as { status?: number }).status === 503 ? 503 : 401,
          'Войдите своим профилем Сот заново',
          (error as { status?: number }).status === 503
            ? 'embed_provider_unavailable'
            : 'embed_session_required',
        );
      }
    }
    async function currentSubject() {
      try {
        return await configuration.currentSotySubject(req, {
          proof: liveProof,
          continuation: proof.continuation,
        });
      } catch (error) {
        const status = (error as { status?: number })?.status;
        if (status === 401 || status === 403)
          throw new ApiError(
            status,
            'Профиль или доступ изменился. Войдите заново.',
            'embed_profile_changed',
          );
        throw new ApiError(
          503,
          'Сервис входа временно недоступен. Повторите позже.',
          'embed_provider_unavailable',
        );
      }
    }
    const current = await currentSubject();
    need(
      current?.issuer === proof.issuer && current.subject === proof.subject,
      'embed_profile_changed',
      401,
    );
    try {
      await protocol.currentSubject(liveProof.accessToken, proof.subject);
    } catch (error) {
      if ((error as { status?: number })?.status === 503)
        throw new ApiError(
          503,
          'Сервис входа временно недоступен. Повторите позже.',
          'embed_provider_unavailable',
        );
      throw new ApiError(
        401,
        'Профиль или доступ изменился. Войдите заново.',
        'embed_proof_revoked',
      );
    }
    const after = await currentSubject();
    need(
      after?.issuer === proof.issuer && after.subject === proof.subject,
      'embed_profile_changed',
      401,
    );
  }
  async function authorize(req: IncomingMessage) {
    need(req.headers.host === embedded.host, 'embed_origin_invalid', 403);
    const session = load<Session>('session', cookie(req, 'planner_soty_session'));
    await live(req, session);
    const row = store.db
      .prepare(
        'SELECT user_id FROM planner_soty_links WHERE issuer=? AND subject=? AND workspace_id=?',
      )
      .get(session.issuer, session.subject, configuration.workspaceId) as
      { user_id: string } | undefined;
    need(
      row?.user_id === session.userId &&
        session.workspaceId === configuration.workspaceId &&
        hasGrant(session),
      'embed_link_required',
      403,
    );
    const user = store.user(session.userId);
    need(user, 'embed_account_unavailable', 401);
    store.requireRole(user, session.workspaceId);
    return { user, workspaceId: session.workspaceId };
  }
  function framePolicy(res: ServerResponse) {
    res.removeHeader('X-Frame-Options');
    res.setHeader('Content-Security-Policy', `frame-ancestors ${parent.origin}; object-src 'none'`);
    res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
    res.setHeader('Referrer-Policy', 'no-referrer');
  }
  function loginPage(req: IncomingMessage, res: ServerResponse) {
    framePolicy(res);
    html(
      res,
      'Планировщик в Сотах',
      '<p>Войдите своим профилем Сот. Локальные пространства откроются только после вашего разрешения.</p>' +
        (configuration.bridge
          ? '<button id="soty-login">Войти через Соты</button><p id="login-status" role="status"></p>'
          : '<a href="/api/embed/login" target="_blank" rel="opener">Войти через Соты</a>') +
        '<small>Обычный вход Планировщика доступен по его отдельному адресу.</small>',
      `window.addEventListener('message',event=>{if(event.origin===${JSON.stringify(embedded.origin)}&&event.data?.type==='planner-embed-ready'){location.reload();}});` +
        (configuration.bridge
          ? `const startEmbedLogin=${startEmbedLogin.toString()};let timer=null;function watch(){if(timer)return;const until=Date.now()+300000;let checking=false;timer=setInterval(()=>{if(checking||Date.now()>until){if(Date.now()>until)clearInterval(timer);return;}checking=true;void fetch('/api/embed/session-status',{credentials:'same-origin',cache:'no-store'}).then(async response=>{if(response.status===200){const value=await response.json();if(Object.keys(value).join(',')==='ready'&&value.ready===true){clearInterval(timer);try{await fetch('/api/embed/session-continue',{method:'POST',credentials:'same-origin',cache:'no-store',redirect:'error',headers:{'content-type':'application/json'},body:JSON.stringify({requestId:crypto.randomUUID().replaceAll('-','')})});}catch{}location.reload();}}else if(response.status===403){clearInterval(timer);document.getElementById('login-status').textContent='Доступ изменился. Откройте приложение заново в Сотах.';}}).catch(()=>{}).finally(()=>{checking=false;});},2000);}document.getElementById('soty-login').addEventListener('click',()=>{const button=document.getElementById('soty-login');button.disabled=true;void startEmbedLogin().then(watch).catch(error=>{document.getElementById('login-status').textContent=error.message;}).finally(()=>{button.disabled=false;});});`
          : ''),
    );
  }
  async function sessionFor(
    req: IncomingMessage,
    res: ServerResponse,
    proof: Proof,
    userId: string,
  ) {
    let sessionProof = proof;
    if (proof.refreshToken) {
      need(
        rpSessions &&
          configuration.renewal?.admissionEnabled === true &&
          proof.nonce &&
          proof.loginStartedAt &&
          configuration.bridge?.context,
        'embed_renewal_unavailable',
        503,
      );
      const marker = await rpSessions!.create({
        proof: {
          issuer: proof.issuer,
          subject: proof.subject,
          accessToken: proof.accessToken,
          expiresAt: proof.expiresAt,
          refreshToken: proof.refreshToken,
          nonce: proof.nonce!,
        },
        userId,
        context: configuration.bridge!.context!(req),
        loginStartedAt: proof.loginStartedAt!,
      });
      const { refreshToken: _rt, nonce: _nonce, ...rest } = proof;
      sessionProof = { ...rest, rpMarker: marker, expiresAt: marker.sessionExpiresAt };
    }
    await live(req, sessionProof);
    const token = save(
      'session',
      { ...sessionProof, userId, workspaceId: configuration.workspaceId },
      Math.min(Date.now() + 300000, sessionProof.expiresAt),
    );
    setCookie(res, embedded.origin, 'planner_soty_session', token);
    framePolicy(res);
    html(
      res,
      'Профиль подключён',
      '<p>Выбранное пространство готово. Вернитесь к открытой вкладке Сот — проект появится автоматически.</p><a href="' +
        escape(parent.origin) +
        '">Вернуться в Соты</a>',
      `if(window.opener){window.opener.postMessage({type:'planner-embed-ready'},${JSON.stringify(embedded.origin)});window.close();}`,
    );
  }
  const isEmbed = (req: IncomingMessage) => req.headers.host === embedded.host;
  const isNative = (req: IncomingMessage) => req.headers.host === native.host;
  async function handle(
    req: IncomingMessage,
    res: ServerResponse,
    url: URL,
    nativeUser: () => User,
    readBody?: () => Promise<Buffer>,
  ) {
    if (
      isEmbed(req) &&
      url.pathname === '/api/embed/session-status' &&
      req.method === 'GET' &&
      !url.search
    ) {
      let ready = true;
      try {
        await authorize(req);
      } catch (error) {
        if (error instanceof ApiError && error.status === 401) ready = false;
        else throw error;
      }
      framePolicy(res);
      res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(JSON.stringify({ ready }));
      return true;
    }
    if (
      isEmbed(req) &&
      url.pathname === '/api/embed/session-continue' &&
      req.method === 'POST' &&
      !url.search
    ) {
      need(
        configuration.bridge?.context && configuration.bridge.assertCurrent,
        'embed_resume_unavailable',
        503,
      );
      need(
        req.headers.origin === embedded.origin &&
          /^application\/json(?:;|$)/i.test(req.headers['content-type'] ?? ''),
        'embed_csrf_invalid',
        403,
      );
      need(readBody, 'embed_resume_unavailable', 503);
      const bytes = await readBody!();
      need(bytes.length <= 512);
      let input;
      try {
        input = JSON.parse(bytes.toString('utf8'));
      } catch {
        need(false);
      }
      need(
        input &&
          Object.keys(input).join(',') === 'requestId' &&
          typeof input.requestId === 'string' &&
          /^[A-Za-z0-9_-]{16,128}$/.test(input.requestId),
      );
      await configuration.bridge!.assertCurrent!(req);
      const context = configuration.bridge!.context!(req);
      // Basic300 confirms only its existing private alias at the SAME original
      // continuation. It cannot resume onto a new slot, and creates no RT head.
      const basicToken = cookie(req, 'planner_soty_session');
      let basic: Session | undefined;
      try {
        basic = load<Session>('session', basicToken);
      } catch (error) {
        if ((error as { status?: number }).status !== 401) throw error;
      }
      if (basic && !basic.rpMarker) {
        try {
          await authorize(req);
          await configuration.bridge!.assertCurrent!(req);
          const fresh = load<Session>('session', basicToken);
          need(
            hash(JSON.stringify(fresh)) === hash(JSON.stringify(basic)),
            'embed_session_required',
          );
          const link = store.db
            .prepare(
              'SELECT user_id FROM planner_soty_links WHERE issuer=? AND subject=? AND workspace_id=?',
            )
            .get(fresh.issuer, fresh.subject, configuration.workspaceId) as
            { user_id: string } | undefined;
          const user = store.user(fresh.userId);
          need(
            user && link?.user_id === fresh.userId && hasGrant(fresh),
            'embed_link_required',
            403,
          );
          store.requireRole(user!, configuration.workspaceId);
          const end = Math.min(fresh.expiresAt, context.expiresAt);
          need(end > Date.now(), 'embed_session_required');
          framePolicy(res);
          res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
          res.end(
            JSON.stringify({
              schema: 'soty.source-session-continuation.v1',
              ready: true,
              renewable: false,
              sessionExpiresAt: end,
              accessExpiresAt: end,
              receiptDigest: hash(
                JSON.stringify([
                  'planner.source-basic-readiness.v1',
                  input.requestId,
                  context.reference,
                  hash(basicToken),
                  end,
                ]),
              ),
            }),
          );
          return true;
        } catch (error) {
          if ((error as { status?: number }).status !== 401) throw error;
          framePolicy(res);
          res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
          res.end(
            JSON.stringify({
              schema: 'soty.source-session-continuation.v1',
              ready: false,
              reason: 'login_required',
            }),
          );
          return true;
        }
      }
      if (!rpSessions) {
        framePolicy(res);
        res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
        res.end(
          JSON.stringify({
            schema: 'soty.source-session-continuation.v1',
            ready: false,
            reason: 'login_required',
          }),
        );
        return true;
      }
      let marker: SourceRpMarker, token: string, expiresAt: number;
      try {
        const receipt = await rpSessions!.readReceipt(input.requestId, context);
        if (receipt) {
          marker = receipt.marker;
          token = receipt.token;
          expiresAt = receipt.expiresAt;
        } else {
          const resumed = await rpSessions!.resume(context);
          marker = resumed.marker;
          const renewedProof: Proof = {
            issuer: marker.issuer,
            subject: marker.subject,
            accessToken: resumed.accessToken,
            expiresAt: marker.sessionExpiresAt,
            rpMarker: marker,
            continuation: context.reference,
            bindingDigest,
          };
          await live(req, renewedProof);
          expiresAt = Math.min(Date.now() + 300000, context.expiresAt);
          token = save(
            'session',
            { ...renewedProof, userId: resumed.userId, workspaceId: configuration.workspaceId },
            expiresAt,
          );
          await rpSessions!.commitReceipt({
            requestId: input.requestId,
            context,
            marker,
            token,
            expiresAt,
          });
          const committed = await rpSessions!.readReceipt(input.requestId, context);
          need(committed, 'embed_resume_unknown', 503);
          if (committed!.token !== token) remove('session', token);
          token = committed!.token;
          expiresAt = committed!.expiresAt;
        }
        const session = load<Session>('session', token);
        need(
          session.rpMarker?.sessionIdHash === marker.sessionIdHash &&
            hash(JSON.stringify(session.continuation)) === hash(JSON.stringify(context.reference)),
          'embed_resume_unknown',
          503,
        );
        await live(req, session);
        await configuration.bridge!.assertCurrent!(req);
      } catch (error) {
        if ((error as { status?: number }).status === 401) {
          framePolicy(res);
          res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
          res.end(
            JSON.stringify({
              schema: 'soty.source-session-continuation.v1',
              ready: false,
              reason: 'login_required',
            }),
          );
          return true;
        }
        throw error;
      }
      const head = rpSessions!.state(marker!);
      need(head && head.accessExpiresAt > Date.now(), 'embed_session_required');
      setCookie(
        res,
        embedded.origin,
        'planner_soty_session',
        token!,
        Math.max(1, Math.floor((expiresAt! - Date.now()) / 1000)),
      );
      framePolicy(res);
      res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(
        JSON.stringify({
          schema: 'soty.source-session-continuation.v1',
          ready: true,
          sessionExpiresAt: marker!.sessionExpiresAt,
          accessExpiresAt: head!.accessExpiresAt,
          renewable: head!.state === 'idle',
          receiptDigest: hash(
            input.requestId + '\0' + context.reference.digest + '\0' + hash(token!),
          ),
        }),
      );
      return true;
    }
    if (
      isEmbed(req) &&
      url.pathname === '/api/embed/login' &&
      (req.method === 'POST' || (req.method === 'GET' && req.headers.accept === 'application/json'))
    ) {
      need(
        !url.search &&
          (!configuration.bridge || typeof configuration.bridge.assertCurrent === 'function'),
        'embed_login_preparation_unavailable',
        503,
      );
      const continuation = configuration.bridge?.continuation(req);
      const fresh = async () => {
        if (configuration.bridge) await configuration.bridge.assertCurrent!(req);
      };
      await fresh();
      if (req.method === 'GET') {
        const csrf = randomBytes(32).toString('base64url'),
          token = save('prepare', {
            csrf,
            ...(continuation ? { continuation } : {}),
            bindingDigest,
            expiresAt: Date.now() + 60000,
          });
        setCookie(res, embedded.origin, 'planner_soty_intent', token, 60);
        res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
        res.end(
          JSON.stringify({
            schema: 'planner.embed-login-preparation.v1',
            csrf,
            issuer: configuration.profile.issuer,
            clientId: configuration.profile.clientId,
            redirectUri: configuration.profile.redirectUri,
          }),
        );
        return true;
      }
      need(
        req.headers.origin === embedded.origin &&
          /^application\/json(?:;|$)/i.test(req.headers['content-type'] ?? ''),
        'embed_csrf_invalid',
        403,
      );
      need(typeof readBody === 'function', 'embed_login_preparation_unavailable', 503);
      const buffer = await readBody();
      need(buffer.length <= 1024, 'embed_csrf_invalid', 403);
      // Closed wire grammar also rejects duplicate keys before JSON parsing.
      const match =
        /^\s*\{\s*"csrf"\s*:\s*"([A-Za-z0-9_-]{43})"\s*(,\s*"cancel"\s*:\s*true\s*)?\}\s*$/.exec(
          buffer.toString('utf8'),
        );
      need(match, 'embed_csrf_invalid', 403);
      const csrf = match[1],
        cancelling = !!match[2],
        cookieToken = cookie(req, 'planner_soty_intent');
      if (cancelling) {
        const intent = load<Intent>('intent', cookieToken);
        need(
          intent.prepareCsrfHash === hash(csrf) &&
            JSON.stringify(intent.continuation) === JSON.stringify(continuation),
          'embed_csrf_invalid',
          403,
        );
        remove('intent', cookieToken);
        await fresh();
        setCookie(res, embedded.origin, 'planner_soty_intent', '', 0);
        res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
        res.end(
          JSON.stringify({
            schema: 'planner.embed-login-cancelled.v1',
            cancelled: true,
            stateDigest: hash(intent.state),
          }),
        );
        return true;
      }
      const prepared = load<Preparation>('prepare', cookieToken);
      need(
        prepared.csrf === csrf &&
          prepared.bindingDigest === bindingDigest &&
          JSON.stringify(prepared.continuation) === JSON.stringify(continuation),
        'embed_csrf_invalid',
        403,
      );
      remove('prepare', cookieToken);
      const intent = await protocol.start();
      await fresh();
      const token = save('intent', {
        ...intent,
        ...(continuation ? { continuation } : {}),
        bindingDigest,
        prepareCsrfHash: hash(csrf),
        loginStartedAt: Date.now(),
        expiresAt: Date.now() + 300000,
      });
      setCookie(res, embedded.origin, 'planner_soty_intent', token);
      res.writeHead(200, { 'content-type': 'application/json', 'cache-control': 'no-store' });
      res.end(
        JSON.stringify({
          schema: 'planner.embed-login-authorization.v1',
          authorizationUrl: intent.location,
        }),
      );
      return true;
    }
    if (isEmbed(req) && url.pathname === '/api/embed/login' && req.method === 'GET') {
      const intent = await protocol.start();
      const continuation = configuration.bridge?.continuation(req);
      const token = save('intent', {
        ...intent,
        loginStartedAt: Date.now(),
        expiresAt: Date.now() + 300000,
        ...(continuation ? { continuation } : {}),
        bindingDigest,
      });
      setCookie(res, embedded.origin, 'planner_soty_intent', token);
      res.writeHead(302, { Location: intent.location, 'Cache-Control': 'no-store' });
      res.end();
      return true;
    }
    if (isEmbed(req) && url.pathname === '/api/embed/callback' && req.method === 'GET') {
      const token = cookie(req, 'planner_soty_intent'),
        intent = load<Intent>('intent', token);
      remove('intent', token);
      let proof: Proof;
      try {
        proof = {
          ...(await protocol.exchange(new URL(req.url!, embedded.origin), intent)),
          ...(intent.continuation ? { continuation: intent.continuation } : {}),
          bindingDigest: intent.bindingDigest,
          loginStartedAt: intent.loginStartedAt,
        };
      } catch {
        throw new ApiError(403, 'Не удалось подтвердить вход', 'embed_proof_invalid');
      }
      await live(req, proof);
      const row = store.db
        .prepare(
          'SELECT user_id FROM planner_soty_links WHERE issuer=? AND subject=? AND workspace_id=?',
        )
        .get(proof.issuer, proof.subject, configuration.workspaceId) as
        { user_id: string } | undefined;
      if (row && hasGrant(proof)) {
        const user = store.user(row.user_id);
        need(user, 'embed_account_unavailable', 401);
        store.requireRole(user, configuration.workspaceId);
        await sessionFor(req, res, proof, user.id);
        return true;
      }
      const consent = save('consent', { ...proof, workspaceId: configuration.workspaceId });
      setCookie(res, embedded.origin, 'planner_soty_link', consent);
      const location = native.origin + '/soty/connect?intent=' + consent;
      framePolicy(res);
      // End the issuer's form redirect on its approved callback. Source-owned
      // navigation then reaches the separately approved native consent host.
      html(
        res,
        'Разрешение пространства',
        `<p>Открываем выбранное пространство для вашего подтверждения.</p><a href="${escape(location)}">Продолжить в Планировщике</a>`,
        `location.replace(${JSON.stringify(location)});`,
      );
      return true;
    }
    if (isEmbed(req) && url.pathname === '/api/embed/complete-link' && req.method === 'GET') {
      const token = cookie(req, 'planner_soty_link'),
        proof = load<Consent>('consent', token);
      if (configuration.bridge)
        need(
          url.searchParams.get('intent') === token && [...url.searchParams.keys()].length === 1,
          'embed_completion_invalid',
          403,
        );
      await live(req, proof);
      const row = store.db
        .prepare(
          'SELECT user_id FROM planner_soty_links WHERE issuer=? AND subject=? AND workspace_id=?',
        )
        .get(proof.issuer, proof.subject, configuration.workspaceId) as
        { user_id: string } | undefined;
      need(
        row && proof.workspaceId === configuration.workspaceId && hasGrant(proof),
        'embed_link_required',
        403,
      );
      const user = store.user(row.user_id);
      need(user, 'embed_account_unavailable', 401);
      store.requireRole(user, configuration.workspaceId);
      remove('consent', token);
      await sessionFor(req, res, proof, user.id);
      return true;
    }
    if (isNative(req) && url.pathname === '/soty/connect' && req.method === 'GET') {
      const token = url.searchParams.get('intent') ?? '',
        consent = load<Consent>('consent', token);
      await live(req, consent);
      const user = nativeUser();
      store.requireRole(user, configuration.workspaceId, ['owner']);
      const workspace = store.read().workspaces.find((w) => w.id === configuration.workspaceId);
      need(workspace, 'embed_workspace_unavailable', 403);
      html(
        res,
        'Подключить пространство',
        `<p>Вы разрешаете профилю Сот · ${escape(consent.subject.slice(-8))}, которым только что вошли, читать, создавать, изменять и удалять объекты «${escape(workspace.name)}» внутри ${escape(parent.origin)} с вашими текущими правами.</p>` +
          `<form method="post" action="/soty/connect"><input type="hidden" name="intent" value="${escape(token)}"><button type="submit">Разрешить только это пространство</button></form><small>Соты не получают права на остальные локальные пространства. Отозвать связь можно в этом Планировщике.</small>`,
        '',
        embedded.origin,
        'origin',
      );
      return true;
    }
    if (isNative(req) && url.pathname === '/soty/connect' && req.method === 'POST') {
      need(
        req.headers.origin === native.origin &&
          (!req.headers['sec-fetch-site'] ||
            ['same-origin', 'none'].includes(String(req.headers['sec-fetch-site']))),
        'csrf',
        403,
      );
      const token = await form(req),
        consent = load<Consent>('consent', token);
      await live(req, consent);
      const user = nativeUser();
      store.transaction(() => {
        store.requireRole(user, configuration.workspaceId, ['owner']);
        const previous = store.db
          .prepare(
            'SELECT user_id FROM planner_soty_links WHERE issuer=? AND subject=? AND workspace_id=?',
          )
          .get(consent.issuer, consent.subject, configuration.workspaceId) as
          { user_id: string } | undefined;
        need(!previous || previous.user_id === user.id, 'embed_link_conflict', 409);
        store.db
          .prepare(
            'INSERT OR IGNORE INTO planner_soty_links(issuer,subject,workspace_id,user_id,created_at) VALUES(?,?,?,?,?)',
          )
          .run(consent.issuer, consent.subject, configuration.workspaceId, user.id, Date.now());
        store.db
          .prepare(
            'INSERT OR IGNORE INTO planner_soty_grants(issuer,subject,workspace_id,binding_digest,created_at) VALUES(?,?,?,?,?)',
          )
          .run(
            consent.issuer,
            consent.subject,
            configuration.workspaceId,
            bindingDigest,
            Date.now(),
          );
        return false;
      });
      // Complete the same encrypted OIDC proof with its embed-host cookie.
      // Native cookies never cross; no second login or broader association.
      res.writeHead(302, {
        Location:
          embedded.origin +
          '/api/embed/complete-link' +
          (configuration.bridge ? '?intent=' + encodeURIComponent(token) : ''),
        'Cache-Control': 'no-store',
      });
      res.end();
      return true;
    }
    if (isNative(req) && url.pathname === '/soty/access' && req.method === 'GET') {
      const user = nativeUser();
      store.requireRole(user, configuration.workspaceId, ['owner']);
      const count = (
        store.db
          .prepare(
            'SELECT count(*) AS n FROM (SELECT DISTINCT links.issuer,links.subject FROM planner_soty_links links JOIN planner_soty_grants grants ON grants.issuer=links.issuer AND grants.subject=links.subject AND grants.workspace_id=links.workspace_id WHERE links.workspace_id=? AND links.user_id=?)',
          )
          .get(configuration.workspaceId, user.id) as { n: number }
      ).n;
      html(
        res,
        'Доступ Сот',
        `<p>К выбранному пространству подключено профилей: ${count}. Обычный вход Планировщика продолжает работать.</p>` +
          '<form method="post" action="/soty/disconnect"><button type="submit">Отключить все профили Сот от этого пространства</button></form>',
        '',
        '',
        'origin',
      );
      return true;
    }
    if (isNative(req) && url.pathname === '/soty/disconnect' && req.method === 'POST') {
      need(
        req.headers.origin === native.origin &&
          (!req.headers['sec-fetch-site'] ||
            ['same-origin', 'none'].includes(String(req.headers['sec-fetch-site']))),
        'csrf',
        403,
      );
      const user = nativeUser();
      store.transaction(() => {
        store.requireRole(user, configuration.workspaceId, ['owner']);
        store.db
          .prepare(
            'DELETE FROM planner_soty_grants WHERE workspace_id=? AND EXISTS(SELECT 1 FROM planner_soty_links links WHERE links.issuer=planner_soty_grants.issuer AND links.subject=planner_soty_grants.subject AND links.workspace_id=planner_soty_grants.workspace_id AND links.user_id=?)',
          )
          .run(configuration.workspaceId, user.id);
        // Revoking selected-resource consent must not silently rebind the same
        // immutable Human identity to another Source account on a later login.
        return false;
      });
      res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
      res.end('{"disconnected":true}');
      return true;
    }
    return false;
  }
  return Object.freeze({
    isEmbed,
    isNative,
    authorize,
    handle,
    framePolicy,
    loginPage,
    workspaceId: configuration.workspaceId,
  });
}

/** A narrow source UI surface, not unrestricted native API forwarding. */
export function embedApiAllowed(method: string, path: string) {
  if (
    method === 'GET' &&
    ['/api/state', '/api/search', '/api/history', '/api/audit', '/api/auth/me'].includes(path)
  )
    return true;
  if (/^\/api\/entities(?:\/[^/]+)?$/.test(path) && ['POST', 'PATCH', 'DELETE'].includes(method))
    return true;
  if (/^\/api\/comments(?:\/[^/]+)?$/.test(path) && ['POST', 'DELETE'].includes(method))
    return true;
  if (
    /^\/api\/dependencies(?:\/[^/]+)?$/.test(path) &&
    ['POST', 'PATCH', 'DELETE'].includes(method)
  )
    return true;
  if (/^\/api\/files(?:\/[^/]+)?$/.test(path) && ['GET', 'HEAD', 'POST', 'DELETE'].includes(method))
    return true;
  if (
    /^\/api\/scenarios(?:\/[^/]+(?:\/(?:submit|approve|reject))?)?$/.test(path) &&
    method === 'POST'
  )
    return true;
  if (/^\/api\/templates(?:\/[^/]+\/apply)?$/.test(path) && method === 'POST') return true;
  if (/^\/api\/assistant(?:\/apply)?$/.test(path) && method === 'POST') return true;
  if (/^\/api\/signals\/[^/]+\/(?:ack|snooze|resolve|accept-risk)$/.test(path) && method === 'POST')
    return true;
  if (/^\/api\/notifications\/[^/]+\/delivered$/.test(path) && method === 'POST') return true;
  return false;
}
