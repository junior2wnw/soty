import { createSourceNativeAuthorityPort, isSourceNativeCommitPort } from '../../server/native-authority.mjs';
import { check, fields, digest, nonce, jsonCopy } from '../../server/wire.mjs';
import { SOURCE_FEEDBACK_LIMITS } from '../../shared/feedback-wire.mjs';
import { validateFeedbackAttachments } from '../../../feedback/server/media.mjs';
import { createOrdinaryFeedbackJobs,requireOrdinaryFeedbackJobs } from './feedback-jobs.mjs';

/** Actual example Native implementation. Every role/resource decision and
 * durable receipt belongs to this Source SQL database, never Root metadata. */
export function createOrdinaryAppNativePort({ store, resourceId, incarnationId, allowEmptyGuest = false, allowLinkedLogin = false, beforeCommit, afterCommit, feedbackProcessing, feedbackJobs } = {}) {
  const { db } = store, proofs = new WeakMap();
  check(!(feedbackJobs&&feedbackProcessing),'ordinary_feedback_jobs_not_ready',503);
  check(typeof allowLinkedLogin==='boolean','ordinary_native_configuration_invalid',503);
  const jobs=feedbackJobs?requireOrdinaryFeedbackJobs(feedbackJobs,store,resourceId,incarnationId):feedbackProcessing?createOrdinaryFeedbackJobs({store,resourceId,incarnationId,...feedbackProcessing}):null;
  const nativeCookie = 'ordinary_native_' + store.realmId;
  function cookie(req) {
    const values = String(req?.headers?.cookie ?? '').split(';').map(value => value.trim()).filter(value => value.startsWith(nativeCookie + '='));
    check(values.length <= 1, 'ordinary_native_session_denied', 403); return values[0]?.slice(nativeCookie.length + 1);
  }
  function resource(binding) {
    const row = db.prepare('SELECT * FROM native_resources WHERE id=?').get(resourceId);
    check(row && row.realm_id === store.realmId && row.incarnation_id === incarnationId
      && binding.resource.selection.nativeId === resourceId && binding.resource.selection.incarnationId === incarnationId, 'ordinary_native_resource_denied', 403); return row;
  }
  function inspect(proof, binding) {
    const captured = proofs.get(proof); check(captured && captured.bindingDigest === digest(binding), 'ordinary_native_proof_denied', 403); resource(binding);
    if (captured.guest) {
      check(binding.operation === 'link' && allowEmptyGuest && db.prepare('SELECT guest_empty FROM native_resources WHERE id=?').get(resourceId).guest_empty === 1
        && db.prepare('SELECT count(*) AS n FROM native_memberships WHERE resource_id=?').get(resourceId).n === 0
        && db.prepare('SELECT count(*) AS n FROM native_items WHERE resource_id=?').get(resourceId).n === 0, 'ordinary_guest_existing_resource_denied', 403);
      return captured;
    }
    const session = db.prepare('SELECT * FROM native_sessions WHERE id_hash=?').get(captured.nativeSessionHash);
    const membership = db.prepare('SELECT * FROM native_memberships WHERE resource_id=? AND principal_id=?').get(resourceId, captured.principalId);
    check(session?.active === 1 && session.expires_at > store.clock() && session.principal_id === captured.principalId && session.generation === captured.sessionGeneration
      && membership?.active === 1 && membership.revision === captured.membershipRevision, 'ordinary_native_access_denied', 403);
    if(captured.linkedLogin){
      // A Source-owned current login grant, never a private-data proof. The
      // BFF must independently exchange fresh OIDC before commitIdentity.
      check(allowLinkedLogin&&binding.operation==='link','ordinary_native_proof_denied',403);
      const link=db.prepare('SELECT principal_id FROM native_links WHERE issuer=? AND subject=?').get(binding.identity.issuer,binding.identity.subject);
      const consent=db.prepare('SELECT native_session_hash FROM native_consents WHERE principal_id=? AND resource_id=? AND semantic_digest=? AND root_device_id=?')
        .get(captured.principalId,resourceId,binding.semanticDigest,binding.rootPrincipal.deviceId);
      check(link?.principal_id===captured.principalId&&consent?.native_session_hash===captured.nativeSessionHash,'ordinary_native_login_grant_denied',403);
    }
    if (captured.sourceSessionHash) {
      const sourceSession = db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(captured.sourceSessionHash);
      const link = db.prepare('SELECT * FROM native_links WHERE issuer=? AND subject=?').get(binding.identity.issuer, binding.identity.subject);
      const consent = db.prepare('SELECT * FROM native_consents WHERE principal_id=? AND resource_id=? AND semantic_digest=? AND root_device_id=?')
        .get(captured.principalId, resourceId, binding.semanticDigest, binding.rootPrincipal.deviceId);
      check(sourceSession?.active === 1 && sourceSession.expires_at > store.clock() && sourceSession.principal_id === captured.principalId
        && sourceSession.native_session_hash === captured.nativeSessionHash && sourceSession.resource_id === resourceId
        && link?.principal_id === captured.principalId && consent?.native_session_hash === captured.nativeSessionHash, 'ordinary_native_access_denied', 403);
    }
    return { ...captured, role: membership.role };
  }
  function dataActor(proof,binding){const actor=inspect(proof,binding);check(!actor.linkedLogin&&!actor.guest,'ordinary_native_login_only',403);return actor;}
  function link(proof, binding, identity, guest) {
    check(store.inTransaction(), 'ordinary_native_transaction_required', 503); let actor = inspect(proof, binding);
    check(identity.issuer === binding.identity.issuer && identity.subject === binding.identity.subject, 'ordinary_native_link_denied', 403);
    // An app's new-empty policy stays enabled after first login. A returning
    // current linked candidate authenticates the SAME principal; it cannot
    // run the empty-resource creator a second time.
    if(guest&&actor.linkedLogin){check(allowLinkedLogin,'ordinary_native_login_grant_denied',403);guest=false;}
    if (guest) {
      check(actor.guest === true, 'ordinary_guest_existing_resource_denied', 403);
      const principalId = 'native-' + nonce().slice(0, 20), nativeSessionHash = digest(nonce());
      db.prepare('INSERT INTO native_principals VALUES(?,?)').run(principalId, store.realmId);
      db.prepare('INSERT INTO native_sessions VALUES(?,?,1,1,?)').run(nativeSessionHash, principalId, store.clock() + 86400000);
      db.prepare("INSERT INTO native_memberships VALUES(?,?,'owner',1,1)").run(resourceId, principalId);
      db.prepare('UPDATE native_resources SET guest_empty=0 WHERE id=?').run(resourceId); actor = { principalId, nativeSessionHash };
      proofs.set(proof, { ...actor, sessionGeneration: 1, membershipRevision: 1, bindingDigest: digest(binding) });
    }
    const prior = db.prepare('SELECT * FROM native_links WHERE issuer=? AND subject=?').get(identity.issuer, identity.subject);
    check(!prior || prior.principal_id === actor.principalId, 'ordinary_native_link_conflict', 409);
    if (!prior) db.prepare('INSERT INTO native_links VALUES(?,?,?)').run(identity.issuer, identity.subject, actor.principalId);
    const consent = db.prepare('SELECT * FROM native_consents WHERE principal_id=? AND resource_id=? AND semantic_digest=? AND root_device_id=?')
      .get(actor.principalId, resourceId, binding.semanticDigest, binding.rootPrincipal.deviceId);
    check(!consent || consent.native_session_hash === actor.nativeSessionHash, 'ordinary_native_consent_conflict', 409);
    if (!consent) db.prepare('INSERT INTO native_consents VALUES(?,?,?,?,?)').run(actor.principalId, resourceId, binding.semanticDigest, binding.rootPrincipal.deviceId, actor.nativeSessionHash);
    return { principalId: actor.principalId, nativeSessionHash: actor.nativeSessionHash, resourceId };
  }
  function receipt(actor, args, intent, action) {
    check(store.inTransaction(), 'ordinary_native_transaction_required', 503);
    const intentDigest = digest(intent), prior = db.prepare('SELECT * FROM native_receipts WHERE resource_id=? AND principal_id=? AND request_id=?').get(resourceId, actor.principalId, args.requestId);
    check(!prior || prior.input_digest === intentDigest, 'ordinary_native_request_conflict', 409);
    if (prior) return { ...JSON.parse(prior.result_json), replayed: true };
    check(db.prepare('SELECT count(*) AS n FROM native_receipts WHERE resource_id=? AND principal_id=?').get(resourceId, actor.principalId).n < 20000, 'ordinary_native_capacity', 503);
    const result = action(intentDigest); db.prepare('INSERT INTO native_receipts VALUES(?,?,?,?,?)').run(resourceId, actor.principalId, args.requestId, intentDigest, JSON.stringify(result)); return result;
  }
  function ticket(actor, id, includeBytes = false) {
    const row = db.prepare('SELECT * FROM native_tickets WHERE id=? AND resource_id=?').get(id, resourceId);
    check(row && (row.reporter_id === actor.principalId || actor.role === 'owner'), 'ordinary_native_ticket_denied', 403);
    const media = db.prepare('SELECT * FROM native_ticket_media WHERE ticket_id=? ORDER BY ordinal').all(id).map(item => ({ ...JSON.parse(item.metadata_json),
      ...(includeBytes ? { dataBase64: Buffer.from(item.bytes).toString('base64') } : {}) }));
    const messages = db.prepare('SELECT * FROM native_ticket_messages WHERE ticket_id=? ORDER BY created_at,id').all(id).map(item => ({ id: item.id, kind: item.kind, body: item.body, createdAt: item.created_at }));
    return { id, revision: row.revision, status: row.status, body: row.body, createdAt: row.created_at, updatedAt: row.updated_at,
      canReply: true, canManage: actor.role === 'owner', canAccept: row.reporter_id === actor.principalId && row.status === 'ready_to_check', attachments: media, messages };
  }
  async function feedbackMutation(operation, proof, binding, args, final) {
    check(isSourceNativeCommitPort(final), 'ordinary_native_commit_required', 503); await beforeCommit?.(operation);
    return store.tx(() => {
      const actor = dataActor(proof, binding);
      const priorTicket = operation === 'submit' ? null : ticket(actor, args.ticketId);
      if (operation === 'status' || operation === 'reply' && priorTicket.canManage) check(actor.role === 'owner', 'ordinary_native_support_denied', 403);
      if (operation === 'accept') check(db.prepare('SELECT reporter_id FROM native_tickets WHERE id=?').get(args.ticketId).reporter_id === actor.principalId, 'ordinary_native_reporter_denied', 403);
      return final.commit(() => receipt(actor, args, { operation: 'feedback.' + operation, ...args }, () => {
        const now = store.clock(); let id = args.ticketId;
        if (operation === 'submit') {
          id = 'ticket-' + nonce().slice(0, 20); db.prepare("INSERT INTO native_tickets VALUES(?,?,?,?,'received',1,?,?)").run(id, resourceId, actor.principalId, args.body, now, now);
          const media = validateFeedbackAttachments(args.attachments);
          for (let ordinal = 0; ordinal < media.length; ordinal++) { const { bytes, ...metadata } = media[ordinal]; db.prepare('INSERT INTO native_ticket_media VALUES(?,?,?,?)').run(id, ordinal, JSON.stringify(metadata), bytes); }
        } else {
          check(priorTicket.revision === args.expectedRevision, 'feedback_revision_conflict', 409);
          if (operation === 'accept') check(priorTicket.canAccept, 'ordinary_native_reporter_denied', 403);
          if (operation === 'reply') {
            check(db.prepare('SELECT count(*) AS n FROM native_ticket_messages WHERE ticket_id=?').get(id).n < 128, 'ordinary_native_capacity', 503);
            db.prepare('INSERT INTO native_ticket_messages VALUES(?,?,?,?,?)').run('message-' + nonce().slice(0, 20), id, actor.role === 'owner' ? 'support' : 'reporter', args.body, now);
          }
          const nextStatus = operation === 'accept' ? 'resolved' : operation === 'status' ? args.status
            : actor.role === 'owner' && priorTicket.status === 'received' ? 'in_progress' : priorTicket.status;
          db.prepare('UPDATE native_tickets SET status=?,revision=revision+1,updated_at=? WHERE id=?').run(nextStatus, now, id);
        }
        const value = ticket(actor, id); return { requestId: args.requestId, replayed: false, receipt: { ticketId: id, revision: value.revision, createdAt: now }, ticket: value };
      }));
    });
  }
  return createSourceNativeAuthorityPort({
    async capture(binding, req) {
      resource(binding); let sourceSession, nativeSessionHash,linkedLogin=false;
      if (req) {
        const token = cookie(req); nativeSessionHash = token ? digest(token) : null;
        if(!token&&allowLinkedLogin&&binding.operation==='link'){
          const link=db.prepare('SELECT principal_id FROM native_links WHERE issuer=? AND subject=?').get(binding.identity.issuer,binding.identity.subject);
          const consent=link?db.prepare('SELECT native_session_hash FROM native_consents WHERE principal_id=? AND resource_id=? AND semantic_digest=? AND root_device_id=?')
            .get(link.principal_id,resourceId,binding.semanticDigest,binding.rootPrincipal.deviceId):null;
          if(consent){nativeSessionHash=consent.native_session_hash;linkedLogin=true;}
        }
      }
      else { sourceSession = db.prepare('SELECT * FROM source_sessions WHERE id_hash=?').get(binding.sessionIdHash); nativeSessionHash = sourceSession?.native_session_hash; }
      const session = nativeSessionHash ? db.prepare('SELECT * FROM native_sessions WHERE id_hash=?').get(nativeSessionHash) : null;
      const membership = session ? db.prepare('SELECT * FROM native_memberships WHERE resource_id=? AND principal_id=?').get(resourceId, session.principal_id) : null;
      const proof = Object.freeze({});
      if (!session && req && allowEmptyGuest&&!linkedLogin) proofs.set(proof, { guest: true, bindingDigest: digest(binding) });
      else {
        check(session && membership, 'ordinary_native_session_denied', 403);
        proofs.set(proof, { principalId: session.principal_id, nativeSessionHash, sessionGeneration: session.generation, membershipRevision: membership.revision,
          bindingDigest: digest(binding), ...(sourceSession ? { sourceSessionHash: sourceSession.id_hash } : {}) });
        if(linkedLogin)proofs.set(proof,{...proofs.get(proof),linkedLogin:true});
      }
      inspect(proof, binding); return proof;
    },
    withCurrent(proof, binding, apply) { inspect(proof, binding); return apply(); },
    rememberLogin(proof, binding, intent) {
      check(store.format >= 2 && store.inTransaction(), 'ordinary_native_login_proof_not_ready', 503);
      inspect(proof, binding); const captured = proofs.get(proof), idHash = digest(nonce()), bindingDigest = digest(binding);
      check(intent.expiresAt > store.clock(), 'ordinary_native_login_proof_denied', 403);
      const cipher = store.encrypt('NativeLoginProof', idHash, 0, captured);
      db.prepare('INSERT INTO native_login_proofs VALUES(?,?,?,?,?,?)').run(idHash, intent.interactionIdHash, bindingDigest, intent.expiresAt, cipher,
        db.prepare('SELECT key_id FROM source_interactions WHERE id_hash=?').get(intent.interactionIdHash).key_id);
      return { idHash, version: 1, bindingDigest };
    },
    async recoverLogin(binding, marker, intent) {
      check(store.format >= 2, 'ordinary_native_login_proof_not_ready', 503);
      const row = db.prepare('SELECT * FROM native_login_proofs WHERE id_hash=?').get(marker.idHash);
      check(row && row.binding_digest === digest(binding) && row.binding_digest === marker.bindingDigest
        && row.interaction_hash === intent.interactionIdHash && row.expires_at === intent.expiresAt && row.expires_at > store.clock(), 'ordinary_native_login_proof_denied', 403);
      const captured = store.decrypt('NativeLoginProof', row.id_hash, 0, row.cipher, row.key_id);
      const proof = Object.freeze({}); proofs.set(proof, captured); inspect(proof, binding); return proof;
    },
    linkVerifiedIdentity: (proof, binding, identity) => link(proof, binding, identity, false),
    ...(allowEmptyGuest ? { createEmptyGuest: (proof, binding, identity) => link(proof, binding, identity, true) } : {}),
    async read(proof, binding, args) {
      if(jobs&&['feedback.job.status','feedback.job.result','feedback.processing.context','feedback.processing.ticket'].includes(args.input?.operation)){const result=jobs.read(dataActor(proof,binding),args.input);dataActor(proof,binding);return result;}
      dataActor(proof, binding); const input = fields(args.input, ['operation']); check(input.operation === 'items.list', 'ordinary_native_operation_denied', 403);
      const rows = db.prepare('SELECT id,title,revision,created_at AS createdAt FROM native_items WHERE resource_id=? ORDER BY created_at,id LIMIT 100').all(resourceId);
      dataActor(proof, binding); return { items: rows.map(row => ({ ...row })), requestId: args.requestId };
    },
    async execute(proof, binding, args, final) {
      if(jobs&&['feedback.processing.consent','feedback.job.grant','feedback.job.revoke'].includes(args.input?.operation)){
        check(isSourceNativeCommitPort(final));await beforeCommit?.(args.input.operation);
        const result=store.tx(()=>{const actor=dataActor(proof,binding);
          const prior=db.prepare('SELECT input_digest FROM native_receipts WHERE resource_id=? AND principal_id=? AND request_id=?').get(resourceId,actor.principalId,args.requestId);
          if(!prior)jobs.validate(actor,args.input,args.requestId);
          return final.commit(()=>receipt(actor,args,args.input,inputDigest=>({
          requestId:args.requestId,inputDigest,outcome:'committed',replayed:false,data:jobs.execute(actor,args.input,args.requestId)})));});
        await afterCommit?.(args.input.operation);return result;
      }
      const input = fields(args.input, ['operation', 'title']); check(input.operation === 'items.create' && typeof input.title === 'string' && input.title.trim().length > 0 && input.title.length <= 500);
      check(isSourceNativeCommitPort(final)); await beforeCommit?.('items.create');
      const result = store.tx(() => { const actor = dataActor(proof, binding); return final.commit(() => receipt(actor, args, args.input, inputDigest => {
        const id = 'item-' + nonce().slice(0, 20), now = store.clock(); db.prepare('INSERT INTO native_items VALUES(?,?,?,?,?)').run(id, resourceId, input.title, 1, now);
        return { requestId: args.requestId, inputDigest, outcome: 'committed', replayed: false, receipt: { id, revision: 1, createdAt: now } };
      })); });
      await afterCommit?.('items.create'); return result;
    },
    async readProof(proof, binding, args) {
      const actor = dataActor(proof, binding), input = fields(args.input, ['inputDigest']); check(/^[a-f0-9]{64}$/u.test(input.inputDigest));
      const row = db.prepare('SELECT * FROM native_receipts WHERE resource_id=? AND principal_id=? AND request_id=?').get(resourceId, actor.principalId, args.requestId);
      check(!row || row.input_digest === input.inputDigest, 'ordinary_native_request_conflict', 409); dataActor(proof, binding);
      return row ? JSON.parse(row.result_json) : { requestId: args.requestId, inputDigest: input.inputDigest, outcome: 'not_applied' };
    },
    feedback: {
      async context(proof, binding) { const actor = dataActor(proof, binding); return { schema: 'soty.source-feedback.context.v1', ready: true, canSubmit: true,
        bindingDigest: digest({ realm: store.realmId, resourceId, incarnationId, principal: actor.principalId }), recipientLabel: 'Владелец приложения',
        limits: SOURCE_FEEDBACK_LIMITS, capabilities: { text: true, voice: true, screenshot: true, asr: false } }; },
      async list(proof, binding, args) { const actor = dataActor(proof, binding); check(!args.cursor, 'ordinary_native_cursor_unavailable', 400);
        const rows = actor.role === 'owner' ? db.prepare('SELECT id FROM native_tickets WHERE resource_id=? ORDER BY created_at,id LIMIT ?').all(resourceId, args.limit ?? 20)
          : db.prepare('SELECT id FROM native_tickets WHERE resource_id=? AND reporter_id=? ORDER BY created_at,id LIMIT ?').all(resourceId, actor.principalId, args.limit ?? 20);
        return { tickets: rows.map(row => ticket(actor, row.id)), nextCursor: null }; },
      async get(proof, binding, args) { return { ticket: ticket(dataActor(proof, binding), args.ticketId, true) }; },
      submit: (...args) => feedbackMutation('submit', ...args), reply: (...args) => feedbackMutation('reply', ...args),
      status: (...args) => feedbackMutation('status', ...args), accept: (...args) => feedbackMutation('accept', ...args),
    },
  });
}
