/** Framework-neutral Node HTTP adapter. Attach before a product's SPA fallback. */
export function createConnectHandler(service, { path = '/api/connect/rpc', maxBytes = 2 * 1024 * 1024 + 32 * 1024 } = {}) {
  return async function connectHttp(req, res, next) {
    const reply = (status, value) => {
      if (res.destroyed || res.writableEnded) return;
      res.setHeader('Cache-Control', 'no-store');
      res.setHeader('Content-Type', 'application/json; charset=utf-8');
      res.setHeader('X-Content-Type-Options', 'nosniff');
      res.statusCode = status; res.end(JSON.stringify(value));
    };
    const error = code => ({ ok: false, error: { code, message: code } });
    let pathname;
    try { pathname = new URL(req.url || '/', 'http://localhost').pathname; }
    catch { reply(400, error('invalid_url')); return true; }
    if (pathname !== path) { if (next) next(); return false; }
    if (req.method !== 'POST') { reply(405, error('method_not_allowed')); return true; }
    if (String(req.headers['content-type'] || '').split(';', 1)[0].trim().toLowerCase() !== 'application/json') { reply(415, error('json_required')); return true; }
    const origin = req.headers.origin;
    if (typeof origin !== 'string' || origin === 'null') { reply(403, error('origin_required')); return true; }
    try {
      const tooLarge = async(reader=req.iterator({destroyOnReturn:false})) => {
        // The peer may request Connection: close. Finish draining before
        // ending the rejection so unread TCP bytes cannot reset its JSON.
        // The body is discarded, never buffered or passed to the service.
        let timer,drained=false;
        try{
          // Keep consuming the same iterator. resume() cannot drain a stream
          // while its async iterator still owns the readable listener.
          const drain=(async()=>{while(!(await reader.next()).done){}return true;})();
          drain.catch(()=>{});
          drained=await Promise.race([drain,new Promise(resolve=>{timer=setTimeout(()=>resolve(false),5000);timer.unref?.();})]);
        }catch{/* An aborted peer cannot receive a response. */}
        finally{clearTimeout(timer);}
        if(!drained&&!req.destroyed){res.setHeader('Connection','close');res.once('finish',()=>req.destroy());}
        reply(413, error('request_too_large'));
      };
      if (Number(req.headers['content-length']) > maxBytes) { await tooLarge(); return true; }
      const chunks = [],reader=req.iterator({destroyOnReturn:false}); let length = 0;
      for await (const chunk of reader) {
        length += chunk.length;
        if (length > maxBytes) { await tooLarge(reader); return true; }
        chunks.push(chunk);
      }
      const body = JSON.parse(Buffer.concat(chunks).toString('utf8'));
      if (!body || typeof body !== 'object' || Array.isArray(body) || body.protocol !== 1) {
        reply(400, error('protocol_unsupported')); return true;
      }
      // req.ip is Express's explicitly configured trusted-proxy result; the neutral
      // adapter uses the socket peer. Never accept a peer identifier from RPC JSON.
      const connection = new AbortController(), disconnected = () => { if (!res.writableEnded) connection.abort(); };
      res.once('close', disconnected);
      let result;
      try { result = await service.handle({ op: body.op, args: body.args || {}, proof: body.proof, origin, peer: req.ip || req.socket?.remoteAddress }, { signal: connection.signal }); }
      finally { res.removeListener('close', disconnected); }
      // A bounded authority/storage fence may be busy without accepting the
      // operation. Preserve the typed RPC error and expose temporary service
      // unavailability, rather than classifying contention as malformed input.
      // The adapter never retries a signed mutation on the caller's behalf.
      const busy = !result.ok && ['apps_saved_busy', 'apps_discussion_busy', 'apps_entry_busy', 'world_authority_busy',
        'apps_authority_busy', 'registration_busy', 'registration_storage_full', 'feedback_busy', 'feedback_closed', 'feedback_storage_capacity',
        'human_identity_storage_full', 'human_identity_storage_busy', 'human_identity_closed',
        'feedback_ticket_capacity', 'feedback_receipt_capacity', 'feedback_message_capacity', 'feedback_conversation_capacity'].includes(result.error?.code);
      const rateLimited = !result.ok && ['apps_discussion_rate_limited', 'human_identity_capacity'].includes(result.error?.code);
      const conflict = !result.ok && ['registration_intent_conflict', 'registration_revision_conflict', 'registration_authority_changed',
        'feedback_request_conflict', 'feedback_revision_conflict', 'human_identity_intent_conflict',
        'human_identity_decision_conflict'].includes(result.error?.code);
      const denied = !result.ok && ['registration_account_mismatch', 'registration_app_not_owned', 'feedback_installation_mismatch',
        'feedback_scope_mismatch', 'feedback_support_required', 'feedback_acceptance_required', 'human_identity_account_mismatch',
        'human_identity_actor_revoked', 'human_identity_browser_mismatch', 'human_identity_client_mismatch',
        'human_identity_profile_changed'].includes(result.error?.code);
      const missing = !result.ok && ['registration_not_found', 'feedback_ticket_unavailable'].includes(result.error?.code);
      const expired = !result.ok && result.error?.code === 'human_identity_interaction_expired';
      reply(result.ok ? 200 : busy ? 503 : rateLimited ? 429 : conflict ? 409 : denied ? 403 : expired ? 410 : missing ? 404 : 400, result);
    } catch { reply(400, error('request_failed')); }
    return true;
  };
}
