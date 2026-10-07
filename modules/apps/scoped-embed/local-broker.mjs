import { createSourceProofSigner } from "./source-proof.mjs";
import { createResourceSourceProofSigner } from './resource-proof.mjs';
import { approvedEmbedProfile, embedRoute } from './profile-dispatch.mjs';
import { SELECTED_RESOURCE_PROFILE } from './resource-profile.mjs';
import { selectedNativeHandoff } from './resource-route-adapters.mjs';
import { request as nodeHttpRequest } from "node:http";
import { Readable } from "node:stream";
import {sourceContinuationAck} from './source-continuation.mjs';
import {
  capture,
  closed,
  continuation,
  hash,
  need,
  scopedRoute,
  SCOPED_EMBED_LIMITS,
} from "./profile.mjs";

export const PUBLIC_ASSET_LIMITS=Object.freeze({active:4,queued:32,waitMs:8000});
const requestHeaders = new Set([
  "accept",
  "accept-language",
  "content-type",
  "if-none-match",
  "range",
  "if-range",
]);
const cookieNames = new Set([
  "planner_soty_session",
  "planner_soty_intent",
  "planner_soty_link",
]);
function fixedLocalHttp(url, options) {
  const target = new URL(url);
  need(
    target.protocol === "http:" && target.hostname === "127.0.0.1",
    "scoped_embed_destination_invalid",
  );
  return new Promise((resolve, reject) => {
    const req = nodeHttpRequest(
      {
        hostname: "127.0.0.1",
        port: Number(target.port),
        path: target.pathname + target.search,
        method: options.method,
        headers: options.headers,
        agent: false,
      },
      (res) =>
        resolve({
          status: res.statusCode,
          redirected: false,
          headers: {
            get(name) {
              const value = res.headers[name.toLowerCase()];
              return Array.isArray(value) ? value.join(",") : (value ?? null);
            },
            getSetCookie() {
              return res.headers["set-cookie"] ?? [];
            },
          },
          body: Readable.toWeb(res),
        }),
    );
    req.on("error", () =>
      reject(
        Object.assign(new Error("scoped_embed_source_unconfirmed"), {
          code: "scoped_embed_source_unconfirmed",
        }),
      ),
    );
    const abort = () => req.destroy(new Error("scoped_embed_cancelled"));
    if (options.signal?.aborted) {
      abort();
      return;
    }
    options.signal?.addEventListener("abort", abort, { once: true });
    req.once("close", () =>
      options.signal?.removeEventListener("abort", abort),
    );
    req.end(options.body);
  });
}
function cookies(values, now, names = cookieNames, maxAge = 300) {
  const result = {};
  need(
    Array.isArray(values) && values.length <= names.size,
    "scoped_embed_cookie_invalid",
    502,
  );
  for (const raw of values ?? []) {
    need(
      typeof raw === "string" && raw.length <= 2048,
      "scoped_embed_cookie_invalid",
      502,
    );
    const [pair, ...attributes] = raw.split(";").map((v) => v.trim()),
      split = pair.indexOf("="),
      name = pair.slice(0, split),
      token = pair.slice(split + 1);
    need(
      names.has(name) && (!token || /^[A-Za-z0-9_-]{43}$/u.test(token)),
      "scoped_embed_cookie_invalid",
      502,
    );
    const attrs = Object.fromEntries(
      attributes.map((v) => {
        const at = v.indexOf("=");
        return at < 0
          ? [v.toLowerCase(), true]
          : [v.slice(0, at).toLowerCase(), v.slice(at + 1)];
      }),
    );
    need(
      attrs.httponly === true &&
        attrs.path === "/" &&
        attrs.samesite === "Lax" &&
        attrs.domain === undefined &&
        Object.keys(attrs).every((k) =>
          ["httponly", "path", "samesite", "secure", "max-age"].includes(k),
        ) &&
        /^\d+$/u.test(attrs["max-age"] ?? "") &&
        Number(attrs["max-age"]) <= maxAge,
      "scoped_embed_cookie_invalid",
      502,
    );
    need(!Object.hasOwn(result, name), "scoped_embed_cookie_invalid", 502);
    result[name] = { token, expiresAt: now + Number(attrs["max-age"]) * 1000 };
  }
  return result;
}

/** A single approved source, not a general network proxy. The production
 * channel must call dispatch only after its own device/binding/context checks.
 * Browser/descriptor/HTTP bodies never select the destination or authority. */
export function createLocalScopedEmbedBroker({
  profile: raw,
  localPort,
  key,
  readAuthority,
  assertBinding,
  fetch: fetcher = fixedLocalHttp,
  clock = Date.now,
} = {}) {
  const profile = approvedEmbedProfile(raw), resources = profile.schema === SELECTED_RESOURCE_PROFILE;
  const approvedCookies = resources ? new Set(['soty_rp_session', 'soty_rp_intent', 'soty_rp_link']) : cookieNames;
  need(
    Number.isSafeInteger(localPort) &&
      localPort >= 1024 &&
      localPort <= 65535 &&
      localPort !== 49424,
  );
  need(
    [readAuthority, assertBinding, fetcher].every(
      (value) => typeof value === "function",
    ),
  );
  const signer = resources ? createResourceSourceProofSigner({ profile, key, clock }) : createSourceProofSigner({ profile: raw, key, clock });
  const jars = new Map();
  const assetQueue=[],assetControllers=new Set();let assetActive=0;
  let active = 0,
    closedBroker = false;
  function assetError(code,status){try{need(false,code,status);}catch(error){return error;}}
  function assetSlot(){assetActive++;const controller=new AbortController();assetControllers.add(controller);let released=false;
    return{controller,release(){if(released)return;released=true;assetControllers.delete(controller);assetActive--;
      if(!closedBroker&&assetQueue.length){const next=assetQueue.shift();next.cleanup();next.resolve(assetSlot());}}};}
  function acquireAsset(signal){need(!closedBroker,'scoped_embed_closed',503);need(!signal?.aborted,'scoped_embed_cancelled',499);
    if(assetActive<PUBLIC_ASSET_LIMITS.active)return Promise.resolve(assetSlot());
    need(assetQueue.length<PUBLIC_ASSET_LIMITS.queued,'scoped_embed_assets_busy',503);
    return new Promise((resolve,reject)=>{let timer;const item={resolve,reject,cleanup(){clearTimeout(timer);signal?.removeEventListener('abort',cancel);}};
      const remove=()=>{const index=assetQueue.indexOf(item);if(index>=0)assetQueue.splice(index,1);item.cleanup();};
      const cancel=()=>{remove();reject(assetError('scoped_embed_cancelled',499));};
      timer=setTimeout(()=>{remove();reject(assetError('scoped_embed_assets_busy',503));},PUBLIC_ASSET_LIMITS.waitMs);timer.unref?.();
      signal?.addEventListener('abort',cancel,{once:true});assetQueue.push(item);
    });}
  function pruneJars() {
    const now = clock();
    for (const [id, jar] of jars) if (jar.expiresAt <= now) jars.delete(id);
  }
  async function current(context) {
    continuation(context.reference);
    need(!closedBroker, "scoped_embed_closed", 503);
    const now = await readAuthority({
      reference: context.reference,
      connector: profile.connector,
    });
    need(
      now.profileDigest === profile.digest &&
        hash(now.reference) === hash(context.reference) &&
        hash(now.rootPrincipal) === hash(context.rootPrincipal) &&
        hash(now.humanPrincipal) === hash(context.humanPrincipal) &&
        now.expiresAt > clock(),
      "scoped_embed_authority_changed",
      403,
    );
    return now;
  }
  function local(context) {
    const result = assertBinding(
      Object.freeze({
        appId: profile.appId,
        target: profile.target,
        sourceProfile: profile.sourceProfile,
        resource: profile.resource,
        connector: profile.connector,
        context,
      }),
    );
    need(result === true && !result?.then, "scoped_embed_binding_changed", 403);
  }
  function redirect(value, kind) {
    const url = new URL(value, profile.embedOrigin);
    if (url.origin === profile.embedOrigin) {
      embedRoute(profile, 'GET', url.pathname + url.search);
      return url.href;
    }
    if (kind === "auth-start") {
      if (resources && url.origin === profile.nativeOrigin) return selectedNativeHandoff(profile, url.href);
      need(
        url.origin === new URL(profile.issuer).origin &&
          url.pathname === "/human-identity/authorize",
        "scoped_embed_redirect_denied",
        502,
      );
      const params = url.searchParams;
      need(
        [...params.keys()].sort().join(',') === 'client_id,code_challenge,code_challenge_method,nonce,redirect_uri,response_type,scope,state' &&
          /^[A-Za-z0-9_-]{43}$/.test(params.get('state') ?? '') && /^[A-Za-z0-9_-]{43}$/.test(params.get('nonce') ?? '') &&
          /^[A-Za-z0-9_-]{43}$/.test(params.get('code_challenge') ?? '') && params.get('scope') === 'openid profile' &&
          !url.username && !url.password && !url.hash && params.get("client_id") === profile.clientId &&
          params.get("redirect_uri") ===
            profile.embedOrigin + "/api/embed/callback" &&
          params.get("response_type") === "code" &&
          params.get("code_challenge_method") === "S256",
        "scoped_embed_redirect_denied",
        502,
      );
      return url.href;
    }
    need(false, "scoped_embed_redirect_denied", 502);
  }
  return Object.freeze({
    profile: profile.schema,
    async probe(signal) {
      const {request,headers}=signer.probeHeaders();
      const response=await fetcher(`http://127.0.0.1:${localPort}/api/embed/transport-ready`,{
        method:'HEAD',headers:{...headers,host:new URL(profile.embedOrigin).host,connection:'close'},
        signal:signal?AbortSignal.any([signal,AbortSignal.timeout(SCOPED_EMBED_LIMITS.callMs)]):AbortSignal.timeout(SCOPED_EMBED_LIMITS.callMs),
      });
      signer.verifyReady(response,request);return {state:'responding',httpStatus:204};
    },
    async dispatch(
      { context, method, path, headers = {}, body = Buffer.alloc(0) },
      signal,
    ) {
      const captured = capture(context),
        capturedHeaders = capture(headers),
        requestBody = Buffer.from(body),
        route = embedRoute(profile, method, path), kind = route.kind;
      closed(capturedHeaders, [], [...requestHeaders, "origin"]);
      need(
        requestBody.length <= route.requestBytes,
        "scoped_embed_request_limit",
        413,
      );
      need(
        !capturedHeaders.origin ||
          capturedHeaders.origin === profile.embedOrigin,
        "scoped_embed_origin_invalid",
        403,
      );
      if (!["GET", "HEAD"].includes(method))
        need(
          capturedHeaders.origin === profile.embedOrigin,
          "scoped_embed_csrf",
          403,
        );
      const selectedHeaders = {};
      for (const [name, value] of Object.entries(capturedHeaders)) {
        need(
          typeof value === "string" &&
            value.length <= 2048 &&
            !/[\r\n\0]/u.test(value),
        );
        selectedHeaders[name] = value;
      }
      let assetLease;
      if(route.credentialFree){await current(captured);local(captured);
        try{assetLease=await acquireAsset(signal);}catch(error){if(error.code!=='scoped_embed_assets_busy')throw error;
          await current(captured);local(captured);need(!signal?.aborted,'scoped_embed_cancelled',499);
          return Object.freeze({status:503,headers:Object.freeze({'content-type':'application/json','cache-control':'no-store','retry-after':'1'}),body:Buffer.from('{"error":{"code":"scoped_embed_assets_busy"}}')});}
      }else{need(
        active < SCOPED_EMBED_LIMITS.inflight && !closedBroker,
        "scoped_embed_busy",
        429,
      );
      active++;}
      try {
        const currentContext = await current(captured);
        local(currentContext);
        need(!signal?.aborted, "scoped_embed_cancelled", 499);
        pruneJars();
        const stored = jars.get(currentContext.reference.id);
        need(
          stored || jars.size < SCOPED_EMBED_LIMITS.continuations,
          "scoped_embed_capacity",
          429,
        );
        const jar = stored?.cookies ?? {},
          cookie = route.credentialFree ? '' : Object.entries(jar)
            .filter(([, value]) => value.token && value.expiresAt > clock())
            .map(([name, value]) => name + "=" + value.token)
            .join("; ");
        // The Source virtual host is fixed in the approved profile; only the
        // actual network destination is the separately pinned local port.
        const proof = route.credentialFree ? {} : signer.headers({
          context: currentContext,
          method,
          path,
          body: requestBody,
          cookie,
        });
        const outboundHeaders = {
          ...selectedHeaders,
          host: new URL(profile.embedOrigin).host,
          ...proof,
          ...(cookie ? { cookie } : {}),
          connection: "close",
        };
        need(
          Object.entries(outboundHeaders).reduce(
            (bytes, [name, value]) =>
              bytes + Buffer.byteLength(name + ": " + value + "\r\n"),
            2,
          ) <= SCOPED_EMBED_LIMITS.headerBytes,
          "scoped_embed_header_limit",
          431,
        );
        local(currentContext);
        const response = await fetcher(`http://127.0.0.1:${localPort}` + path, {
          method,
          redirect: "manual",
          credentials: "omit",
          signal: AbortSignal.any([...(signal?[signal]:[]),...(assetLease?[assetLease.controller.signal]:[]),AbortSignal.timeout(SCOPED_EMBED_LIMITS.callMs)]),
          headers: outboundHeaders,
          ...(["GET", "HEAD"].includes(method) ? {} : { body: requestBody }),
        });
        need(
          response.status >= 200 &&
            response.status <= 599 &&
            !response.redirected,
          "scoped_embed_response_invalid",
          502,
        );
        const parts = [];
        let bytes = 0;
        if (response.body) {
          const reader = response.body.getReader();
          try {
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              bytes += value.byteLength;
              if (bytes > route.responseBytes) {
                await reader.cancel();
                need(false, "scoped_embed_response_limit", 502);
              }
              parts.push(value);
            }
          } finally {
            reader.releaseLock();
          }
        }
        // Buffer before releasing protected bytes, then independently recheck
        // the original Root scope. A failed write reply is not proof of none.
        await current(currentContext);
        local(currentContext);
        const incomingCookies=response.headers.getSetCookie?.()??[];
        if(route.credentialFree)need(incomingCookies.length===0&&!response.headers.get('location')&&[200,206,304].includes(response.status),'scoped_embed_public_asset_invalid',502);
        const incoming = cookies(
          incomingCookies,
          clock(),
          approvedCookies,
          resources ? 86400 : 300,
        );
        pruneJars();
        need(
          jars.has(currentContext.reference.id) ||
            jars.size < SCOPED_EMBED_LIMITS.continuations,
          "scoped_embed_capacity",
          429,
        );
        if(!route.credentialFree)jars.set(currentContext.reference.id, {
          expiresAt: currentContext.expiresAt,
          cookies: { ...jar, ...incoming },
        });
        const out = {};
        for (const name of [
          "content-type",
          "content-disposition",
          "etag",
          "last-modified",
          "cache-control",
          "content-security-policy",
          "referrer-policy",
          "permissions-policy",
        ]) {
          const value = response.headers.get(name);
          if (value) out[name] = value;
        }
        const location = response.headers.get("location");
        if (location) out.location = redirect(location, kind);
        let auth;
        if(kind==='auth-continue'&&response.status===200){const body=Buffer.concat(parts.map(part=>Buffer.from(part)));need(body.length<=1024,'scoped_embed_continue_invalid',502);
          let value;try{value=JSON.parse(body.toString('utf8'));}catch{need(false,'scoped_embed_continue_invalid',502);}
          auth={kind:'continued',ack:sourceContinuationAck(value,clock())};}
        if(kind==='auth-read' && response.status===200) {
          const bytes=Buffer.concat(parts.map(part=>Buffer.from(part)));need(bytes.length<=128,'scoped_embed_auth_invalid',502);
          let value;try{value=JSON.parse(bytes.toString('utf8'));}catch{need(false,'scoped_embed_auth_invalid',502);}
          closed(value,['ready']);need(typeof value.ready==='boolean','scoped_embed_auth_invalid',502);
        }
        if(kind==='auth-start' && method==='POST' && response.status===200) {
          let value;try{value=JSON.parse(Buffer.concat(parts.map(part=>Buffer.from(part))).toString('utf8'));}catch{need(false,'scoped_embed_auth_invalid',502);}
          if(resources && value?.schema==='soty.source-embed-auth.v1') {
            closed(value,['schema','nativeUrl']);
            const url=new URL(selectedNativeHandoff(profile,value.nativeUrl));
            auth={kind:'start',digest:hash(url.searchParams.get('intent'))};
          } else if(value?.schema==='planner.embed-login-authorization.v1' && !resources) {
            closed(value,['schema','authorizationUrl']);need(typeof value.authorizationUrl==='string','scoped_embed_auth_invalid',502);
            const url=new URL(redirect(value.authorizationUrl,kind));
            need(url.origin===new URL(profile.issuer).origin && url.pathname==='/human-identity/authorize','scoped_embed_auth_invalid',502);
            auth={kind:'start',digest:hash(url.searchParams.get('state'))};
          } else {
            closed(value,['schema','cancelled','stateDigest']);need(value.schema===(resources ? 'soty.source-embed-cancelled.v1' : 'planner.embed-login-cancelled.v1')&&value.cancelled===true&&/^[a-f0-9]{64}$/.test(value.stateDigest),'scoped_embed_auth_invalid',502);
            auth={kind:'cancel',digest:value.stateDigest};
          }
        }
        if(kind==='auth-start' && out.location) {
          const url=new URL(out.location),state=url.searchParams.get(resources&&url.origin===profile.nativeOrigin?'intent':'state');
          need(typeof state==='string' && /^[A-Za-z0-9_-]{43}$/.test(state),'scoped_embed_auth_invalid',502);
          auth={kind:'start',digest:hash(state)};
        } else if(kind==='auth-callback' && incoming[resources ? 'soty_rp_link' : 'planner_soty_link']?.token) {
          auth={kind:'completion',digest:hash(incoming[resources ? 'soty_rp_link' : 'planner_soty_link'].token)};
        }
        need(
          Object.entries(out).reduce(
            (bytes, [name, value]) =>
              bytes + Buffer.byteLength(name + ": " + value + "\r\n"),
            2,
          ) <= SCOPED_EMBED_LIMITS.headerBytes,
          "scoped_embed_header_limit",
          502,
        );
        return Object.freeze({
          status: response.status,
          headers: Object.freeze(out),
          ...(auth?{auth:Object.freeze(auth)}:{}),
          body: Buffer.concat(parts.map((part) => Buffer.from(part))),
        });
      } finally {
        if(assetLease)assetLease.release();else active--;
      }
    },
    forget(reference) {
      continuation(reference);
      jars.delete(reference.id);
    },
    close() {
      closedBroker = true;
      for(const controller of assetControllers)controller.abort();
      for(const item of assetQueue.splice(0)){item.cleanup();item.reject(assetError('scoped_embed_closed',503));}
      jars.clear();
    },
  });
}

/** App-owned private join. The actual RP verifier/userinfo port must supply
 * issuer/sub; a Root account claim or launch envelope alone is insufficient. */
export function createSourceCurrentSubjectPort({
  profile: raw,
  verifier,
  readAuthority,
  verifyHuman,
} = {}) {
  const profile = approvedEmbedProfile(raw);
  need(
    verifier &&
      typeof verifier.context === "function" &&
      [readAuthority, verifyHuman].every((fn) => typeof fn === "function"),
  );
  return async function currentSotySubject(request, expected) {
    need(
      expected && expected.proof && expected.continuation,
      "scoped_embed_human_proof_required",
      401,
    );
    continuation(expected.continuation);
    const native = request.headers.host === new URL(profile.nativeOrigin).host;
    if (native)
      need(
        ["/soty/connect", "/soty/disconnect", "/soty/access"].includes(
          new URL(request.url, profile.nativeOrigin).pathname,
        ),
        "scoped_embed_route_denied",
        403,
      );
    else {
      const incoming = verifier.context(request);
      need(
        hash(incoming.reference) === hash(expected.continuation),
        "scoped_embed_profile_changed",
        401,
      );
    }
    const authority = await readAuthority({
      reference: expected.continuation,
      connector: profile.connector,
    });
    need(
      authority.profileDigest === profile.digest,
      "scoped_embed_context_mismatch",
      403,
    );
    const human = await verifyHuman(expected.proof);
    need(
      human &&
        human.issuer === profile.issuer &&
        human.subject === authority.humanPrincipal.subject && human.issuer === authority.humanPrincipal.issuer,
      "scoped_embed_human_mismatch",
      401,
    );
    const after = await readAuthority({
      reference: expected.continuation,
      connector: profile.connector,
    });
    need(
      hash(after.reference) === hash(authority.reference) &&
        hash(after.rootPrincipal) === hash(authority.rootPrincipal) && hash(after.humanPrincipal) === hash(authority.humanPrincipal),
      "scoped_embed_profile_changed",
      401,
    );
    return Object.freeze({ issuer: human.issuer, subject: human.subject });
  };
}
