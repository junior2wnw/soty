import { canonicalHash, freezeDeep, AccessError } from './validation.mjs';
import { captureQueryData, createTrustedReadonlyQueryAdapter, READONLY_QUERY_PROFILE } from './readonly-queries.mjs';

export const PEREMETRIKA_READ_PROTOCOL = 'soty.peremetrika-selected-read-query.v1';
const need = (value, code = 'external_query_invalid') => { if (!value) throw new AccessError(code); };
const closed = (value, required, optional = [], code) => need(value && typeof value === 'object' && !Array.isArray(value)
  && required.every(key => Object.hasOwn(value, key)) && Object.keys(value).every(key => [...required, ...optional].includes(key)), code);
const id = value => need(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,159}$/u.test(value)
  && !value.includes('..') && !value.includes('//'));
const nativeId = value => need(typeof value === 'string' && /^[A-Za-z0-9][A-Za-z0-9_-]{0,79}$/u.test(value)
  && !['constructor', 'prototype', '__proto__'].includes(value));
const pin = value => {
  closed(value, ['id', 'version', 'digest']); id(value.id);
  need(Number.isSafeInteger(value.version) && value.version > 0 && value.version <= 1000000
    && /^[a-f0-9]{64}$/u.test(value.digest)); return value;
};

/** Trusted CODE port. Existing pma/pts secrets stay private; pma has native write
 * rights and is NOT advertised as a read-only credential. This handler can only
 * issue the reviewed fixed GET routes. It never registers, edits or publishes. */
export function createPeremetrikaReadonlyQueryAdapter({ origin, resource, selection, expectedProfile,
  sourceRelease, bindingId, bindingVersion = 1, withAuthority: hostAuthority, resolveCredential,
  assertDestination, allowLoopback = false, fetch: fetcher = globalThis.fetch } = {}) {
  const scope = captureQueryData(resource), selected = captureQueryData(selection), profile = pin(captureQueryData(expectedProfile)),
    release = pin(captureQueryData(sourceRelease));
  closed(scope, ['registryId', 'tenantId', 'appId', 'environmentId', 'resourceId', 'sourceActorId', 'sourceMode']);
  Object.values(scope).forEach(id);
  need(['agent-token', 'participant'].includes(scope.sourceMode), 'external_resource_denied');
  if (selected.kind === 'page') {
    closed(selected, ['kind', 'pageId']); nativeId(selected.pageId); need(/^pag_/u.test(selected.pageId));
  } else {
    closed(selected, ['kind', 'itemId', 'version', 'sourcePageId']);
    need(selected.kind === 'library-version' && Number.isSafeInteger(selected.version) && selected.version > 0 && selected.version <= 1000000);
    nativeId(selected.itemId); nativeId(selected.sourcePageId); need(/^pag_/u.test(selected.sourcePageId));
  }
  id(bindingId); need(Number.isSafeInteger(bindingVersion) && bindingVersion > 0 && bindingVersion <= 1000000);
  let url; try { url = new URL(origin); } catch { need(false); }
  need(origin === url.origin && !url.username && !url.password && (url.protocol === 'https:'
    || (allowLoopback === true && url.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)
      && Number(url.port) >= 1024 && Number(url.port) <= 65535)));
  need([hostAuthority, resolveCredential, assertDestination, fetcher].every(value => typeof value === 'function')
    && hostAuthority.constructor?.name !== 'AsyncFunction');
  const selectionDigest = canonicalHash(selected);
  const binding = freezeDeep({ id: bindingId, version: bindingVersion, digest: canonicalHash({ profile: READONLY_QUERY_PROFILE,
    protocol: PEREMETRIKA_READ_PROTOCOL, origin, resource: scope, selection: selected, expectedProfile: profile, sourceRelease: release }) });
  const path = selected.kind === 'page' ? '/api/v1/soty-read/pages/' + selected.pageId
    : '/api/v1/soty-read/library/' + selected.itemId + '/versions/' + selected.version;
  function authorization(value) {
    need(value && value.resources?.length === 1 && value.resources[0] === scope.resourceId && value.effects?.length === 0
      && value.recipients?.length === 1 && value.recipients[0] === scope.resourceId, 'external_resource_denied'); id(value.accountId);
  }
  const adapter = createTrustedReadonlyQueryAdapter({
    withAuthority(request, callback) {
      if (request.authorization) authorization(request.authorization); else id(request.actor?.accountId);
      return hostAuthority(request, freezeDeep({ resource: scope, binding }), callback);
    },
    async query(raw, signal, currentAuthority) {
      const request = captureQueryData(raw);
      closed(request, ['requestId', 'input', 'authorization']); authorization(request.authorization);
      closed(request.input, [], ['blockId']);
      if (request.input.blockId !== undefined) nativeId(request.input.blockId);
      need(typeof currentAuthority === 'function' && currentAuthority.constructor?.name !== 'AsyncFunction');
      const fresh = () => { need(!signal?.aborted, 'external_query_unconfirmed'); need(currentAuthority() === true, 'external_resource_denied'); };
      fresh();
      const lease = captureQueryData(await resolveCredential(request, freezeDeep({ resource: scope, selection: selected, purpose: 'query' })));
      fresh();
      closed(lease, ['sotyAccountId', 'sourceActorId', 'sourceMode', 'expiresAt', 'token']);
      need(lease.sotyAccountId === request.authorization.accountId && lease.sourceActorId === scope.sourceActorId
        && lease.sourceMode === scope.sourceMode && Number.isFinite(Date.parse(lease.expiresAt)) && Date.parse(lease.expiresAt) > Date.now()
        && (scope.sourceMode === 'agent-token' ? /^pma_[A-Za-z0-9_-]{43}$/u : /^pts_[A-Za-z0-9_-]{43}$/u).test(lease.token), 'external_resource_denied');
      async function http(route) {
        fresh(); need((await assertDestination(origin)) === true, 'external_query_unconfirmed'); fresh();
        need(Date.parse(lease.expiresAt) > Date.now(), 'external_resource_denied');
        const response = await fetcher(origin + route, { method: 'GET', credentials: 'omit', redirect: 'error', referrerPolicy: 'no-referrer', signal,
          headers: { accept: 'application/json', authorization: 'Bearer ' + lease.token } });
        fresh();
        need(!response.redirected && (!response.url || response.url === origin + route)
          && /^application\/json(?:;|$)/iu.test(response.headers.get('content-type') || ''), 'external_query_output_invalid');
        if ([401, 403, 404].includes(response.status)) { await response.body?.cancel(); need(false, 'external_resource_denied'); }
        need(response.ok, 'external_query_unconfirmed');
        const length = response.headers.get('content-length');
        need(!length || (/^\d+$/u.test(length) && Number(length) <= 65536), 'external_query_output_invalid');
        need(response.body, 'external_query_output_invalid');
        const reader = response.body.getReader(), parts = []; let bytes = 0;
        try {
          for (;;) {
            const part = await reader.read(); fresh(); if (part.done) break;
            bytes += part.value.byteLength;
            if (bytes > 65536) { await reader.cancel(); need(false, 'external_query_output_invalid'); }
            parts.push(Buffer.from(part.value));
          }
        } finally { reader.releaseLock(); }
        let envelope;
        try { envelope = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.concat(parts, bytes))); }
        catch { need(false, 'external_query_output_invalid'); }
        closed(envelope, ['ok', 'data'], [], 'external_query_output_invalid'); need(envelope.ok === true, 'external_query_output_invalid');
        fresh(); return captureQueryData(envelope.data);
      }
      function verify(data, contentRequired = false) {
        closed(data, ['profile', 'authority', 'metadata'], ['content', 'selectedVersion'], 'external_query_output_invalid');
        need(canonicalHash(data.profile) === canonicalHash(profile), 'external_resource_denied');
        closed(data.authority, ['actorId', 'mode', 'expiresAt', 'canRead', 'selection', 'selectionDigest'], [], 'external_query_output_invalid');
        need(data.authority.actorId === scope.sourceActorId && data.authority.mode === scope.sourceMode && data.authority.canRead === true
          && canonicalHash(data.authority.selection) === selectionDigest && data.authority.selectionDigest === selectionDigest
          && typeof data.authority.expiresAt === 'string' && Date.parse(data.authority.expiresAt) > Date.now(), 'external_resource_denied');
        closed(data.metadata, ['pageId', 'revision', 'specHash', 'title', 'status'], [], 'external_query_output_invalid');
        need(data.metadata.pageId === (selected.kind === 'page' ? selected.pageId : selected.sourcePageId)
          && Number.isSafeInteger(data.metadata.revision) && data.metadata.revision > 0 && /^[a-f0-9]{64}$/u.test(data.metadata.specHash)
          && typeof data.metadata.title === 'string' && data.metadata.title.length <= 300
          && typeof data.metadata.status === 'string' && data.metadata.status.length <= 80, 'external_query_output_invalid');
        need(contentRequired === Object.hasOwn(data, 'content'), 'external_query_output_invalid');
        if (contentRequired) {
          closed(data.content, ['outline'], request.input.blockId ? ['block'] : [], 'external_query_output_invalid');
          need(!request.input.blockId || data.content.block?.id === request.input.blockId, 'external_query_output_invalid');
        }
        if (selected.kind === 'library-version') {
          closed(data.selectedVersion, ['itemId', 'kind', 'version', 'title', 'sourcePageId', 'sourceRevision', 'specHash', 'versionHash'], [], 'external_query_output_invalid');
          need(data.selectedVersion.itemId === selected.itemId && data.selectedVersion.version === selected.version
            && data.selectedVersion.sourcePageId === selected.sourcePageId && ['template', 'module'].includes(data.selectedVersion.kind)
            && /^[a-f0-9]{64}$/u.test(data.selectedVersion.specHash) && /^[a-f0-9]{64}$/u.test(data.selectedVersion.versionHash), 'external_query_output_invalid');
        } else need(!Object.hasOwn(data, 'selectedVersion'), 'external_query_output_invalid');
        return data;
      }
      const before = verify(await http(path + '/authority'));
      const data = verify(await http(path + (request.input.blockId ? '/blocks/' + request.input.blockId : '')), true);
      const after = verify(await http(path + '/authority'));
      // A changing native revision is not silently described as one snapshot.
      need(canonicalHash(before.metadata) === canonicalHash(data.metadata) && canonicalHash(after.metadata) === canonicalHash(data.metadata)
        && (!data.selectedVersion || canonicalHash(before.selectedVersion) === canonicalHash(data.selectedVersion)
          && canonicalHash(after.selectedVersion) === canonicalHash(data.selectedVersion)), 'external_query_unconfirmed');
      const outline = data.content.outline;
      closed(outline, ['schemaVersion', 'title', 'sheets'], [], 'external_query_output_invalid');
      need(typeof outline.schemaVersion === 'string' && ['1.0', '2.0'].includes(outline.schemaVersion)
        && typeof outline.title === 'string' && outline.title.length <= 300 && Array.isArray(outline.sheets), 'external_query_output_invalid');
      const blocks = outline.sheets.flatMap(sheet => {
        closed(sheet, ['id', 'title', 'sections'], [], 'external_query_output_invalid'); nativeId(sheet.id);
        need(typeof sheet.title === 'string' && sheet.title.length <= 300 && Array.isArray(sheet.sections), 'external_query_output_invalid');
        return sheet.sections.flatMap(section => {
          closed(section, ['id', 'label', 'blocks'], [], 'external_query_output_invalid'); nativeId(section.id);
          need(typeof section.label === 'string' && section.label.length <= 300 && Array.isArray(section.blocks), 'external_query_output_invalid');
          return section.blocks.map(block => {
            closed(block, ['id', 'type', 'title'], [], 'external_query_output_invalid'); nativeId(block.id); nativeId(block.type);
            need(typeof block.title === 'string' && block.title.length <= 1000, 'external_query_output_invalid');
            return { sheetId: sheet.id, sheetTitle: sheet.title, sectionId: section.id, sectionLabel: section.label, ...block };
          });
        });
      });
      need(blocks.length <= 128, 'external_query_output_invalid');
      const content = { outline: { schemaVersion: outline.schemaVersion, title: outline.title, blocks },
        ...(data.content.block ? { blockJson: JSON.stringify(data.content.block) } : {}) };
      fresh();
      return captureQueryData({ resourceId: scope.resourceId, selection: selected, metadata: data.metadata, content,
        ...(data.selectedVersion ? { selectedVersion: data.selectedVersion } : {}) });
    },
  });
  return Object.freeze({ binding, adapter });
}
