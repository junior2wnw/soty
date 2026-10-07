import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { createSourceAppBff } from '../../server/bff.mjs';
import { fields, check } from '../../server/wire.mjs';
import { createOrdinaryAppStore } from './store.mjs';
import { createOrdinaryAppNativePort } from './native.mjs';
import { createOrdinaryFeedbackJobs } from './feedback-jobs.mjs';

/** Runnable new Source, configured by its own trusted host. Private config is
 * supplied by approved deployment; no request/HTML author config is loaded. */
export async function createOrdinaryAppServer(options) {
  const value = fields(options, ['profile', 'transportKey', 'connectorPort', 'rp', 'databasePath', 'realmId', 'cipherKey', 'keyId'],
    ['initialize', 'newResource', 'appLabel', 'allowEmptyGuest', 'allowLinkedLogin', 'listen', 'clock', 'allowLoginProofMigration','allowFeedbackJobsMigration','feedbackProcessing']);
  const listener=value.listen===undefined?null:fields(value.listen,['host','port']);
  check(listener===null||['127.0.0.1','::1'].includes(listener.host)&&Number.isSafeInteger(listener.port)&&listener.port>=1024&&listener.port<=65535,
    'ordinary_source_listener_invalid',503);
  const processing=value.feedbackProcessing===undefined?null:fields(value.feedbackProcessing,['policy','engines'],['enforcer']);
  check(value.allowLinkedLogin===undefined||typeof value.allowLinkedLogin==='boolean','ordinary_native_configuration_invalid',503);
  const label = value.appLabel ?? 'Моё приложение'; check(typeof label === 'string' && label.trim().length > 0 && label.length <= 160 && label.isWellFormed());
  const store = createOrdinaryAppStore({ databasePath: value.databasePath, realmId: value.realmId, key: value.cipherKey, keyId: value.keyId,
    initialize: value.initialize === true, format:processing?3:value.profile.sourceProfile.version===2?2:1,
    allowLoginProofMigration:value.allowLoginProofMigration===true,allowFeedbackJobsMigration:value.allowFeedbackJobsMigration===true, ...(value.clock ? { clock: value.clock } : {}) });
  try {
    if (value.newResource) {
      const resource = fields(value.newResource, ['title'], ['guestEmpty']);
      check(typeof resource.title === 'string' && resource.title.length <= 500 && resource.title.trim().length > 0);
      store.createResource({ id: value.profile.resource.selection.nativeId, incarnationId: value.profile.resource.selection.incarnationId,
        title: resource.title, guestEmpty: resource.guestEmpty === true });
    }
    let bff;
    // One private Source job service owns claim and cancellation. No separate
    // Native and host maps, and no permission/service object arrives in JSON.
    const jobs=processing?createOrdinaryFeedbackJobs({store,resourceId:value.profile.resource.selection.nativeId,incarnationId:value.profile.resource.selection.incarnationId,
      ...processing,currentProof:sessionHash=>{check(bff,'source_feedback_processor_not_ready',503);return bff.currentFeedbackJobProof(sessionHash);}}):null;
    const native = createOrdinaryAppNativePort({ store, resourceId: value.profile.resource.selection.nativeId,
      incarnationId: value.profile.resource.selection.incarnationId, allowEmptyGuest: value.allowEmptyGuest === true,allowLinkedLogin:value.allowLinkedLogin===true,
      ...(jobs?{feedbackJobs:jobs}:{}) });
    bff = createSourceAppBff({ profile: value.profile, transportKey: value.transportKey, connectorPort: value.connectorPort, rp: value.rp, storage: store.storage, native,
      allowCreateEmptyGuest: value.allowEmptyGuest === true, processingProofEnabled:processing!==null,
      ui: { appLabel: label, resourceLabel: 'Выбранный проект' }, ...(value.clock ? { clock: value.clock } : {}) });
    const escape = text => text.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
    const page = `<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>${escape(label)}</title>
      <link rel="stylesheet" href="/assets/ordinary-app.css"><main data-native-origin="${escape(value.profile.nativeOrigin)}" data-processing="${processing!==null}"><h1>${escape(label)}</h1><p id="state" role="status">Проверяем подключение…</p>
      <button id="connect" type="button" hidden>Войти через Соты</button>
      <section id="content" hidden><form id="create"><label>Название записи<input id="title" maxlength="500" required></label><button>Добавить запись</button></form>
      <ul id="items"></ul><button id="feedback-open" type="button">Сообщить проблему</button><section id="feedback" hidden><h2>Сообщить проблему</h2>
      <p id="recipient"></p><label>Что произошло?<textarea id="feedback-body" maxlength="8000"></textarea></label><p>Сообщение и выбранные материалы увидит владелец приложения.</p>
      <label>Материал к обращению<input id="feedback-file" type="file" accept="image/png,image/jpeg,image/webp,audio/webm,audio/ogg"></label>
      <button id="feedback-send" type="button">Отправить</button><p id="feedback-state" role="status"></p></section>
      ${processing?'<section id="processing"><button id="processing-open" type="button">Обработка материалов обращения</button><div id="processing-panel" hidden><h2>Обработка материалов</h2><p>Только локально и только для выбранных материалов. Автор разрешает обработку; владелец отдельно выбирает проверенный обработчик. Результат — данные для проверки человеком, он не меняет статус обращения.</p><ul id="processing-tickets"></ul><p id="processing-state" role="status"></p></div></section>':''}
      </section></main><script type="module" src="/assets/ordinary-app.js"></script></html>`;
    const bundled = typeof __SOTY_SOURCE_APP_BUNDLE__ !== 'undefined' && __SOTY_SOURCE_APP_BUNDLE__ === true;
    const assetRoot = new URL('./assets/', import.meta.url), browserBundle = new URL(bundled ? './browser.mjs' : '../../dist/browser.mjs', import.meta.url);
    const assets = new Map([['/assets/ordinary-app.js', { file: new URL('app.js', assetRoot), type: 'text/javascript' }],
      ['/assets/ordinary-app.css', { file: new URL('app.css', assetRoot), type: 'text/css' }],
      ['/assets/processing.js',{file:new URL('processing.js',assetRoot),type:'text/javascript'}], ['/assets/source-sdk.js', { file: browserBundle, type: 'text/javascript' }]]);
    const server = createServer(async (req, res) => {
      try {
        if (await bff.handleRequest(req, res)) return;
        if (!['GET', 'HEAD'].includes(req.method) || typeof req.url !== 'string') { res.writeHead(404).end(); return; }
        if (req.url === '/embed') {
          res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store', 'referrer-policy': 'no-referrer',
            'content-security-policy': "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors " + value.profile.parentOrigin });
          res.end(req.method === 'HEAD' ? undefined : page); return;
        }
        const asset = assets.get(req.url); if (!asset) { res.writeHead(404).end(); return; }
        const bytes = await readFile(asset.file); check(bytes.length <= 4194304);
        res.writeHead(200, { 'content-type': asset.type, 'cache-control': 'no-store', 'x-content-type-options': 'nosniff' }); res.end(req.method === 'HEAD' ? undefined : bytes);
      } catch { if (!res.headersSent) res.writeHead(503, { 'content-type': 'application/json' }).end('{"error":"ordinary_source_unavailable"}'); else res.destroy(); }
    });
    return Object.freeze({ server, store, bff, processing:jobs, async listen() {
      const port = listener?.port ?? Number(new URL(value.profile.nativeOrigin).port); check(Number.isSafeInteger(port) && port >= 1024 && port <= 65535);
      await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, listener?.host ?? '127.0.0.1', () => { server.off('error', reject); resolve(); }); }); return port;
    }, async close() { jobs?.close();server.closeAllConnections(); if (server.listening) await new Promise(resolve => server.close(resolve)); bff.close(); store.close(); } });
  } catch (error) { store.close(); throw error; }
}
