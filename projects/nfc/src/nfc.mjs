import { messageSignature, snapshotMessage } from './records.mjs';

export function nfcAvailability({ secure = true, embedded = false, Reader, userAgent = '' } = {}) {
  if (embedded) return { supported: false, reason: 'embedded', title: 'Откройте для работы с NFC', detail: 'Телефон получает доступ к меткам, когда приложение открыто отдельным окном.' };
  if (!secure) return { supported: false, reason: 'secure', title: 'Нужен защищённый адрес', detail: 'Откройте приложение по ссылке https.' };
  if (!Reader) return { supported: false, reason: /iPhone|iPad/i.test(userAgent) ? 'ios' : 'browser', title: 'Подготовьте здесь, запишите с Android',
    detail: /iPhone|iPad/i.test(userAgent) ? 'iPhone может открыть ссылку с метки. Для записи из этого приложения нужен Chrome на Android с NFC.' : 'Запись и чтение доступны в Chrome на Android с NFC. На этом устройстве можно подготовить данные и сохранить шаблон.' };
  return { supported: true, reason: 'available', title: 'Можно работать с NFC', detail: 'Включите NFC на телефоне. Приложите метку к задней стороне телефона после нажатия кнопки.' };
}
export function nfcError(error) {
  const messages = {
    NotAllowedError: 'Разрешите этому сайту доступ к NFC в настройках браузера и повторите действие.',
    NotSupportedError: 'Телефон или метка не поддерживает эту операцию. Попробуйте обычную перезаписываемую NDEF-метку.',
    NotReadableError: 'Не удалось включить NFC. Проверьте, что NFC включён и экран телефона разблокирован.',
    InvalidStateError: 'Откройте приложение отдельным окном и повторите действие.',
    NetworkError: 'Не удалось закончить операцию. Держите метку неподвижно; проверьте её защиту и объём памяти.',
    TimeoutError: 'Метка не появилась вовремя. Поднесите её к NFC-антенне телефона и попробуйте снова.',
    AbortError: 'Операция остановлена.',
    VerifyError: 'Данные прочитанной метки отличаются от того, что вы записывали. Прочитайте метку ещё раз.',
  };
  return messages[error?.name] || 'Не удалось выполнить операцию. Попробуйте ещё раз.';
}
export function nfcFailure(error, { action = 'read', overwrite = false, permission = 'unknown' } = {}) {
  const failure = { action, errorName: error?.name || 'Error', canOverwrite: false, title: 'Не получилось', message: nfcError(error) };
  if (error?.name !== 'NotAllowedError') return failure;
  if (permission === 'denied') return { ...failure, title: 'Нужен доступ к NFC' };
  if (action === 'write' && !overwrite) {
    return { ...failure, canOverwrite: true, title: permission === 'granted' ? 'На метке уже есть данные' : 'Запись остановлена',
      message: permission === 'granted' ? 'Можно заменить их новым содержимым.' : 'Метка может быть занята. Замените её содержимое или проверьте доступ к NFC.' };
  }
  if (action === 'write' && permission === 'granted') return { ...failure, title: 'Запись запрещена', message: 'Проверьте защиту метки. Для записи нужна перезаписываемая NDEF-метка.' };
  return { ...failure, title: 'Нужен доступ к NFC' };
}
export function createNfcController({ Reader, onState = () => {}, getPermission = () => 'unknown', timeoutMs = 45_000, verifyMs = 12_000, watchMs = 300_000, now = () => Date.now() } = {}) {
  let active = null;
  const state = (phase, extra = {}) => onState({ phase, ...extra });
  function cancel(reason = 'cancelled') { if (active) { active.reason = reason; active.controller.abort(); } }
  function begin() {
    if (active) throw Object.assign(new Error('busy'), { name: 'InvalidStateError' });
    if (!Reader) throw Object.assign(new Error('unsupported'), { name: 'NotSupportedError' });
    const operation = { controller: new AbortController(), reader: new Reader(), reason: '', written: false };
    active = operation; return operation;
  }
  function capture(event) {
    return { serialNumber: event.serialNumber || '', records: snapshotMessage(event.message), message: event.message };
  }
  function scan(operation, duration) {
    const { reader, controller } = operation;
    return new Promise((resolve, reject) => {
      let done = false;
      const finish = (fn, value) => { if (done) return; done = true; clearTimeout(timer); reader.removeEventListener('reading', reading); reader.removeEventListener('readingerror', readingError); controller.signal.removeEventListener('abort', abort); fn(value); };
      const reading = event => { try { finish(resolve, capture(event)); } catch (error) { finish(reject, error); } };
      const readingError = () => state(operation.written ? 'verifying' : 'scanning', { hint: 'Эта метка не читается. Попробуйте повернуть её или использовать NDEF-метку.' });
      const abort = () => finish(reject, new DOMException('Aborted', operation.reason === 'timeout' ? 'TimeoutError' : 'AbortError'));
      const timer = setTimeout(() => { operation.reason = 'timeout'; controller.abort(); }, duration);
      reader.addEventListener('reading', reading); reader.addEventListener('readingerror', readingError); controller.signal.addEventListener('abort', abort, { once: true });
      if (controller.signal.aborted) { abort(); return; }
      try { Promise.resolve(reader.scan({ signal: controller.signal })).catch(error => finish(reject, error)); }
      catch (error) { finish(reject, error); }
    });
  }
  function release(operation) { operation.controller.abort(); if (active === operation) active = null; }
  async function read() {
    const operation = begin(); state('scanning');
    try { const result = await scan(operation, timeoutMs); state('read', { result }); return result; }
    catch (error) { state(error.name === 'AbortError' ? 'cancelled' : 'error', { ...nfcFailure(error, { permission: getPermission() }), message: operation.reason === 'hidden' ? 'Чтение остановлено: приложение было свёрнуто.' : nfcError(error) }); throw error; }
    finally { release(operation); }
  }
  async function watch() {
    const operation = begin(); state('watch-starting');
    let count = 0, lastKey = '', lastAt = -Infinity, lastHint = '';
    try {
      return await new Promise((resolve, reject) => {
        const { reader, controller } = operation;
        let done = false;
        const finish = (fn, value) => {
          if (done) return; done = true; clearTimeout(timer);
          reader.removeEventListener('reading', reading); reader.removeEventListener('readingerror', readingError); controller.signal.removeEventListener('abort', abort); fn(value);
        };
        const reading = event => {
          try {
            const result = capture(event), key = result.serialNumber + ':' + messageSignature(event.message), at = now();
            if (key === lastKey && at - lastAt >= 0 && at - lastAt < 1500) return;
            lastKey = key; lastAt = at; count++; lastHint = '';
            state('watch-read', { result, count });
          } catch { state('watching', { count, hint: 'Эти данные не удалось разобрать. Попробуйте другую метку.' }); }
        };
        const readingError = () => { lastHint = 'Метка не читается. Поверните её или попробуйте NDEF-метку.'; state('watching', { count, hint: lastHint }); };
        const abort = () => {
          state('watch-stopped', { count, message: operation.reason === 'hidden' ? 'Чтение остановлено: приложение было свёрнуто.' : operation.reason === 'timeout' ? 'Чтение остановлено через 5 минут. Можно начать снова.' : 'Чтение остановлено.' });
          finish(resolve, { count });
        };
        const timer = setTimeout(() => { operation.reason = 'timeout'; controller.abort(); }, watchMs);
        reader.addEventListener('reading', reading); reader.addEventListener('readingerror', readingError); controller.signal.addEventListener('abort', abort, { once: true });
        if (controller.signal.aborted) { abort(); return; }
        try { Promise.resolve(reader.scan({ signal: controller.signal })).then(() => { if (!done) state('watching', { count, ...(lastHint ? { hint: lastHint } : {}) }); }, error => finish(reject, error)); }
        catch (error) { finish(reject, error); }
      });
    } catch (error) { state('error', nfcFailure(error, { permission: getPermission() })); throw error; }
    finally { release(operation); }
  }
  async function write(message, { overwrite = false } = {}) {
    const operation = begin(); state('writing'); const timer = setTimeout(() => { operation.reason = 'timeout'; operation.controller.abort(); }, timeoutMs);
    try {
      await operation.reader.write(message, { overwrite, signal: operation.controller.signal });
      clearTimeout(timer); operation.written = true; state('verifying');
      try {
        const result = await scan(operation, verifyMs);
        if (messageSignature(result.message) !== messageSignature(message)) throw new DOMException('Different content', 'VerifyError');
        state('verified', { result }); return { written: true, verified: true, result };
      } catch (error) {
        const detail = error.name === 'VerifyError' ? nfcError(error) : 'Запись завершена, но проверить её не удалось. Прочитайте эту метку ещё раз.';
        state('written', { verified: false, message: detail }); return { written: true, verified: false };
      }
    } catch (error) {
      const aborted = error.name === 'AbortError';
      const failure = nfcFailure(error, { action: 'write', overwrite, permission: getPermission() });
      state(aborted ? 'cancelled' : 'error', { ...failure, message: operation.reason === 'hidden' ? 'Запись остановлена: приложение было свёрнуто. Прочитайте метку, чтобы проверить её содержимое.' : operation.reason === 'timeout' ? nfcError({ name: 'TimeoutError' }) : aborted ? 'Запись остановлена. Прочитайте метку, если хотите проверить её содержимое.' : failure.message });
      throw error;
    } finally { clearTimeout(timer); release(operation); }
  }
  async function lock() {
    const operation = begin();
    if (typeof operation.reader.makeReadOnly !== 'function') { release(operation); throw new DOMException('Unsupported', 'NotSupportedError'); }
    state('locking'); const timer = setTimeout(() => { operation.reason = 'timeout'; operation.controller.abort(); }, timeoutMs);
    try { await operation.reader.makeReadOnly({ signal: operation.controller.signal }); state('locked'); }
    catch (error) { state(error.name === 'AbortError' ? 'cancelled' : 'error', nfcFailure(error, { action: 'lock', permission: getPermission() })); throw error; }
    finally { clearTimeout(timer); release(operation); }
  }
  return { read, watch, write, lock, cancel, get busy() { return active !== null; } };
}
