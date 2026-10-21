import { coverError, coverId } from './cover-api.mjs';
import { ownedCoverImage } from './cover-editor-state.mjs';
/** Current-account RAM only. Each visible admission is a fresh signed read;
 * off-screen pictures are dropped rather than persisted in a cross-ACL cache. */
export function createAppCoverPresenter({ accountId, isCurrent, readApp, makeImage = ownedCoverImage, now = () => Date.now(),
  maximumTargets = 32, maximumBindings = 128, maximumBytes = 8 * 1024 * 1024 }) {
  if (!coverId(accountId) || typeof isCurrent !== 'function' || typeof readApp !== 'function'
      || !Number.isSafeInteger(maximumTargets) || maximumTargets < 1 || maximumTargets > 48 || !Number.isSafeInteger(maximumBindings) || maximumBindings < maximumTargets || maximumBindings > 256
      || !Number.isSafeInteger(maximumBytes) || maximumBytes < 1 || maximumBytes > 8 * 1024 * 1024) throw coverError('cover_configuration_invalid');
  const bindings = new Set(), entries = new Map(), queue = []; let disposed = false, generation = 0, running = 0, bytes = 0;
  const current = () => !disposed && isCurrent() === true;
  const live = binding => current() && !binding.superseded && bindings.has(binding) && binding.isCurrent() === true;
  const paint = (binding, value) => { if (live(binding)) { try { binding.onVisual(value); } catch { /* A detached host target cannot stall other workers or resource disposal. */ } } };
  function reset(entry) {
    for (const binding of entry.bindings) paint(binding, { mode: 'loading' });
    if (entry.image) { bytes -= entry.byteCount; entry.image.dispose(); entry.image = null; entry.byteCount = 0; }
  }
  function removeEntry(entry, retire = false) {
    reset(entry); entry.generation++; entries.delete(entry.appId);
    for (let i = queue.length - 1; i >= 0; i--) if (queue[i] === entry) queue.splice(i, 1); entry.queued = false;
    for (const binding of entry.bindings) if (binding.entry === entry) { binding.entry = null; if (retire) binding.superseded = true; }
    entry.bindings.clear();
  }
  function display(binding, entry) {
    if (!binding.visible || binding.entry !== entry) return;
    paint(binding, entry.mode === 'custom' && entry.image ? { mode: 'custom', url: entry.image.url, revision: entry.revision }
      : { mode: entry.mode === 'none' ? 'none' : 'inherit', revision: entry.revision });
  }
  function admit(binding) {
    if (!live(binding) || !binding.visible || binding.entry) return;
    let entry = entries.get(binding.appId);
    if (entry && entry.identity !== binding.identity) { removeEntry(entry, true); entry = null; }
    if (!entry) {
      if (entries.size >= maximumTargets) return;
      entry = { appId: binding.appId, identity: binding.identity, generation: 0, bindings: new Set(), queued: false, inflight: false, image: null, byteCount: 0, checkedAt: 0, mode: 'loading', revision: 0 };
      entries.set(binding.appId, entry);
    }
    binding.entry = entry; entry.bindings.add(binding); if (entry.mode !== 'loading') display(binding, entry); else enqueue(entry);
  }
  function admitWaiting() { for (const binding of bindings) if (binding.visible && !binding.entry) admit(binding); }
  function enqueue(entry) {
    if (!current() || entry.queued || entry.inflight || entries.get(entry.appId) !== entry) return;
    entry.queued = true; queue.push(entry); pump();
  }
  function pump() {
    while (current() && running < 3 && queue.length) {
      const entry = queue.shift(); entry.queued = false;
      if (entries.get(entry.appId) !== entry || entry.inflight || ![...entry.bindings].some(binding => live(binding) && binding.visible)) continue;
      entry.inflight = true; running++; const ticket = entry.generation, context = generation;
      void (async () => {
        try {
          const result = await readApp(entry.appId, 320);
          if (!current() || context !== generation || entries.get(entry.appId) !== entry || ticket !== entry.generation || ![...entry.bindings].some(binding => live(binding) && binding.visible)) { result.image?.bytes?.fill(0); return; }
          const length = result.image?.bytes?.length ?? 0;
          if (length && bytes - entry.byteCount + length > maximumBytes) { result.image.bytes.fill(0); throw coverError('cover_cache_capacity'); }
          const image = result.image ? makeImage(result.image) : null;
          if (!current() || context !== generation || ticket !== entry.generation) { image?.dispose(); return; }
          reset(entry); entry.image = image; entry.byteCount = length; bytes += length; entry.mode = result.current.mode; entry.revision = result.current.revision; entry.checkedAt = now();
          for (const binding of entry.bindings) display(binding, entry);
        } catch {
          if (current() && context === generation && entries.get(entry.appId) === entry && ticket === entry.generation) {
            // Access loss, decode failure and an unconfigured API all drop the
            // private bytes. They never leave yesterday's authorized photo up.
            reset(entry); entry.mode = 'inherit'; entry.checkedAt = now(); for (const binding of entry.bindings) display(binding, entry);
          }
        } finally {
          entry.inflight = false; running--;
          if (current() && entries.get(entry.appId) === entry && entry.bindings.size && ticket !== entry.generation) enqueue(entry);
          admitWaiting(); pump();
        }
      })();
    }
  }
  function detach(binding) {
    const entry = binding.entry; binding.entry = null; paint(binding, { mode: 'inherit' });
    if (entry) { entry.bindings.delete(binding); if (!entry.bindings.size) removeEntry(entry); } admitWaiting();
  }
  function invalidate(appId) {
    const entry = entries.get(appId); if (!entry) return; reset(entry); entry.mode = 'loading'; entry.generation++; entry.checkedAt = 0; enqueue(entry);
  }
  return Object.freeze({
    watch({ appId, identity, isCurrent: bindingCurrent, onVisual }) {
      if (!current() || !coverId(appId) || typeof identity !== 'string' || identity.length < 1 || identity.length > 16384 || typeof bindingCurrent !== 'function' || typeof onVisual !== 'function' || bindings.size >= maximumBindings) return null;
      const binding = { appId, identity, isCurrent: bindingCurrent, onVisual, visible: false, superseded: false, entry: null }; bindings.add(binding);
      return Object.freeze({
        setVisible(visible) { if (!live(binding) || binding.visible === (visible === true)) return; binding.visible = visible === true; if (binding.visible) { admit(binding); pump(); } else detach(binding); },
        refresh() { if (live(binding) && binding.visible) { if (binding.entry) invalidate(appId); else { admit(binding); pump(); } } },
        dispose() { if (!bindings.has(binding)) return; binding.visible = false; detach(binding); bindings.delete(binding); },
      });
    },
    invalidate(appId) { if (current() && coverId(appId)) invalidate(appId); },
    invalidateAll() { if (!current()) return; generation++; for (const entry of entries.values()) invalidate(entry.appId); },
    refreshVisible() { if (!current()) return; for (const entry of entries.values()) if (now() - entry.checkedAt >= 30_000) invalidate(entry.appId); },
    stats() { return Object.freeze({ running, queued: queue.filter(entry => entries.get(entry.appId) === entry && entry.queued).length, targets: entries.size, bindings: bindings.size, bytes }); },
    dispose() { if (disposed) return; for (const binding of bindings) paint(binding, { mode: 'inherit' }); for (const entry of entries.values()) removeEntry(entry); disposed = true; generation++; bindings.clear(); queue.length = 0; },
  });
}

function nodePainter(target, valid) {
  let owned = null, staticImages = [], removed = false;
  const clearOwned = () => { if (owned) { owned.remove(); owned = null; } };
  function hideStatic() {
    for (const image of target.querySelectorAll(':scope > img.uf-art')) if (image !== owned && !staticImages.includes(image)) { staticImages.push(image); image.remove(); }
    removed = true;
  }
  function restoreStatic() {
    if (!removed) return; for (const image of staticImages) if (!image.isConnected && (!image.complete || image.naturalWidth > 0)) target.append(image); removed = false;
    target.classList.toggle('has-art', staticImages.some(image => image.complete && image.naturalWidth > 0));
    staticImages = [];
  }
  return {
    paint(value) {
      if (!valid()) return; clearOwned();
      if (value.mode === 'inherit' || value.mode === 'loading') { restoreStatic(); delete target.dataset.privateCoverMode; return; }
      hideStatic(); target.classList.remove('has-art'); target.dataset.artState = 'fallback'; target.dataset.privateCoverMode = value.mode;
      if (value.mode !== 'custom') return;
      const image = document.createElement('img'); owned = image; image.className = 'uf-art'; image.alt = ''; image.decoding = 'async'; image.referrerPolicy = 'no-referrer'; image.style.objectPosition = '50% 50%';
      image.addEventListener('load', () => { if (valid() && owned === image && image.parentElement === target) { target.classList.add('has-art'); target.dataset.artState = 'ready'; } }, { once: true });
      image.addEventListener('error', () => { if (valid() && owned === image) { clearOwned(); target.classList.remove('has-art'); target.dataset.artState = 'fallback'; } }, { once: true });
      target.append(image); image.src = value.url;
    },
    dispose() { clearOwned(); if (valid()) { restoreStatic(); delete target.dataset.privateCoverMode; } staticImages = []; },
  };
}
/** Bounded bridge for the existing renderer. No renderer-wide URL relaxation,
 * entity-list request, canvas rebuild, camera write or art-key derivation. */
export function mountAppCoverField({ root, presenter, isCurrent, Observer = globalThis.IntersectionObserver, Mutations = globalThis.MutationObserver, maximumNodes = 128 }) {
  if (!Number.isSafeInteger(maximumNodes) || maximumNodes < 1 || maximumNodes > 256) throw coverError('cover_configuration_invalid');
  if (!root || typeof Observer !== 'function' || typeof Mutations !== 'function') return { dispose() {} };
  let disposed = false, scheduled = false; const records = new Map();
  const current = () => !disposed && isCurrent() === true;
  const visible = new Observer(entries => { if (!current()) return; for (const entry of entries) records.get(entry.target)?.binding.setVisible(entry.isIntersecting === true && entry.intersectionRatio > 0); }, { root, threshold: 0 });
  function remove(target, record) { visible.unobserve(target); record.binding.dispose(); record.painter.dispose(); records.delete(target); }
  function sync() {
    scheduled = false; if (!current()) return;
    for (const [target, record] of records) if (!root.contains(target) || target.dataset.entityId !== record.id || target.dataset.signature !== record.signature) remove(target, record);
    let count = 0;
    for (const target of root.querySelectorAll('.uf-node[data-entity-id]')) {
      if (++count > maximumNodes || records.size >= maximumNodes) break;
      if (records.has(target)) continue; const id = target.dataset.entityId ?? ''; if (!id.startsWith('app:')) continue;
      const appId = id.slice(4), signature = target.dataset.signature;
      const valid = () => current() && root.contains(target) && target.dataset.entityId === id && target.dataset.signature === signature;
      const painter = nodePainter(target, valid), binding = presenter.watch({ appId, identity: signature ?? id, isCurrent: valid, onVisual: value => painter.paint(value) });
      if (binding) { records.set(target, { id, signature, binding, painter }); visible.observe(target); }
    }
  }
  const mutation = new Mutations(changes => {
    if (!current() || scheduled || !changes.some(change => change.type === 'attributes' || change.target === root || Array.from(change.addedNodes).some(node => node.nodeType === 1 && (node.matches?.('.uf-node') || node.querySelector?.('.uf-node')))
      || Array.from(change.removedNodes).some(node => records.has(node) || node.nodeType === 1 && node.querySelector?.('.uf-node')))) return;
    scheduled = true; queueMicrotask(sync);
  });
  mutation.observe(root, { subtree: true, childList: true, attributes: true, attributeFilter: ['data-entity-id', 'data-signature'] }); sync();
  return Object.freeze({ dispose() { if (disposed) return; for (const [target, record] of records) remove(target, record); disposed = true; mutation.disconnect(); visible.disconnect(); } });
}
