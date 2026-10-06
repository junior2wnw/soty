export interface ViewportInput { layoutHeight: number; height: number; top: number; scale: number; editable: boolean }
export function resolveVisibleViewport(input: ViewportInput): { height: number; top: number; keyboard: boolean; short: boolean } | null {
  if (![input.layoutHeight, input.height, input.top, input.scale].every(Number.isFinite) || input.layoutHeight <= 0 || input.height <= 0 || Math.abs(input.scale - 1) > .02) return null;
  const height = Math.min(input.layoutHeight, input.height), top = Math.max(0, Math.min(input.top, input.layoutHeight - height));
  return { height: Math.round(height), top: Math.round(top), keyboard: input.editable && input.layoutHeight - height > 100, short: height < 500 };
}

const owners = new WeakMap<Document, { refs: number; stop(): void }>();
/** One bounded observer per document; browser pinch zoom keeps its native geometry. */
export function observeVisibleViewport(doc: Document): () => void {
  let released = false;
  const present = owners.get(doc);
  if (present) { present.refs++; return release; }
  const view = doc.defaultView, viewport = view?.visualViewport;
  if (!view || !viewport) return () => {};
  const root = doc.documentElement; let frame = 0, previous = '';
  const clear = (): void => { root.style.removeProperty('--soty-visible-height'); root.style.removeProperty('--soty-visible-top'); delete root.dataset.sotyKeyboard; delete root.dataset.sotyShortViewport; };
  const update = (): void => {
    frame = 0;
    const editable = !!doc.activeElement?.matches('textarea, input:not([type=checkbox]):not([type=radio]):not([type=button]):not([type=submit]):not([type=file]), [contenteditable=true]');
    const state = resolveVisibleViewport({ layoutHeight: view.innerHeight, height: viewport.height, top: viewport.offsetTop, scale: viewport.scale, editable });
    const key = JSON.stringify(state); if (key === previous) return; previous = key;
    if (!state) { clear(); return; }
    root.style.setProperty('--soty-visible-height', `${state.height}px`); root.style.setProperty('--soty-visible-top', `${state.top}px`);
    if (state.keyboard) root.dataset.sotyKeyboard = 'true'; else delete root.dataset.sotyKeyboard;
    if (state.short) root.dataset.sotyShortViewport = 'true'; else delete root.dataset.sotyShortViewport;
  };
  const schedule = (): void => { if (!frame) frame = view.requestAnimationFrame(update); };
  for (const type of ['resize', 'scroll']) viewport.addEventListener(type, schedule);
  view.addEventListener('resize', schedule); doc.addEventListener('focusin', schedule); doc.addEventListener('focusout', schedule);
  owners.set(doc, { refs: 1, stop() {
    for (const type of ['resize', 'scroll']) viewport.removeEventListener(type, schedule);
    view.removeEventListener('resize', schedule); doc.removeEventListener('focusin', schedule); doc.removeEventListener('focusout', schedule);
    if (frame) view.cancelAnimationFrame(frame); clear();
  } });
  update(); return release;
  function release(): void {
    if (released) return; released = true;
    const owner = owners.get(doc); if (owner && --owner.refs === 0) { owner.stop(); owners.delete(doc); }
  }
}
