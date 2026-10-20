export interface SheetGripDismissOptions {
  /** Must identify this exact open dialog, including its current account/mount. */
  isCurrent(): boolean;
  signal?: AbortSignal;
}

/** Bind only an explicit, labelled sheet handle. Content keeps native scrolling. */
export function bindSheetGripDismiss(grip: HTMLElement, close: () => void, options: SheetGripDismissOptions): () => void {
  const doc = grip.ownerDocument, view = doc.defaultView;
  const events = new AbortController(), pointers = new Set<number>();
  let disposed = false, suppressClick = false;
  let owned: { id: number; x: number; y: number; dx: number; dy: number; moved: boolean } | null = null;
  const live = (): boolean => !disposed && !options.signal?.aborted && grip.isConnected && options.isCurrent();
  const release = (id: number): void => { try { if (grip.hasPointerCapture(id)) grip.releasePointerCapture(id); } catch { /* Capture can already be lost. */ } };
  const cancel = (): void => {
    const previous = owned; owned = null;
    if (previous) { suppressClick = true; release(previous.id); }
  };
  const cancelAll = (): void => { pointers.clear(); cancel(); };
  const dispose = (): void => {
    if (disposed) return;
    disposed = true; events.abort(); options.signal?.removeEventListener('abort', dispose); cancelAll();
  };
  if (options.signal?.aborted) { dispose(); return dispose; }
  options.signal?.addEventListener('abort', dispose, { once: true });
  const listenerOptions = { capture: true, signal: events.signal };
  doc.addEventListener('pointerdown', event => {
    pointers.add(event.pointerId);
    if (pointers.size > 1) { cancel(); suppressClick = true; }
  }, listenerOptions);
  grip.addEventListener('pointerdown', event => {
    if (event.defaultPrevented || event.button !== 0 || !event.isPrimary || pointers.size !== 1 || !live()) return;
    cancel(); suppressClick = false;
    owned = { id: event.pointerId, x: event.clientX, y: event.clientY, dx: 0, dy: 0, moved: false };
    try { grip.setPointerCapture(event.pointerId); } catch { cancel(); }
  }, { signal: events.signal });
  doc.addEventListener('pointermove', event => {
    if (!owned || event.pointerId !== owned.id) return;
    if (!live() || pointers.size !== 1 || event.pointerType !== 'touch' && event.buttons === 0) { cancel(); return; }
    owned.dx = event.clientX - owned.x; owned.dy = event.clientY - owned.y;
    if (Math.hypot(owned.dx, owned.dy) > 7) { owned.moved = true; suppressClick = true; if (event.cancelable) event.preventDefault(); }
  }, listenerOptions);
  doc.addEventListener('pointerup', event => {
    pointers.delete(event.pointerId);
    if (!owned || event.pointerId !== owned.id) return;
    const previous = owned; owned = null;
    const dx = event.clientX - previous.x, dy = event.clientY - previous.y;
    const moved = previous.moved || Math.hypot(dx, dy) > 7;
    if (moved) { suppressClick = true; if (event.cancelable) event.preventDefault(); }
    release(previous.id);
    if (live() && !pointers.size && dy > 64 && dy > Math.abs(dx) * 1.2) close();
  }, listenerOptions);
  doc.addEventListener('pointercancel', event => {
    pointers.delete(event.pointerId);
    if (event.pointerId === owned?.id) cancel();
  }, listenerOptions);
  grip.addEventListener('lostpointercapture', event => {
    if (event.pointerId === owned?.id && !grip.hasPointerCapture(event.pointerId)) cancel();
  }, { signal: events.signal });
  grip.addEventListener('click', event => {
    if (event.defaultPrevented || event.detail > 0 && suppressClick || owned || pointers.size || !live()) {
      event.preventDefault(); event.stopImmediatePropagation(); return;
    }
    // Native button click covers tap, mouse, Enter, Space and assistive activation.
    close();
  }, { capture: true, signal: events.signal });
  view?.addEventListener('blur', cancelAll, { signal: events.signal });
  view?.addEventListener('pagehide', cancelAll, { signal: events.signal });
  return dispose;
}
