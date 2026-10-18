/** Optional gallery-stage gesture only. Buttons/keyboard remain independent.
 * Stage input and a scoped viewport-resize listener; normal pan-y/pinch-zoom is CSS. */
export function bindGallerySwipe(stage, { current, previous, next, selectionEpoch = () => 0,
  viewport = stage.ownerDocument?.defaultView?.visualViewport ?? globalThis.visualViewport ?? null }) {
  const abort = new AbortController(), pointers = new Set(); let gesture = null, closed = false;
  const style = stage.style, originalAction = style?.getPropertyValue?.('touch-action') ?? style?.touchAction ?? '', originalPriority = style?.getPropertyPriority?.('touch-action') ?? '';
  const restoreAction = () => {
    if (!style) return;
    if (style.setProperty) { if (originalAction) style.setProperty('touch-action', originalAction, originalPriority); else style.removeProperty('touch-action'); }
    else style.touchAction = originalAction;
  };
  const pointer = event => event.pointerType === 'touch' || event.pointerType === 'pen';
  const clear = () => {
    const id = gesture?.id; gesture = null;
    try { if (id !== undefined && stage.hasPointerCapture?.(id)) stage.releasePointerCapture?.(id); } catch { /* Native cancellation already released this capture. */ }
  };
  const cancel = () => { clear(); pointers.clear(); };
  const epoch = () => { const value = selectionEpoch(); return Number.isSafeInteger(value) && value >= 0 ? value : null; };
  const viewportReady = () => {
    let scale; try { scale = viewport === null ? 1 : viewport.scale; } catch { scale = undefined; }
    const ready = typeof scale === 'number' && Number.isFinite(scale) && scale > 0 && scale <= 1.01;
    if (!ready) {
      cancel();
      if (style?.setProperty) style.setProperty('touch-action', 'pan-x pan-y pinch-zoom'); else if (style) style.touchAction = 'pan-x pan-y pinch-zoom';
    } else restoreAction();
    return ready;
  };
  const down = event => {
    if (closed || !pointer(event)) return;
    if (!viewportReady()) return;
    pointers.add(event.pointerId);
    if (pointers.size !== 1 || event.isPrimary !== true || event.button !== 0) { clear(); return; }
    const selectedEpoch = epoch();
    if (!current() || selectedEpoch === null || !Number.isFinite(event.clientX) || !Number.isFinite(event.clientY) || event.target?.closest?.('button,a,input,textarea,select,[role="button"]')) { clear(); return; }
    gesture = { id: event.pointerId, x: event.clientX, y: event.clientY, vertical: false, epoch: selectedEpoch };
    try { stage.setPointerCapture?.(event.pointerId); } catch { clear(); }
  };
  const move = event => {
    const zoomReady = viewportReady();
    const selected = gesture;
    if (!selected || selected.id !== event.pointerId) return;
    if (!zoomReady || !current() || selected.epoch !== epoch()) { cancel(); return; }
    const dx = event.clientX - selected.x, dy = event.clientY - selected.y;
    if (Math.abs(dy) >= 12 && Math.abs(dy) > Math.abs(dx)) selected.vertical = true;
    // Delay default prevention until a clearly horizontal, full-length intent.
    if (!selected.vertical && Math.abs(dx) >= 48 && Math.abs(dx) >= Math.abs(dy) * 1.5 && event.cancelable) event.preventDefault();
  };
  const up = event => {
    pointers.delete(event.pointerId);
    const zoomReady = viewportReady();
    if (!gesture || gesture.id !== event.pointerId) return;
    const selected = gesture, dx = event.clientX - selected.x, dy = event.clientY - selected.y;
    const ready = zoomReady && current() && selected.epoch === epoch();
    clear();
    if (closed || !ready || selected.vertical || event.isPrimary !== true || !Number.isFinite(dx) || !Number.isFinite(dy)
      || Math.abs(dx) < 48 || Math.abs(dx) < Math.abs(dy) * 1.5) return;
    if (event.cancelable) event.preventDefault(); if (dx < 0) next(); else previous();
  };
  stage.addEventListener('pointerdown', down, { signal: abort.signal });
  stage.addEventListener('pointermove', move, { passive: false, signal: abort.signal });
  stage.addEventListener('pointerup', up, { signal: abort.signal });
  const pointerCancelled = event => { pointers.delete(event.pointerId); clear(); };
  stage.addEventListener('pointercancel', pointerCancelled, { signal: abort.signal });
  stage.addEventListener('lostpointercapture', pointerCancelled, { signal: abort.signal });
  const resized = () => { cancel(); viewportReady(); };
  viewport?.addEventListener?.('resize', resized, { signal: abort.signal });
  viewportReady();
  const dispose = () => { if (closed) return; closed = true; abort.abort(); viewport?.removeEventListener?.('resize', resized); cancel(); restoreAction(); };
  dispose.cancel = cancel;
  return dispose;
}
