const guards = new WeakMap<Document, { style: HTMLStyleElement; users: number }>();

/** Keep the shell pointer stream over embedded apps without touching their nodes or styles. */
export function holdIframePointers(doc: Document): () => void {
  let guard = guards.get(doc);
  if (!guard) {
    const style = doc.createElement('style');
    style.dataset.sotyIframePointerGuard = '';
    style.textContent = 'iframe { pointer-events: none !important; }';
    doc.head.append(style);
    guard = { style, users: 0 }; guards.set(doc, guard);
  }
  const held = guard; held.users++;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    if (--held.users === 0) { held.style.remove(); guards.delete(doc); }
  };
}
