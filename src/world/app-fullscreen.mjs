/** Fullscreen changes presentation only: the running iframe stays connected. */
export function mountAppFullscreen({ screen, isCurrent, onChange }) {
  const doc = screen.ownerDocument;
  let disposed = false, pending = false, windowExpanded = false, generation = 0;
  const inertSiblings = new Map();
  const active = () => windowExpanded || doc.fullscreenElement === screen;
  const current = () => !disposed && isCurrent() && screen.isConnected;
  function render() {
    screen.classList.toggle('is-expanded', active());
    screen.classList.toggle('is-window-expanded', windowExpanded);
    if (!disposed) onChange({ active: active(), pending });
  }
  function restoreWindow() {
    if (!windowExpanded) return;
    windowExpanded = false;
    for (const [node, before] of inertSiblings) if (node.inert === true) node.inert = before;
    inertSiblings.clear();
  }
  function expandWindow() {
    if (!current() || doc.fullscreenElement) return;
    windowExpanded = true;
    // Keep the stage in its original DOM location, including on mobile browsers
    // without Fullscreen API. Only the surrounding shell leaves the tab order.
    for (let node = screen; node?.parentElement; node = node.parentElement) {
      for (const sibling of node.parentElement.children) {
        if (sibling !== node && !['SCRIPT', 'STYLE', 'LINK'].includes(sibling.tagName)) {
          inertSiblings.set(sibling, Boolean(sibling.inert)); sibling.inert = true;
        }
      }
      if (node.parentElement === doc.body) break;
    }
  }
  async function exitOwnFullscreen() {
    if (doc.fullscreenElement === screen && typeof doc.exitFullscreen === 'function') {
      try { await doc.exitFullscreen(); } catch { /* The browser may already be exiting. */ }
    }
  }
  async function leave() {
    generation++; restoreWindow();
    await exitOwnFullscreen();
    pending = false; render();
  }
  async function toggle() {
    if (!current() || pending) return;
    if (active()) { await leave(); return; }
    // Never take over another fullscreen element, such as an account prompt.
    if (doc.fullscreenElement) return;
    const turn = ++generation;
    pending = true; render();
    try {
      if (doc.fullscreenEnabled && typeof screen.requestFullscreen === 'function') {
        // Called in the original click stack, before any await or network read.
        await screen.requestFullscreen({ navigationUI: 'hide' });
        if (!current() || generation !== turn) await exitOwnFullscreen();
      } else if (current()) expandWindow();
    } catch {
      if (current() && generation === turn) expandWindow();
    } finally {
      if (generation === turn) { pending = false; render(); }
    }
  }
  function changed() {
    if (!current()) { void leave(); return; }
    if (doc.fullscreenElement === screen) restoreWindow();
    render();
  }
  function escape(event) {
    if (event.key === 'Escape' && windowExpanded && !event.defaultPrevented) {
      event.preventDefault(); void leave();
    }
  }
  doc.addEventListener('fullscreenchange', changed);
  doc.addEventListener('keydown', escape);
  render();
  return { toggle, leave, active,
    dispose() {
      if (disposed) return;
      disposed = true; generation++; restoreWindow();
      doc.removeEventListener('fullscreenchange', changed); doc.removeEventListener('keydown', escape);
      void exitOwnFullscreen(); render();
    },
  };
}
