import { button, el, emptyState, iconButton } from './dom';
import { createDialog, type WorldDialog } from './dialogs';
import { clampOrb, dockOrb, readOrbPosition, writeOrbPosition, type OrbPoint, type OrbViewport } from './agent-orb-position';
import type { WorldAssistantContext, WorldBrowserAssist, WorldAssistantHandle, WorldAppOptions } from './types';
import './agent-orb.css';
import { holdIframePointers } from './iframe-pointer-guard';
import { floatingObstacles, placeFloatingControl } from './floating-layout';

export interface AgentOrbHandle {
  setAccount(accountId: string): void;
  setStage(stage: HTMLElement, expanded: boolean): void;
  setModal(modal: HTMLDialogElement | null): void;
  releaseModal(modal: HTMLDialogElement): boolean;
  resetScreen(): void;
  raise(): void;
  mountIn(host: HTMLElement): Promise<WorldAssistantHandle>;
  flush(): Promise<void>;
  hasUnsavedChanges(): boolean;
  dispose(): void;
}

/** A shell control only. It never reads the iframe or creates app permissions. */
export function mountAgentOrb(root: HTMLElement, options: {
  mount: WorldAppOptions['openAssistant'];
  context(): WorldAssistantContext | null;
  browserAssist?(): WorldBrowserAssist | null;
  focusExisting(): boolean;
}): AgentOrbHandle {
  const doc = root.ownerDocument, view = doc.defaultView!;
  const abort = new AbortController();
  const orb = iconButton('Помощник', 'sparkle'); orb.classList.add('sw-agent-orb'); orb.hidden = true;
  orb.setAttribute('popover', 'manual');
  orb.title = 'Помощник · можно перемещать'; orb.setAttribute('aria-haspopup', 'dialog'); orb.setAttribute('aria-expanded', 'false');
  const hint = el('span', 'sw-sr-only', 'Нажмите, чтобы открыть. Положение можно изменить кнопками в окне помощника, перетаскиванием или стрелками. Home — вернуть в угол.');
  hint.id = `agent-orb-${crypto.randomUUID()}`; orb.setAttribute('aria-describedby', hint.id); orb.append(hint);
  const parking = el('div'); parking.hidden = true; parking.className = 'sw-agent-parking';
  root.append(orb, parking);
  let account = '', epoch = 0, sessionEpoch = 0, disposed = false, expandedStage: HTMLElement | null = null;
  let modal: HTMLDialogElement | null = null;
  let position: OrbPoint | null = null, drag: { pointer: number; start: OrbPoint; before: OrbPoint | null; x: number; y: number; moved: boolean; releaseIframes(): void } | null = null;
  let suppressClick = false, dialog: WorldDialog | null = null, host: HTMLElement | null = null;
  let handle: WorldAssistantHandle | null = null, mounting: Promise<void> | null = null;
  const viewport = (): OrbViewport => {
    const visible = view.visualViewport;
    return { width: visible?.width ?? view.innerWidth, height: visible?.height ?? view.innerHeight, left: visible?.offsetLeft ?? 0, top: visible?.offsetTop ?? 0 };
  };
  const storage = (): Storage | null => { try { return view.localStorage; } catch { return null; } };
  function raise(): void {
    if (disposed) return;
    layout();
    if (typeof orb.showPopover !== 'function') return;
    try {
      if (orb.matches(':popover-open')) orb.hidePopover();
      if (account && orb.isConnected) orb.showPopover();
    } catch { /* The control remains usable in browsers without top-layer support. */ }
  }
  function layout(): void {
    if (disposed) return;
    const box = viewport(), preferred = position ? clampOrb(position, box) : dockOrb(box);
    const point = placeFloatingControl(preferred, box, floatingObstacles(doc, element => element === orb || orb.contains(element), modal ? '' : '.sa-stage-runtime-notice'));
    orb.style.left = `${point.x}px`; orb.style.top = `${point.y}px`;
    orb.dataset.projected = String(point.x !== preferred.x || point.y !== preferred.y);
    root.dataset.agentOrb = account ? 'ready' : 'hidden';
    orb.hidden = !account; orb.dataset.docked = String(!position);
    if (typeof orb.showPopover === 'function') {
      try {
        if (!account && orb.matches(':popover-open')) orb.hidePopover();
        else if (account && !orb.matches(':popover-open')) orb.showPopover();
      } catch { /* CSS supplies the ordinary fixed-position fallback. */ }
    }
  }
  function save(): void { const store = storage(); if (store) writeOrbPosition(store, account, position, viewport()); }
  function move(x: number, y: number): void {
    const rect = orb.getBoundingClientRect();
    position = clampOrb({ x: rect.left + x, y: rect.top + y }, viewport()); layout(); save();
  }
  function viewportChanged(): void {
    if (!drag) { const store = storage(); if (store) position = readOrbPosition(store, account, viewport()); }
    layout();
  }
  function closeDialog(restoreFocus = true): void { dialog?.close({ restoreFocus }); }
  function clearModal(): void { modal?.classList.remove('sw-agent-modal-host'); modal = null; }
  function placeOrb(): void {
    if (modal && (!modal.isConnected || !modal.open)) clearModal();
    const parent = modal ?? (expandedStage?.isConnected ? expandedStage : root);
    orb.inert = false; if (orb.parentElement !== parent) parent.append(orb); layout();
  }
  function resetScreen(): void {
    cancelDrag();
    closeDialog(false); if (host) parking.append(host);
    expandedStage = null; placeOrb();
  }
  function resetSession(): void {
    sessionEpoch++; resetScreen(); handle?.dispose(); handle = null; host?.remove(); host = null; mounting = null;
  }
  function ensure(parent: HTMLElement): Promise<void> {
    const owner = account, generation = epoch, screen = sessionEpoch;
    const current = (): boolean => !disposed && !!owner && account === owner && epoch === generation && sessionEpoch === screen;
    host ??= el('div', 'sw-assistant-host'); host.dataset.pwaIgnore = ''; parent.append(host);
    if (handle || mounting) return mounting ?? Promise.resolve();
    const target = host;
    if (!options.mount) { target.replaceChildren(emptyState('Помощник пока не подключён', 'Продолжайте работу в приложении.')); return Promise.resolve(); }
    target.replaceChildren(el('p', 'sw-muted', 'Открываем помощника…'));
    mounting = Promise.resolve().then(() => {
      if (!current()) return null;
      return options.mount!(target, { context: () => current() ? options.context() : null, browserAssist: () => current() ? options.browserAssist?.() ?? null : null, isCurrent: current });
    }).then(result => {
      if (!result) return;
      if (!current()) { result.dispose(); return; }
      handle = result;
    }).catch(() => {
      if (current()) target.replaceChildren(emptyState('Помощник пока недоступен', 'Закройте это окно и попробуйте ещё раз.'));
    }).finally(() => { if (current()) mounting = null; });
    return mounting;
  }
  function open(): void {
    if (disposed || !account || dialog?.element.open || !modal && options.focusExisting()) return;
    const owner = account, generation = epoch, screen = sessionEpoch;
    const current = (): boolean => !disposed && account === owner && epoch === generation && sessionEpoch === screen;
    const parent = modal?.isConnected && modal.open ? modal : expandedStage?.isConnected ? expandedStage : doc.body;
    const next = createDialog('Помощник', () => {
      if (host) parking.append(host);
      if (dialog === next) dialog = null;
      orb.setAttribute('aria-expanded', 'false');
    }, { isCurrent: current, resolve: () => orb }, parent);
    dialog = next; next.element.classList.add('sw-agent-dialog'); orb.setAttribute('aria-expanded', 'true');
    next.element.addEventListener('keydown', event => {
      if (event.key !== 'Escape') return;
      event.preventDefault(); event.stopPropagation(); next.close();
    });
    const context = current() ? options.context() : null;
    if (context) next.body.append(el('p', 'sw-agent-context', `Открыто: ${context.name}`));
    const placement = el('details', 'sw-agent-position'), summary = el('summary', '', 'Положение кнопки помощника'), actions = el('div', 'sw-agent-position-actions');
    for (const [label, symbol, x, y] of [['Влево', 'back', -32, 0], ['Вверх', 'up', 0, -32], ['Вниз', 'down', 0, 32], ['Вправо', 'arrow', 32, 0]] as const)
      actions.append(button(label, symbol, 'sw-button-quiet', () => { if (current()) move(x, y); }));
    actions.append(button('Вернуть в угол', 'grid', 'sw-button-quiet', () => { if (current()) { position = null; layout(); save(); } }));
    placement.append(summary, actions); next.body.append(placement);
    void ensure(next.body);
  }
  orb.addEventListener('click', event => { if (suppressClick && event.detail > 0) { suppressClick = false; event.preventDefault(); return; } suppressClick = false; open(); }, { signal: abort.signal });
  orb.addEventListener('pointerdown', event => {
    if (drag || event.button !== 0 || !event.isPrimary || !account) return;
    suppressClick = false;
    const rect = orb.getBoundingClientRect(); drag = { pointer: event.pointerId, start: { x: rect.x, y: rect.y }, before: position, x: event.clientX, y: event.clientY, moved: false, releaseIframes: holdIframePointers(doc) };
    try { orb.setPointerCapture(event.pointerId); } catch { cancelDrag(); }
  }, { signal: abort.signal });
  orb.addEventListener('pointermove', event => {
    if (!drag || event.pointerId !== drag.pointer) return;
    const dx = event.clientX - drag.x, dy = event.clientY - drag.y;
    if (!drag.moved && Math.hypot(dx, dy) < 6) return;
    drag.moved = true; position = clampOrb({ x: drag.start.x + dx, y: drag.start.y + dy }, viewport()); layout();
  }, { signal: abort.signal });
  orb.addEventListener('pointerup', event => {
    if (!drag || event.pointerId !== drag.pointer) return;
    const previous = drag; suppressClick = previous.moved; drag = null; previous.releaseIframes(); if (previous.moved) save(); viewportChanged();
    if (orb.hasPointerCapture(event.pointerId)) orb.releasePointerCapture(event.pointerId);
  }, { signal: abort.signal });
  function cancelDrag(): void { if (!drag) return; const previous = drag, pointer = previous.pointer; position = previous.before; drag = null; previous.releaseIframes(); suppressClick = true;
    if (orb.hasPointerCapture(pointer)) orb.releasePointerCapture(pointer); viewportChanged(); }
  const cancelPointerDrag = (event: PointerEvent): void => { if (event.pointerId === drag?.pointer) cancelDrag(); };
  orb.addEventListener('pointercancel', cancelPointerDrag, { signal: abort.signal }); orb.addEventListener('lostpointercapture', cancelPointerDrag, { signal: abort.signal });
  orb.addEventListener('keydown', event => {
    if (event.altKey || event.ctrlKey || event.metaKey || event.isComposing) return;
    if (event.key === 'Escape' && drag) { event.preventDefault(); event.stopPropagation(); cancelDrag(); return; }
    const delta: Record<string, [number, number]> = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
    if (event.key !== 'Home' && !delta[event.key]) return;
    cancelDrag();
    event.preventDefault();
    if (event.key === 'Home') position = null;
    else { const [x, y] = delta[event.key]!; move(x * (event.shiftKey ? 40 : 16), y * (event.shiftKey ? 40 : 16)); return; }
    layout(); save();
  }, { signal: abort.signal });
  view.addEventListener('resize', viewportChanged, { signal: abort.signal });
  view.visualViewport?.addEventListener('resize', viewportChanged, { signal: abort.signal }); view.visualViewport?.addEventListener('scroll', viewportChanged, { signal: abort.signal });
  let layoutFrame: number | null = null;
  const scheduleLayout = (): void => {
    if (disposed || layoutFrame !== null) return;
    if (typeof view.requestAnimationFrame !== 'function') { layout(); return; }
    layoutFrame = view.requestAnimationFrame(() => { layoutFrame = null; if (!disposed) layout(); });
  };
  root.addEventListener('soty:floating-layout', layout, { signal: abort.signal });
  doc.addEventListener('scroll', scheduleLayout, { capture: true, signal: abort.signal });
  doc.addEventListener('focusin', scheduleLayout, { signal: abort.signal });
  const changes = typeof MutationObserver === 'function' ? new MutationObserver(records => {
    if (records.some(record => !orb.contains(record.target))) scheduleLayout();
  }) : null;
  changes?.observe(doc.body, { subtree: true, childList: true, attributes: true, attributeFilter: ['class', 'hidden', 'open', 'style'] });
  layout();
  return {
    setAccount(next) { if (next === account) return; epoch++; clearModal(); resetSession(); account = next;
      const store = storage(); position = store ? readOrbPosition(store, account, viewport()) : null; layout(); },
    setStage(stage, expanded) { if (disposed) return; cancelDrag(); closeDialog(false); expandedStage = expanded ? stage : null; placeOrb(); },
    setModal(next) {
      if (disposed) return;
      const target = next?.ownerDocument === doc && next.isConnected && next.open ? next : null;
      if (target === modal) { placeOrb(); return; }
      cancelDrag(); closeDialog(false); clearModal(); modal = target;
      modal?.classList.add('sw-agent-modal-host'); placeOrb();
    },
    releaseModal(previous) { if (disposed || modal !== previous) return false; cancelDrag(); closeDialog(false); clearModal(); placeOrb(); return true; },
    resetScreen, raise,
    async mountIn(parent) { closeDialog(false); const owner = account, generation = epoch;
      const pending = ensure(parent), target = host; await pending;
      const bound = owner === account && generation === epoch ? handle : null;
      return { dispose: () => { if (target && parent.contains(target)) parking.append(target); },
        flush: async () => { await bound?.flush?.(); }, hasUnsavedChanges: () => false }; },
    async flush() { await mounting; await handle?.flush?.(); },
    hasUnsavedChanges: () => !!handle?.hasUnsavedChanges?.(),
    dispose() { if (disposed) return; clearModal(); resetSession(); disposed = true; epoch++; changes?.disconnect(); if (layoutFrame !== null) view.cancelAnimationFrame(layoutFrame); abort.abort(); orb.remove(); parking.remove(); delete root.dataset.agentOrb; },
  };
}
