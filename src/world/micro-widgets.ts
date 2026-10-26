import './micro-widgets.css';
import { holdIframePointers } from './iframe-pointer-guard';
import { floatingObstacles, placeFloatingControl } from './floating-layout';
import { button, el, iconButton } from './dom';
import type { WorldDialog } from './dialogs';
import type { AppStageHandle } from './app-stage';
import { formatAppLaunchRoute, parseAppLaunchRoute, type AppLaunchIntent } from './app-launch.mjs';
import type { WorldAppRecord, WorldAssistantContext } from './types';
import { editWidgetWorkspace, readWidgetWorkspace, widgetLimit, widgetPosition, widgetRect, widgetStorageKey,
  type WorkspaceWidget, type WidgetWorkspace, type WidgetViewport } from './workspace-widgets.mjs';

export interface MicroWidgetsHandle {
  setAccount(account: string): void;
  pin(app: WorldAppRecord, intent: AppLaunchIntent): void;
  replaceEntry(instanceId: string, intent: AppLaunchIntent): Promise<boolean>;
  clearActive(): void;
  hasActiveWidget(): boolean;
  assistantContext(): WorldAssistantContext | null;
  flush(): Promise<void>;
  hasUnsavedChanges(): boolean;
  hasUnconfirmedWork(): boolean;
  dispose(): void;
}

/** A presentation of existing admitted applications. No URLs or authority are invented here. */
export function mountMicroWidgets(root: HTMLElement, options: {
  mount(host: HTMLElement, app: WorldAppRecord, intent: AppLaunchIntent, current: () => boolean, instanceId: string): AppStageHandle;
  dialog(title: string, onClose: () => void): WorldDialog;
  notify(text: string): void;
  onLayerChange?(): void;
}): MicroWidgetsHandle {
  const doc = root.ownerDocument, view = doc.defaultView!, abort = new AbortController();
  const layer = el('div', 'sm-widget-layer'); layer.dataset.pwaIgnore = '';
  layer.setAttribute('popover', 'manual'); layer.setAttribute('aria-label', 'Личные виджеты');
  const manage = iconButton('Личные виджеты', 'grid', () => openManager()); manage.classList.add('sm-widget-manage');
  layer.append(manage); root.append(layer);
  const mounted = new Map<string, { node: HTMLElement; host: HTMLElement; grip: HTMLElement; stage: AppStageHandle | null; abort: AbortController; isDragging(): boolean; cancelGesture(viewport?: WidgetViewport): void }>();
  let account = '', generation = 0, disposed = false, active = '', state: WidgetWorkspace = { hidden: false, widgets: [] };
  let manager: WorldDialog | null = null, placement: WorldDialog | null = null, volatile = false, layoutRevision = 0;
  const storage = (): Storage | null => { try { return view.localStorage; } catch { return null; } };
  // Only shell chrome is reserved. Application content and field cells remain
  // behind the always-on-top widgets; a widget's own toolbar is not shell chrome.
  const topChrome = '.sw-topbar,.sw-pwa-banner,.sw-inbox-head,.sw-chat-header,.sa-toolbar,.sw-app-toolbar,.uf-screen-head,.uf-local-search,.uf-filters';
  const fieldChrome = '.uf-context-toolbar,.uf-pan-controls';
  const bottomChrome = '.sw-field-controls,.sx-personal-field-controls,.uf-screen-arrange,.uf-undo,.uf-move-tools';
  const chromeElements = (selector: string): Element[] => [...root.querySelectorAll?.(selector) ?? []].filter(element => !layer.contains(element));
  let chromeResize: ResizeObserver | null = null;
  const observedChrome = new Set<Element>();
  const observeChrome = (): void => {
    if (!chromeResize) return;
    const navigation = doc.querySelector('.sw-mobile-nav');
    const next = new Set([...chromeElements(`${topChrome},${fieldChrome},${bottomChrome}`), ...(navigation ? [navigation] : [])]);
    for (const element of observedChrome) if (!next.has(element)) { chromeResize.unobserve(element); observedChrome.delete(element); }
    for (const element of next) if (!observedChrome.has(element)) { chromeResize.observe(element); observedChrome.add(element); }
  };
  const visibleBox = (): WidgetViewport => ({ left: view.visualViewport?.offsetLeft ?? 0, top: view.visualViewport?.offsetTop ?? 0,
    width: view.visualViewport?.width ?? view.innerWidth, height: view.visualViewport?.height ?? view.innerHeight });
  const navigationInset = (box: WidgetViewport): number => {
    const nav = doc.querySelector?.('.sw-mobile-nav')?.getBoundingClientRect();
    return nav && nav.width > 0 && nav.height > 0 && nav.top >= box.top && nav.top < box.top + box.height
      ? Math.max(36, box.top + box.height - nav.top + 8) : 36;
  };
  const viewport = (box = visibleBox()): WidgetViewport => {
    let topInset = 56, bottomInset = navigationInset(box);
    const reserve = (elements: Iterable<Element>, side: 'top' | 'bottom'): void => {
      for (const element of elements) {
        if (element.closest('[hidden],[inert]') || !element.getClientRects().length || view.getComputedStyle(element).visibility === 'hidden') continue;
        const rect = element.getBoundingClientRect();
        if (![rect.left, rect.top, rect.width, rect.height].every(Number.isFinite) || rect.width <= 0 || rect.height <= 0
          || rect.left >= box.left + box.width || rect.left + rect.width <= box.left || rect.top >= box.top + box.height || rect.top + rect.height <= box.top) continue;
        if (side === 'top') topInset = Math.max(topInset, rect.top + rect.height - box.top + 8);
        else bottomInset = Math.max(bottomInset, box.top + box.height - rect.top + 8);
      }
    };
    reserve(chromeElements(topChrome), 'top'); reserve(chromeElements(bottomChrome), 'bottom');
    for (const toolbar of chromeElements(fieldChrome)) {
      const style = view.getComputedStyle(toolbar);
      const side = style.position === 'static' || style.position === 'relative' || style.top !== 'auto' ? 'top' : 'bottom';
      // The responsive toolbar can stretch between top and bottom while its
      // buttons occupy one row. Reserve those controls, not the whole field.
      reserve(toolbar.querySelectorAll('button,select,input'), side);
    }
    return { ...box, topInset, bottomInset };
  };

  function showLayer(): void {
    manage.hidden = !account || !state.widgets.length;
    root.dataset.microWidgets = manage.hidden ? 'hidden' : 'ready';
    if (!account || !state.widgets.length) { if (typeof layer.hidePopover === 'function' && layer.matches(':popover-open')) layer.hidePopover(); options.onLayerChange?.(); return; }
    try { if (typeof layer.showPopover === 'function' && !layer.matches(':popover-open')) layer.showPopover(); }
    catch { /* CSS keeps the layer usable in browsers without the top-layer API. */ }
    options.onLayerChange?.();
  }
  function layout(): void {
    observeChrome();
    const box = visibleBox(), viewBox = viewport(box);
    // The launcher owns its global header slot; route content insets apply to
    // windows only and must not push the launcher down into those windows.
    const point = placeFloatingControl({ x: box.left + box.width - 100, y: box.top + 6 }, { ...box, bottomInset: navigationInset(box) },
      floatingObstacles(doc, element => layer.contains(element) || element.classList.contains('sw-agent-orb')));
    Object.assign(manage.style, { left: `${point.x}px`, top: `${point.y}px` });
    state.widgets.forEach((widget, index) => {
      const row = mounted.get(widget.id); if (!row) return;
      const node = row.node; node.style.zIndex = String(index + 1);
      // Unrelated layout notifications must not erase the owned drag preview.
      if (row.isDragging()) return;
      const rect = widgetRect(widget, viewBox);
      Object.assign(node.style, { left: `${rect.x}px`, top: `${rect.y}px`, width: `${rect.width}px`, height: `${rect.height}px` });
    });
    root.dispatchEvent(new Event('soty:floating-layout'));
  }
  function stop(id: string): void {
    const row = mounted.get(id); if (!row) return;
    row.abort.abort(); row.stage?.dispose(); row.node.remove(); mounted.delete(id); if (active === id) active = '';
  }
  function commit(change: (value: WidgetWorkspace) => void): void {
    layoutRevision++;
    const result = editWidgetWorkspace(storage(), account, value => {
      // A synchronous local edit must not import another tab's unflushed removal.
      const closesMounted = [...mounted.keys()].some(id => value.hidden || !value.widgets.some(widget => widget.id === id && widget.visible));
      if (volatile || closesMounted) { value.hidden = state.hidden; value.widgets = state.widgets.map(widget => ({ ...widget })); }
      change(value);
    });
    state = result.state; volatile = !result.saved;
    if (!result.saved) options.notify('Раскладка работает, но браузер не смог сохранить её на этом устройстве.');
    render();
  }
  function update(id: string, patch: Partial<WorkspaceWidget>): void {
    commit(value => { const row = value.widgets.find(widget => widget.id === id); if (row) Object.assign(row, patch); });
  }
  function bringToFront(id: string): void {
    active = id;
    if (state.widgets.at(-1)?.id !== id) commit(value => {
      const index = value.widgets.findIndex(widget => widget.id === id);
      if (index >= 0) value.widgets.push(value.widgets.splice(index, 1)[0]!);
    });
  }
  async function hide(id: string, remove = false): Promise<void> {
    const row = mounted.get(id), owner = account, epoch = generation;
    const current = (): boolean => !disposed && owner === account && epoch === generation && mounted.get(id) === row;
    try { await row?.stage?.flush(); } catch { if (current()) options.notify('Сохраните незавершённое действие перед скрытием виджета.'); return; }
    if (!current()) return;
    if (row?.stage?.hasUnsavedChanges()) { options.notify('Сохраните незавершённое действие перед скрытием виджета.'); return; }
    commit(value => { if (remove) value.widgets = value.widgets.filter(widget => widget.id !== id);
      else { const widget = value.widgets.find(item => item.id === id); if (widget) widget.visible = false; } });
  }
  async function replaceEntry(instanceId: string, intent: AppLaunchIntent): Promise<boolean> {
    const row = mounted.get(instanceId), previous = state.widgets.find(widget => widget.id === instanceId), owner = account, epoch = generation;
    if (disposed || !owner || !row || !previous?.visible || state.hidden || intent.presentation || intent.takeInvitation) return false;
    let route: string;
    try {
      if (parseAppLaunchRoute(previous.route)?.target.appId !== intent.target.appId) return false;
      route = formatAppLaunchRoute(intent.target, intent.communityId);
    } catch { return false; }
    const current = (): boolean => !disposed && account === owner && generation === epoch && mounted.get(instanceId) === row
      && !state.hidden && state.widgets.some(widget => widget.id === instanceId && widget.visible && widget.route === previous.route);
    try { await row.stage?.flush(); }
    catch { if (current()) options.notify('Сохраните незавершённое действие перед открытием актуальной версии.'); return false; }
    if (!current()) return false;
    if (row.stage?.hasUnsavedChanges()) { options.notify('Сохраните незавершённое действие перед открытием актуальной версии.'); return false; }
    // Another tab may have changed this entry while its storage event is still
    // queued. A stale recovery button cannot overwrite that newer selection.
    const store = storage();
    if (store && !volatile) {
      const stored = readWidgetWorkspace(store, owner), selected = stored.widgets.find(widget => widget.id === instanceId);
      if (stored.hidden || !selected?.visible || selected.route !== previous.route) return false;
    }
    const selected = active === instanceId;
    stop(instanceId); if (selected) active = instanceId;
    // A new stage captures the new location. The old launcher's immutable
    // target/binding and every other live widget remain untouched.
    commit(value => { const widget = value.widgets.find(item => item.id === instanceId); if (widget) widget.route = route; });
    return mounted.has(instanceId);
  }
  function mount(widget: WorkspaceWidget): void {
    const intent = parseAppLaunchRoute(widget.route); if (!intent) return;
    const owner = account, epoch = generation, local = new AbortController();
    const node = el('section', 'sm-widget'); node.dataset.widgetId = widget.id; node.setAttribute('aria-label', `Виджет: ${widget.name}`);
    const header = el('header', 'sm-widget-header');
    const grip = button(widget.name, 'cells', 'sm-widget-grip'); grip.title = 'Положение и размер · нажмите или перетащите';
    grip.setAttribute('aria-label', `Переместить виджет: ${widget.name}`); grip.setAttribute('aria-keyshortcuts', 'ArrowLeft ArrowRight ArrowUp ArrowDown Home');
    const open = iconButton(`Открыть полностью: ${widget.name}`, 'expand', () => { view.location.hash = widget.route; });
    const close = iconButton(`Скрыть виджет: ${widget.name}`, 'close', () => { void hide(widget.id); });
    header.append(grip, open, close);
    const host = el('div', 'sm-widget-content'), resize = iconButton(`Размер виджета: ${widget.name}`, 'expand'); resize.classList.add('sm-widget-resize');
    resize.title = 'Потяните, чтобы изменить размер';
    node.append(header, host, resize); layer.append(node);
    const row = { node, host, grip, stage: null as AppStageHandle | null, abort: local, isDragging: (): boolean => false, cancelGesture: (_viewport?: WidgetViewport): void => {} }; mounted.set(widget.id, row);
    const current = (): boolean => !disposed && account === owner && generation === epoch && mounted.get(widget.id) === row
      && !state.hidden && state.widgets.some(item => item.id === widget.id && item.visible);
    node.addEventListener('pointerdown', () => bringToFront(widget.id), { signal: local.signal });
    node.addEventListener('focusin', () => bringToFront(widget.id), { signal: local.signal });
    let drag: { pointer: number; x: number; y: number; rect: ReturnType<typeof widgetRect>; viewport: WidgetViewport; kind: 'move' | 'size'; moved: boolean; releaseIframes(): void } | null = null;
    row.isDragging = () => drag !== null;
    let suppressMoveClick = false;
    const bindDrag = (handle: HTMLElement, kind: 'move' | 'size'): (() => void) => {
      handle.addEventListener('pointerdown', event => {
        if (drag || !current() || event.button !== 0 || !event.isPrimary) return;
        suppressMoveClick = false;
        const latest = state.widgets.find(item => item.id === widget.id)!, box = viewport();
        drag = { pointer: event.pointerId, x: event.clientX, y: event.clientY, rect: widgetRect(latest, box), viewport: box, kind, moved: false, releaseIframes: holdIframePointers(doc) };
        try { handle.setPointerCapture(event.pointerId); } catch { cancel(); }
      }, { signal: local.signal });
      handle.addEventListener('pointermove', event => {
        if (!drag || drag.pointer !== event.pointerId || !current()) return;
        const dx = event.clientX - drag.x, dy = event.clientY - drag.y; if (!drag.moved && Math.hypot(dx, dy) < 6) return;
        drag.moved = true; const latest = state.widgets.find(item => item.id === widget.id)!, box = viewport();
        const preview = drag.kind === 'move' ? { ...latest, ...widgetPosition({ ...drag.rect, x: drag.rect.x + dx, y: drag.rect.y + dy }, box, latest) }
          : { ...latest, width: Math.max(220, Math.min(640, drag.rect.width + dx)), height: Math.max(160, Math.min(640, drag.rect.height + dy)) };
        const rect = widgetRect(preview, box); Object.assign(node.style, { left: `${rect.x}px`, top: `${rect.y}px`, width: `${rect.width}px`, height: `${rect.height}px` });
      }, { signal: local.signal });
      handle.addEventListener('pointerup', event => {
        if (!drag || drag.pointer !== event.pointerId) return;
        const previous = drag; drag = null; suppressMoveClick = previous.moved; previous.releaseIframes();
        if (current() && previous.moved) {
          const rect = node.getBoundingClientRect();
          const latest = state.widgets.find(item => item.id === widget.id)!;
          update(widget.id, previous.kind === 'move' ? widgetPosition(rect, viewport(), latest) : { width: rect.width, height: rect.height });
        }
        if (handle.hasPointerCapture(event.pointerId)) handle.releasePointerCapture(event.pointerId);
      }, { signal: local.signal });
      const cancel = (): void => { if (!drag || drag.kind !== kind) return; const previous = drag; drag = null; suppressMoveClick = true; previous.releaseIframes(); layout(); if (handle.hasPointerCapture(previous.pointer)) handle.releasePointerCapture(previous.pointer); };
      local.signal.addEventListener('abort', cancel, { once: true });
      const cancelled = (event: PointerEvent): void => { if (event.pointerId === drag?.pointer && drag.kind === kind) cancel(); };
      handle.addEventListener('pointercancel', cancelled, { signal: local.signal }); handle.addEventListener('lostpointercapture', cancelled, { signal: local.signal });
      handle.addEventListener('click', event => {
        if (suppressMoveClick && event.detail > 0) { suppressMoveClick = false; event.preventDefault(); return; }
        suppressMoveClick = false; if (current()) openPlacement(widget.id);
      }, { signal: local.signal });
      handle.addEventListener('keydown', event => {
        if (event.altKey || event.ctrlKey || event.metaKey || event.isComposing) return;
        if (event.key === 'Escape' && drag) { event.preventDefault(); event.stopPropagation(); row.cancelGesture(); return; }
        if (!current()) return;
        const delta: Record<string, [number, number]> = { ArrowLeft: [-1, 0], ArrowRight: [1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] };
        if (event.key === 'Home' && kind === 'move') { row.cancelGesture(); event.preventDefault(); update(widget.id, { x: 0.65, y: 0.12 }); return; }
        if (!delta[event.key]) return;
        row.cancelGesture();
        event.preventDefault(); const latest = state.widgets.find(item => item.id === widget.id)!, [x, y] = delta[event.key]!, step = event.shiftKey ? 40 : 16;
        const rect = widgetRect(latest, viewport());
        update(widget.id, kind === 'move' ? widgetPosition({ ...rect, x: rect.x + x * step, y: rect.y + y * step }, viewport(), latest)
          : { width: Math.max(220, Math.min(640, latest.width + x * step)), height: Math.max(160, Math.min(640, latest.height + y * step)) });
      }, { signal: local.signal });
      return cancel;
    };
    const cancelMove = bindDrag(grip, 'move'), cancelSize = bindDrag(resize, 'size');
    row.cancelGesture = box => {
      // A reflow may clamp the visible rectangle without changing the saved
      // preference. Never persist that temporary rectangle on a late pointerup.
      if (!drag || box && (['left', 'top', 'width', 'height', 'topInset', 'bottomInset'] as const).every(key => drag!.viewport[key] === box[key])) return;
      cancelMove(); cancelSize();
    };
    layout();
    row.stage = options.mount(host, { appId: intent.target.appId, name: widget.name, status: 'unknown' }, intent, current, widget.id);
    row.stage.mountRuntimeControls?.(header,{isCurrent:current,resolve:()=>grip});
  }
  function render(): void {
    for (const id of mounted.keys()) if (state.hidden || !state.widgets.some(widget => widget.id === id && widget.visible)) stop(id);
    for (const widget of state.widgets) if (!state.hidden && widget.visible && !mounted.has(widget.id)) mount(widget);
    showLayer(); layout(); renderManager();
  }
  function openPlacement(id: string): void {
    const owner = account, epoch = generation;
    const item = state.widgets.find(widget => widget.id === id); if (!item) return;
    placement?.close({ restoreFocus: false });
    const editor = options.dialog(`Положение и размер: ${item.name}`, () => { if (placement === editor) placement = null; }); placement = editor;
    const current = (): WorkspaceWidget | undefined => !disposed && owner === account && epoch === generation && editor.element.open
      ? state.widgets.find(widget => widget.id === id) : undefined;
    editor.body.append(el('p', 'sw-muted', 'Перемещайте и меняйте размер кнопками. Приложение остаётся открытым.'));
    const actions = el('div', 'sm-widget-placement');
    const action = (label: string, symbol: string, change: (widget: WorkspaceWidget) => Partial<WorkspaceWidget>): HTMLElement => button(label, symbol, 'sw-button-quiet', () => {
      const widget = current(); if (widget) update(id, change(widget));
    });
    for (const [label, symbol, x, y] of [['Влево', 'back', -1, 0], ['Вверх', 'up', 0, -1], ['Вниз', 'down', 0, 1], ['Вправо', 'arrow', 1, 0]] as const)
      actions.append(action(label, symbol, widget => { const box = viewport(), rect = widgetRect(widget, box); return widgetPosition({ ...rect, x: rect.x + x * 32, y: rect.y + y * 32 }, box, widget); }));
    for (const [label, dimension, amount] of [['Уже', 'width', -40], ['Шире', 'width', 40], ['Высота −', 'height', -40], ['Высота +', 'height', 40]] as const)
      actions.append(action(label, 'expand', widget => ({ [dimension]: Math.max(dimension === 'width' ? 220 : 160, Math.min(640, widget[dimension] + amount)) })));
    actions.append(action('Исходное положение', 'grid', () => ({ x: .65, y: .12, width: 320, height: 260 })));
    editor.body.append(actions, button('Готово', 'check', 'sw-button-primary', () => editor.close()));
  }
  function renderManager(): void {
    if (!manager?.element.open) return;
    manager.body.replaceChildren();
    const all = button(state.hidden ? 'Показать виджеты' : 'Скрыть все виджеты', state.hidden ? 'grid' : 'close', 'sw-button-quiet', () => {
      const owner = account, epoch = generation, rows = [...mounted.entries()], hidden = !state.hidden;
      const current = (): boolean => !disposed && owner === account && epoch === generation
        && mounted.size === rows.length && rows.every(([id, row]) => mounted.get(id) === row);
      void (async () => { await Promise.all(rows.map(([, row]) => row.stage?.flush()));
        if (!current()) return;
        if (rows.some(([, row]) => row.stage?.hasUnsavedChanges())) { options.notify('Сохраните незавершённое действие перед скрытием виджетов.'); return; }
        commit(value => { value.hidden = hidden; }); })().catch(() => { if (current()) options.notify('Не удалось сохранить незавершённое действие.'); });
    });
    manager.body.append(all);
    for (const widget of state.widgets) {
      const row = el('div', 'sm-widget-setting'); row.append(el('strong', '', widget.name),
        button(widget.visible ? 'Скрыть' : 'Показать', undefined, 'sw-button-quiet', () => {
          if (widget.visible) void hide(widget.id); else commit(value => { const item = value.widgets.find(candidate => candidate.id === widget.id); if (item) item.visible = true; value.hidden = false; });
        }), button('Убрать', undefined, 'sw-button-quiet', () => { void hide(widget.id, true); }));
      if (!state.hidden && widget.visible) row.append(button('На передний план', undefined, 'sw-button-quiet', () => {
        const captured = mounted.get(widget.id), owner = account, epoch = generation, dialog = manager;
        if (!captured || !dialog) return;
        bringToFront(widget.id);
        dialog.close({ restoreFocus: false });
        if (!disposed && owner === account && epoch === generation && mounted.get(widget.id) === captured) captured.grip.focus();
      }));
      row.append(button('Положение и размер', 'expand', 'sw-button-quiet', () => openPlacement(widget.id)));
      manager.body.append(row);
    }
    if (!state.widgets.length) manager.body.append(el('p', 'sw-muted', 'Откройте приложение и выберите «Закрепить виджет» в его меню.'));
  }
  function openManager(): void { if (!account || manager?.element.open) return; manager = options.dialog('Личные виджеты', () => { manager = null; }); renderManager(); }
  let resizeFrame: number | null = null;
  const resizeLayout = (): void => {
    if (disposed) return;
    const box = viewport();
    for (const row of mounted.values()) row.cancelGesture(box);
    layout();
    if (resizeFrame !== null) view.cancelAnimationFrame(resizeFrame);
    // Responsive shell navigation may finish appearing after the resize event.
    resizeFrame = view.requestAnimationFrame(() => { resizeFrame = null; if (!disposed) layout(); });
  };
  view.addEventListener('resize', resizeLayout, { signal: abort.signal });
  view.visualViewport?.addEventListener('resize', resizeLayout, { signal: abort.signal }); view.visualViewport?.addEventListener('scroll', resizeLayout, { signal: abort.signal });
  root.addEventListener('scroll', resizeLayout, { signal: abort.signal, capture: true });
  // Routes replace headers; browser reflow, keyboard and font loading can resize
  // existing ones. Rebind the bounded observer on layout, then discard old nodes.
  chromeResize = typeof ResizeObserver === 'function' ? new ResizeObserver(resizeLayout) : null;
  const changes = typeof MutationObserver === 'function' ? new MutationObserver(records => {
    if (records.some(record => !layer.contains(record.target) && !(record.target instanceof Element && record.target.classList.contains('sw-agent-orb')))) resizeLayout();
  }) : null;
  changes?.observe(root, { subtree: true, childList: true, attributes: true, attributeFilter: ['class', 'hidden', 'open', 'style'] });
  view.addEventListener('blur', () => {
    for (const row of mounted.values()) row.cancelGesture();
    queueMicrotask(() => {
    if (disposed) return;
    for (const [id, row] of mounted) if (row.host.contains(doc.activeElement)) { active = id; return; }
    if (doc.activeElement?.tagName === 'IFRAME') active = '';
  }); }, { signal: abort.signal });
  view.addEventListener('storage', event => {
    if (volatile || !account || (event.key !== null && event.key !== widgetStorageKey(account))) return;
    const owner = account, epoch = generation, revision = ++layoutRevision, rows = [...mounted.entries()];
    const next = readWidgetWorkspace(storage(), account);
    const current = (): boolean => !disposed && owner === account && epoch === generation && revision === layoutRevision
      && mounted.size === rows.length && rows.every(([id, row]) => mounted.get(id) === row);
    const postpone = (): void => {
      if (!current()) return;
      volatile = true;
      options.notify('Раскладка из другой вкладки не применена: сохраните незавершённое действие. Текущие виджеты остаются открыты.');
    };
    void (async () => {
      await Promise.all(rows.map(([, row]) => row.stage?.flush()));
      if (!current()) return;
      if (rows.some(([, row]) => row.stage?.hasUnsavedChanges())) { postpone(); return; }
      state = next; render();
    })().catch(postpone);
  }, { signal: abort.signal });
  doc.addEventListener('fullscreenchange', () => { if (typeof layer.hidePopover === 'function' && layer.matches(':popover-open')) layer.hidePopover(); showLayer(); }, { signal: abort.signal });
  return {
    setAccount(next) { if (next === account) return; generation++; layoutRevision++; manager?.close({ restoreFocus: false }); placement?.close({ restoreFocus: false }); for (const id of mounted.keys()) stop(id);
      account = next; active = ''; volatile = false; state = readWidgetWorkspace(storage(), next); render(); },
    pin(app, intent) {
      if (!account || disposed) return;
      const route = formatAppLaunchRoute(intent.target, intent.communityId), existing = state.widgets.find(widget => widget.route === route);
      if (!existing && state.widgets.length >= widgetLimit) { options.notify(`Можно закрепить до ${widgetLimit} виджетов. Уберите ненужный в меню виджетов.`); return; }
      commit(value => { value.hidden = false; const found = value.widgets.find(widget => widget.route === route);
        if (found) found.visible = true;
        else value.widgets.push({ id: `widget-${crypto.randomUUID()}`, route, name: app.name.slice(0, 100) || 'Приложение', visible: true,
          x: Math.max(0, 0.65 - value.widgets.length * 0.08), y: Math.min(0.75, 0.12 + value.widgets.length * 0.06), width: 320, height: 260 }); });
    },
    replaceEntry,
    clearActive() { active = ''; },
    hasActiveWidget: () => mounted.has(active),
    assistantContext: () => mounted.get(active)?.stage?.assistantContext() ?? null,
    async flush() { await Promise.all([...mounted.values()].map(row => row.stage?.flush())); },
    hasUnsavedChanges: () => [...mounted.values()].some(row => row.stage?.hasUnsavedChanges()),
    hasUnconfirmedWork: () => [...mounted.values()].some(row => row.stage?.hasUnconfirmedWork?.()),
    dispose() { if (disposed) return; disposed = true; generation++; chromeResize?.disconnect(); observedChrome.clear(); changes?.disconnect(); if (resizeFrame !== null) view.cancelAnimationFrame(resizeFrame); manager?.close({ restoreFocus: false }); placement?.close({ restoreFocus: false }); for (const id of mounted.keys()) stop(id); abort.abort(); layer.remove(); delete root.dataset.microWidgets; },
  };
}
