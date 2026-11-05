import './workspace-dialog.css';

/** Native top-layer focus containment and background inertness for tool panels. */
export function createWorkspaceDialog(className: string, label: string, onCancel?: () => void): HTMLDialogElement {
  const previous = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  const dialog = document.createElement('dialog');
  dialog.className = `${className} workspace-dialog`;
  dialog.setAttribute('aria-label', label);
  if (onCancel) dialog.addEventListener('cancel', event => { event.preventDefault(); onCancel(); });
  dialog.addEventListener('close', () => {
    dialog.remove();
    if (previous?.isConnected) previous.focus();
  }, { once: true });
  return dialog;
}

export function showWorkspaceDialog(dialog: HTMLDialogElement, initialFocus?: string): void {
  document.body.append(dialog);
  dialog.showModal();
  if (dialog.ownerDocument?.defaultView?.HTMLCanvasElement) {
    let material: { dispose(): void } | null = null;
    dialog.addEventListener('close', () => { material?.dispose(); material = null; }, { once: true });
    // Keep the initial shell small; a late import cannot revive a closed portal.
    void import('../world/shell-glass').then(({ mountDialogGlass }) => {
      if (dialog.open && dialog.isConnected) material = mountDialogGlass(dialog);
    });
  }
  if (initialFocus) dialog.querySelector<HTMLElement>(initialFocus)?.focus();
}
