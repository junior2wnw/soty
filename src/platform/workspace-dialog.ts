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
  if (initialFocus) dialog.querySelector<HTMLElement>(initialFocus)?.focus();
}
