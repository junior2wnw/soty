import { useEffect, useRef, type ReactNode } from 'react';
import { X } from 'lucide-react';

export default function Modal({
  title,
  subtitle,
  children,
  onClose,
  wide = false,
  compact = false,
}: {
  title: string;
  subtitle?: string;
  children: ReactNode;
  onClose: () => void;
  wide?: boolean;
  compact?: boolean;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const previousFocus = useRef<HTMLElement | null>(
    document.activeElement?.closest('.minimal-menu')
      ? document.getElementById('timeline-menu-trigger')
      : (document.activeElement as HTMLElement | null),
  );
  useEffect(() => {
    const dialog = ref.current;
    const focusable = () =>
      [
        ...(dialog?.querySelectorAll<HTMLElement>(
          'button:not([disabled]), input:not([disabled]):not([type="hidden"]), select:not([disabled]), textarea:not([disabled]), a[href], [tabindex="0"]',
        ) ?? []),
      ].filter(
        (element) =>
          element.getClientRects().length > 0 && getComputedStyle(element).visibility !== 'hidden',
      );
    if (!dialog?.contains(document.activeElement))
      (dialog?.querySelector<HTMLElement>('[autofocus]') ?? focusable()[0] ?? dialog)?.focus();
    const handle = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.stopPropagation();
        onClose();
      }
      if (event.key === 'Tab') {
        const elements = focusable();
        const first = elements[0];
        const last = elements.at(-1);
        if (event.shiftKey && document.activeElement === first) {
          event.preventDefault();
          last?.focus();
        } else if (!event.shiftKey && document.activeElement === last) {
          event.preventDefault();
          first?.focus();
        }
      }
    };
    document.addEventListener('keydown', handle);
    document.body.classList.add('modal-open');
    return () => {
      document.removeEventListener('keydown', handle);
      document.body.classList.remove('modal-open');
      const previous = previousFocus.current;
      if (previous?.isConnected) previous.focus();
      else document.querySelector<HTMLElement>('.minimal-add')?.focus();
    };
  }, [onClose]);
  return (
    <div
      className="modal-backdrop"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={ref}
        className={`modal ${wide ? 'wide' : ''} ${compact ? 'compact' : ''}`}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        tabIndex={-1}
      >
        <header className="modal-heading">
          <div>
            <h2 className={compact ? 'sr-only' : undefined}>{title}</h2>
            {subtitle && <p className="muted">{subtitle}</p>}
          </div>
          <button className="icon-button" onClick={onClose} aria-label="Закрыть">
            <X size={20} />
          </button>
        </header>
        {children}
      </div>
    </div>
  );
}
