import { useEffect, useId, useRef } from 'react';
import type { ReactNode } from 'react';
import { X } from 'lucide-react';

export default function Dialog({ title, children, onClose, compact = false, className = '' }: { title: string; children: ReactNode; onClose: () => void; compact?: boolean; className?: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const titleId = useId();
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  const closeWhenIdle = () => { if (!ref.current?.querySelector('[aria-busy="true"]')) closeRef.current(); };

  useEffect(() => {
    const previous = document.activeElement as HTMLElement | null;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    const first = ref.current?.querySelector<HTMLElement>('[data-autofocus]') || ref.current?.querySelector<HTMLElement>('button:not([disabled]), input:not([disabled]), [tabindex="0"]');
    first?.focus({ preventScroll: true });
    const background = Array.from(document.querySelectorAll<HTMLElement>('.topbar, main, .loading-dashboard'));
    const previousInert = background.map((element) => element.inert);
    background.forEach((element) => { element.inert = true; });
    function keydown(event: KeyboardEvent) {
      if (event.key === 'Escape') { event.preventDefault(); closeWhenIdle(); }
      if (event.key !== 'Tab') return;
      const elements = Array.from(ref.current?.querySelectorAll<HTMLElement>('button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), a[href], [tabindex="0"]') || [])
        .filter((element) => element.getClientRects().length > 0);
      const firstElement = elements[0];
      const lastElement = elements[elements.length - 1];
      if (!firstElement) { event.preventDefault(); return; }
      if (event.shiftKey && document.activeElement === firstElement) { event.preventDefault(); lastElement.focus(); }
      else if (!event.shiftKey && document.activeElement === lastElement) { event.preventDefault(); firstElement.focus(); }
    }
    document.addEventListener('keydown', keydown);
    return () => {
      document.body.style.overflow = previousOverflow;
      document.removeEventListener('keydown', keydown);
      background.forEach((element, index) => { element.inert = previousInert[index]; });
      previous?.focus({ preventScroll: true });
    };
  }, [title]);

  return <div className={`dialog-layer ${compact ? 'compact' : ''}`} onMouseDown={(event) => { if (event.target === event.currentTarget) closeWhenIdle(); }}>
    <div ref={ref} className={`dialog ${className}`} role="dialog" aria-modal="true" aria-labelledby={titleId}>
      <div className="sheet-handle" aria-hidden="true" />
      <div className="dialog-heading"><h2 id={titleId}>{title}</h2><button className="icon-button dialog-close" onClick={closeWhenIdle} aria-label="Закрыть"><X size={26} strokeWidth={1.8} /></button></div>
      <div className="dialog-body">{children}</div>
    </div>
  </div>;
}
