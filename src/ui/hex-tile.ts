import '../geometry/dom';

export interface HexTileOptions {
  label: string;
  status?: string;
  visual?: Node;
  radius?: number;
  className?: string;
  onSelect?: () => void;
}

/** A real button with an independent hex surface and an explicit interior content rectangle. */
export function createHexTile({ label, status, visual, radius = 72, className = '', onSelect }: HexTileOptions): HTMLButtonElement {
  const tile = document.createElement('button');
  tile.type = 'button'; tile.className = `soty-hex soty-hex-shell ${className}`.trim();
  tile.style.setProperty('--hex-width', `${radius * 2}px`);
  tile.setAttribute('aria-label', [label, status].filter(Boolean).join(', '));
  tile.title = [label, status].filter(Boolean).join(' · ');
  const safe = document.createElement('span'); safe.className = 'soty-hex-safe';
  if (visual) {
    const frame = document.createElement('span'); frame.className = 'soty-hex-visual';
    frame.append(visual); safe.append(frame);
  }
  const name = document.createElement('strong'); name.className = 'soty-hex-label'; name.textContent = label; safe.append(name);
  if (status) { const detail = document.createElement('small'); detail.className = 'soty-hex-status'; detail.textContent = status; safe.append(detail); }
  tile.append(safe);
  if (onSelect) tile.addEventListener('click', onSelect);
  return tile;
}
