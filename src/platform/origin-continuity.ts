/** Retained origins belong in account settings, never over the active field. */
export function createOriginContinuityLinks(): HTMLDetailsElement | null {
  if (location.hostname !== '4-2.xn--p1ai') return null;
  const section = document.createElement('details'); section.className = 'sw-origin-links';
  const heading = document.createElement('summary'); heading.textContent = 'Прежние входы';
  const text = document.createElement('p'); text.className = 'sw-small-note';
  text.textContent = 'Прежние аккаунты, проекты и локальные черновики доступны по этим адресам.';
  const link = document.createElement('a'); link.href = 'https://xn--n1afe0b.online/__soty';
  link.className = 'sw-button sw-button-wide sw-button-quiet'; link.textContent = 'Прежние Соты';
  const hive = document.createElement('a'); hive.href = '/__hive';
  hive.className = 'sw-button sw-button-wide sw-button-quiet'; hive.textContent = 'Прежний HIVE';
  section.append(heading, text, link, hive); return section;
}
