import './entry.css';
import './geometry/dom';
import { loadPreferences } from './world/preferences';
import { applyThemePreferences } from './world/theme/theme';
import './platform/connect-theme.css';
import { getPwaController } from './platform/pwa';

applyThemePreferences(loadPreferences());
getPwaController();

const root = document.querySelector<HTMLElement>('#app')!;
const url = new URL(window.location.href);
const consent = /^\/oauth\/interaction\/([A-Za-z0-9_-]{16,128})$/u.exec(url.pathname);
const legacyParameters = ['j', 'connector', 'link', 'agent', 'agentRelay', 'agentRelayId', 'reset-local', 'soty-reset', 'repair', 'traffic'];
const classic = url.searchParams.get('view') === 'classic'
  || legacyParameters.some(name => url.searchParams.has(name))
  || url.pathname.startsWith('/install/');
document.body.dataset.sotySurface = classic ? 'classic' : 'world';

async function start(): Promise<void> {
  if (consent) {
    const { startOAuthConsent } = await import('./platform/oauth-consent');
    await startOAuthConsent(root, consent[1]!);
    return;
  }
  if (classic) {
    await import('./main');
    return;
  }
  const loading = document.createElement('div');
  loading.className = 'soty-start';
  loading.setAttribute('role', 'status');
  const mark = document.createElement('span');
  mark.className = 'soty-start-mark'; mark.setAttribute('aria-hidden', 'true');
  const label = document.createElement('span'); label.textContent = 'Собираем ваши соты';
  loading.append(mark, label); root.replaceChildren(loading);
  const { startWorld } = await import('./platform/world-adapter');
  await startWorld(root);
}

void start().catch(() => {
  const failure = document.createElement('div'); failure.className = 'soty-start';
  const title = document.createElement('h1'); title.textContent = 'Не удалось открыть Соты';
  const text = document.createElement('p'); text.textContent = 'Проверьте соединение. Ваши данные сохранены.';
  const retry = document.createElement('button'); retry.textContent = 'Повторить';
  retry.addEventListener('click', () => window.location.reload());
  failure.append(title, text, retry); root.replaceChildren(failure);
});
