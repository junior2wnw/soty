import './entry.css';
import './geometry/dom';
import { loadPreferences } from './world/preferences';
import { applyThemePreferences } from './world/theme/theme';
import './platform/connect-theme.css';
import { getPwaController } from './platform/pwa';
import { recordStartupTiming } from './platform/startup-timing';

applyThemePreferences(loadPreferences());
getPwaController();

const root = document.querySelector<HTMLElement>('#app')!;
root.dataset.startupEntryAt = String(Date.now());
recordStartupTiming(root);
window.addEventListener('load', () => { window.setTimeout(() => recordStartupTiming(root), 0); }, { once: true });
const url = new URL(window.location.href);
const consent = /^\/oauth\/interaction\/([A-Za-z0-9_-]{16,128})$/u.exec(url.pathname);
const humanLogin = /^\/human-identity\/interaction\/([A-Za-z0-9_-]{16,128})$/u.exec(url.pathname);
const legacyParameters = ['j', 'connector', 'link', 'agent', 'agentRelay', 'agentRelayId', 'reset-local', 'soty-reset', 'repair', 'traffic', 'trafficProfile'];
const retiredPairing = /^\/install\/spreadex\/?$/u.test(url.pathname);
const classic = url.searchParams.get('view') === 'classic'
  || legacyParameters.some(name => url.searchParams.has(name))
  || url.pathname.startsWith('/install/');
document.body.dataset.sotySurface = classic && !retiredPairing ? 'classic' : 'world';

async function start(): Promise<void> {
  if (humanLogin) {
    const { startHumanLogin } = await import('./platform/human-login');
    await startHumanLogin(root, humanLogin[1]!);
    return;
  }
  if (consent) {
    const { startOAuthConsent } = await import('./platform/oauth-consent');
    await startOAuthConsent(root, consent[1]!);
    return;
  }
  if (retiredPairing) {
    if (url.searchParams.has('pair')) {
      url.searchParams.delete('pair');
      window.history.replaceState(window.history.state, '', `${url.pathname}${url.search}${url.hash}`);
    }
    const notice = document.createElement('div'); notice.className = 'soty-start';
    const title = document.createElement('h1'); title.textContent = 'Этот способ подключения больше недоступен';
    const text = document.createElement('p'); text.textContent = 'Откройте Соты, чтобы продолжить.';
    const link = document.createElement('a'); link.href = location.hostname === 'xn--n1afe0b.online' ? '/__soty' : '/'; link.textContent = 'Открыть Соты';
    notice.append(title, text, link); root.replaceChildren(notice);
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
  const failure = document.createElement('div'); failure.className = 'soty-start'; failure.setAttribute('role', 'alert');
  const title = document.createElement('h1'); title.textContent = 'Не удалось открыть Соты';
  const text = document.createElement('p'); text.textContent = 'Запуск прервался. Состояние профиля и сохранения пока не проверено. Незавершённую работу в других вкладках сохраните перед повторным открытием.';
  const retry = document.createElement('button'); retry.textContent = 'Повторить';
  retry.addEventListener('click', () => window.location.reload());
  failure.append(title, text, retry); root.replaceChildren(failure);
});
