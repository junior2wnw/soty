export function showOriginContinuity(): void {
  if (location.hostname !== '4-2.xn--p1ai') return;
  try { if (localStorage.getItem('soty.origin-continuity.dismissed') === '1') return; } catch { /* Storage can be disabled. */ }
  const notice = document.createElement('aside');
  notice.setAttribute('aria-label', 'Прежние Соты');
  notice.style.cssText = 'position:fixed;z-index:10000;bottom:18px;left:50%;transform:translateX(-50%);width:min(420px,calc(100vw - 28px));box-sizing:border-box;padding:14px 16px;border-radius:16px;border:1px solid #d8ded4;background:#fafcf8;color:#253023;box-shadow:0 8px 32px #17220e1c;font:14px/1.45 system-ui,sans-serif';
  const title = document.createElement('strong'); title.textContent = 'Соты теперь на этом адресе';
  const text = document.createElement('p'); text.style.margin = '6px 0 12px';
  text.textContent = 'Прежние аккаунты, проекты и локальные черновики доступны через сохранённые входы.';
  const link = document.createElement('a'); link.href = 'https://xn--n1afe0b.online/__soty';
  link.textContent = 'Прежние Соты'; link.style.cssText = 'color:#24552d;font-weight:700';
  const hive = document.createElement('a'); hive.href = '/__hive'; hive.textContent = 'Прежний HIVE';
  hive.style.cssText = 'color:#24552d;font-weight:700;margin-left:18px';
  const close = document.createElement('button'); close.type = 'button'; close.textContent = 'Понятно';
  close.style.cssText = 'float:right;border:0;background:transparent;color:#53604e;cursor:pointer;padding:0 0 0 12px;font:inherit';
  close.addEventListener('click', () => {
    try { localStorage.setItem('soty.origin-continuity.dismissed', '1'); } catch { /* No application data is modified. */ }
    notice.remove();
  });
  notice.append(title, text, link, hive, close); document.body.append(notice);
}
