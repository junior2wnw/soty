// Run with pinned playwright-cli in the isolated fixture browser. All login,
// permissions and writes use real UI/HTTP; no cookie/session injection.
async (page) => {
  const checks = {}, check = (value, name) => { if (!value) throw new Error('ordinary_browser_' + name); checks[name] = true; };
  const app = () => page.frameLocator('iframe');
  async function helperButton(name) { await page.bringToFront(); await page.getByRole('button', { name, exact: true }).click();
    await page.waitForFunction(() => document.querySelector('#result')?.dataset.status !== 'pending');
    check(await page.locator('#result').getAttribute('data-status') !== 'fail', 'fixtureAction'); }
  async function login() {
    await app().getByRole('button', { name: 'Войти через Соты', exact: true }).waitFor();
    await page.evaluate(() => { window.__ordinaryFrame = document.querySelector('iframe'); });
    const opened = page.context().waitForEvent('page'); await app().getByRole('button', { name: 'Войти через Соты', exact: true }).click(); const popup = await opened;
    await popup.getByRole('checkbox', { name: 'Подключить этот выбранный ресурс с моими текущими правами' }).waitFor();
    const nativeOrigin = await popup.evaluate(() => location.origin);
    await popup.getByRole('checkbox', { name: 'Подключить этот выбранный ресурс с моими текущими правами' }).check();
    const observed = popup.waitForRequest(request => request.method() === 'POST' && request.url().split('?')[0].endsWith('/soty/authorize'));
    await popup.getByRole('button', { name: 'Войти через Соты', exact: true }).click();
    const request = await observed, headers = await request.allHeaders(); check(headers.origin === nativeOrigin, 'nativeExactOrigin');
    check(!headers.referer || headers.referer === nativeOrigin + '/', 'nativeRefererHasNoIntent');
    await popup.getByRole('button', { name: 'Войти', exact: true }).click({ timeout: 20000 });
    await popup.getByRole('heading', { name: 'Вход подтверждён', exact: true }).waitFor({ timeout: 20000 });
    await popup.getByRole('link', { name: 'Завершить подключение', exact: true }).click();
    await popup.getByRole('heading', { name: 'Приложение подключено', exact: true }).waitFor({ timeout: 15000 });
    await app().getByRole('textbox', { name: 'Название записи', exact: true }).waitFor({ timeout: 20000 });
    check(await page.evaluate(() => document.querySelector('iframe') === window.__ordinaryFrame), 'sameIframeAfterLogin');
    await popup.close(); await page.bringToFront();
  }
  async function create(title) { await app().getByRole('textbox', { name: 'Название записи', exact: true }).fill(title);
    await app().getByRole('button', { name: 'Добавить запись', exact: true }).click(); await app().getByText(title, { exact: true }).waitFor(); }
  await helperButton('Открыть новое приложение A'); await login(); await create('Synthetic browser A item');
  await app().getByRole('button', { name: 'Сообщить проблему', exact: true }).click();
  await app().getByRole('textbox', { name: 'Что произошло?', exact: true }).fill('Synthetic private browser feedback');
  await app().getByRole('button', { name: 'Отправить', exact: true }).click(); await app().getByText('Обращение получено', { exact: true }).waitFor(); checks.realPrivateFeedback = true;
  await helperButton('Открыть новое приложение B'); await login(); await create('Synthetic browser B item');
  await helperButton('Перезапустить приложения');
  const frame = await (await page.locator('iframe').elementHandle()).contentFrame();
  check(await frame.evaluate(async () => { const response = await fetch('/api/embed/query', { method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ requestId: 'browser-read-restart-0001', input: { operation: 'items.list' } }) }); const body = await response.json(); return response.status === 200 && body.data.items.length === 1; }), 'sourceRestartRetained');
  check(await frame.evaluate(async () => (await fetch('/api/embed/private-admin')).status === 403), 'privateRouteDenied');
  await page.setViewportSize({ width: 390, height: 844 });
  check(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), 'parentMobileBounds');
  check(await frame.evaluate(() => document.documentElement.scrollWidth <= innerWidth && [...document.querySelectorAll('button')].filter(button => {
    const box = button.getBoundingClientRect(); return box.width > 0 && box.height > 0 && (box.width < 43.5 || box.height < 43.5); }).length === 0), 'sourceMobileBounds');
  await page.screenshot({ path: 'output/playwright/source-app-ordinary-mobile.png', scale: 'css' });
  const proof = await page.evaluate(async () => (await fetch('/__fixture/proof')).json());
  check(proof.realms.every(realm => realm.format === 2 && realm.principals === 1 && realm.links === 1 && realm.consents === 1 && realm.items === 1), 'twoIndependentNewEmptyRealms');
  check(proof.realms[0].tickets === 1 && proof.realms[1].tickets === 0, 'feedbackRealmIsolation');
  check(proof.sourceCookiesInjected === false && proof.humanCookiesInjected === false, 'noSessionInjection');
  await helperButton('Отозвать доступ в приложениях');
  check(await frame.evaluate(async () => (await fetch('/api/embed/context')).status === 403), 'freshNativeRevoke');
  return { synthetic: true, productionHttpsValidated: false, newEmptyGuestPolicyOnly: true, basic300NoResume: true, checks };
}
