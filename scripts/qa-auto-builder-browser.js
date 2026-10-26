// Run in a dedicated Playwright CLI session against the Vite UI fixture on
// 127.0.0.1:5313 and the local app fixture on 127.0.0.1:5324. No real API.
async page => {
  const base = 'http://127.0.0.1:5313';
  const results = [];
  for (const path of ['app-create', 'app-builder-open', 'app-builder-auto', 'app-builder-tabs']) {
    await page.goto(`${base}/src/platform/${path}.test.html`);
    await page.locator('#run').click();
    await page.waitForFunction(() => ['passed', 'failed'].includes(document.querySelector('#proof')?.dataset.testStatus), null, { timeout: 20_000 });
    const proof = await page.locator('#proof').evaluate(node => ({ status: node.dataset.testStatus, text: node.textContent }));
    if (proof.status !== 'passed') throw new Error(`${path}: ${proof.text}`);
    results.push(proof.text);
  }
  await page.goto(`${base}/src/world/ui-fixture.html?layout=one-field`);
  // This is the fixture's synthetic account, in this isolated browser session.
  await page.evaluate(() => localStorage.removeItem('soty.app-create.v1:qa-onefield-anna'));
  await page.reload();
  await page.getByRole('button', { name: 'Создать', exact: true }).click();
  await page.getByRole('button', { name: 'Создать с ИИ' }).click();
  await page.getByRole('textbox', { name: 'Идея приложения' }).fill('Личный список покупок');
  await page.getByRole('region', { name: 'Задача помощника' }).getByRole('button', { name: 'Создать', exact: true }).click();
  await page.frameLocator('iframe').getByRole('heading', { name: 'Покупки' }).waitFor({ timeout: 15_000 });
  const counts = await page.evaluate(() => window.qaAutoBuilder());
  if (!counts.automaticJob || counts.browserRegistrationCalls !== 0 || !page.url().includes('#app/app-1234567890abcdef1234567890abcdef'))
    throw new Error('Wrong server receipt, stage, or browser registration');
  await page.screenshot({ path: '.playwright-cli/auto-builder-stage.png', fullPage: true });
  return [...results, 'PASS: synthetic server receipt -> real app stage -> visible iframe; zero browser registrations'];
}
