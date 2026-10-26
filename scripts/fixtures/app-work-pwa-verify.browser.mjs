// Run after appending a comment to the synthetic preview worker, never to production.
async page => {
  const supported = page.url().includes('appOrigin='), frame = page.frameLocator('iframe.sa-frame');
  await page.evaluate(async () => { const registration = await navigator.serviceWorker.getRegistration(); await registration.update(); });
  await page.getByRole('button', { name: 'Обновить', exact: true }).waitFor({ timeout: 12000 });
  await page.getByRole('button', { name: 'Обновить', exact: true }).click();
  if (supported) {
    await page.getByText('Изменения ещё не сохранены.', { exact: false }).first().waitFor();
    if (await frame.getByLabel('Текст').inputValue() !== 'Тест PWA') throw Error('compatible_draft_lost_on_failure');
    if (!await page.evaluate(async () => !!(await navigator.serviceWorker.getRegistration()).waiting)) throw Error('worker_activated_on_failure');
    await frame.getByLabel('Состояние сохранения').selectOption('ok');
    await Promise.all([page.waitForEvent('load', { timeout: 15000 }), page.getByRole('button', { name: 'Обновить', exact: true }).click()]);
    await frame.getByLabel('Текст').waitFor();
    if (await frame.getByLabel('Текст').inputValue() !== 'Тест PWA') throw Error('compatible_draft_lost_after_update');
  } else {
    await page.getByRole('button', { name: 'Повторить', exact: true }).waitFor();
    if (!await page.getByText('Сохраните данные приложения в этом окне и закройте его или скройте виджет. Затем нажмите «Повторить».', { exact: true }).isVisible()) throw Error('legacy_guidance_missing');
    if (await frame.getByLabel('Состояние приложения').inputValue() !== 'Тест PWA без SDK') throw Error('legacy_draft_lost_on_update');
    if (!await page.evaluate(async () => !!(await navigator.serviceWorker.getRegistration()).waiting)) throw Error('worker_activated_with_legacy');
    // The test reads out its synthetic text before closing, as the instruction asks a person to save their own work.
    await page.getByRole('button', { name: 'Закрыть приложение', exact: true }).click();
    await page.getByRole('button', { name: 'Доска с длинным понятным названием проекта', exact: true }).waitFor();
    await Promise.all([page.waitForEvent('load', { timeout: 15000 }), page.getByRole('button', { name: 'Повторить', exact: true }).click()]);
  }
  return { supported, updateBlockedUntilSafe: true, updated: true };
}
