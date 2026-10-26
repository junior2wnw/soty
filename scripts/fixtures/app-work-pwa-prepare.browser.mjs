// Run before changing the synthetic preview worker; use a fresh local browser profile.
async page => {
  const supported = page.url().includes('appOrigin='), frame = page.frameLocator('iframe.sa-frame');
  await page.getByRole('button', { name: 'Доска с длинным понятным названием проекта', exact: true }).click();
  if (supported) { await frame.getByLabel('Текст').fill('Тест PWA'); await frame.getByLabel('Состояние сохранения').selectOption('error'); }
  else await frame.getByLabel('Состояние приложения').fill('Тест PWA без SDK');
  return page.evaluate(async () => { const registration = await navigator.serviceWorker.ready;
    if (!registration.active || !navigator.serviceWorker.controller) throw Error('fixture_worker_not_active'); return { active: true }; });
}
