// The child begins its SDK after load; the person starts editing before it is ready.
async page => {
  const title = 'Доска с длинным понятным названием проекта', frame = page.frameLocator('iframe.sa-frame');
  await page.getByRole('button', { name: title, exact: true }).click();
  await frame.getByLabel('Текст').fill('Редактирование до готовности SDK');
  await page.waitForFunction(() => document.querySelector('.sa-work-status')?.hidden === true, { timeout: 6000 });
  await page.getByRole('button', { name: 'Закрыть приложение', exact: true }).click();
  await page.getByRole('button', { name: title, exact: true }).waitFor();
  await page.getByRole('button', { name: title, exact: true }).click();
  await frame.getByLabel('Текст').waitFor();
  if (await frame.getByLabel('Текст').inputValue() !== 'Редактирование до готовности SDK') throw Error('late_sdk_draft_lost');
  return { lateSdkDiscoveredWithoutReload: true, draftSaved: true };
}
