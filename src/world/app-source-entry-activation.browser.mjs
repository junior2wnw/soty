// Run only against the owned app-source.test.html development fixture.
async page => {
  const ensure = (value, code) => { if (!value) throw Error(code); }, url = page.url();
  ensure(/^http:\/\/(?:localhost|127\.0\.0\.1):\d+\/src\/world\/app-source\.test\.html(?:\?.*)?$/u.test(url), 'own_source_fixture_required');
  const key = value => page.locator('[data-settings-key="' + value + '"]');
  const open = async mode => {
    await page.locator('#qa-mode').selectOption(mode);
    await page.getByRole('button', { name: 'Сбросить только стенд', exact: true }).click();
    await page.getByRole('button', { name: 'Открыть', exact: true }).click();
    await page.getByLabel('Подключение', { exact: true }).selectOption('soty.selected-human-embed.v2');
    await page.getByLabel('Порт проекта', { exact: true }).fill('5321');
  };
  const leaveDraft = async () => {
    await page.getByRole('button', { name: 'Закрыть', exact: true }).click();
    await page.getByRole('button', { name: 'Выйти без сохранения', exact: true }).click();
  };
  const rows = [];
  for (const width of [320, 390, 1024]) {
    const sourceOrigin = width === 320 ? 'https://source-entry-with-a-long-owned-name-for-the-working-space.apps.fixture.invalid' : 'https://source.apps.fixture.invalid';
    await page.setViewportSize({ width, height: 844 }); await page.goto(url.split('?')[0] + (width === 320 ? '?long=1' : '')); await open('atomic');
    const alias = page.getByLabel('Адрес входа Сот', { exact: true });
    ensure(await alias.inputValue() === 'dom_' + 'c3'.repeat(16), 'sole_bound_inactive_alias_not_prefilled');
    ensure(await page.locator('#qa-results').getAttribute('data-source-alias-active') === 'false', 'alias_open_before_check');
    await page.getByRole('button', { name: 'Проверить', exact: true }).click();
    const review = page.getByText('При переключении ' + sourceOrigin + ' включится для защищённого входа Сот. Сайт https://demo.apps.fixture.invalid сохранится.', { exact: true });
    await review.waitFor();
    ensure(await page.locator('#qa-results').getAttribute('data-source-alias-active') === 'false', 'private_check_activated_alias');
    ensure(await page.locator('.sw-app-settings-source-form input[type="checkbox"]').count() === 2, 'duplicate_activation_ack');
    await key('source-public-ack').check();
    ensure(await key('source-promote').getAttribute('aria-disabled') === 'true', 'source_ack_borrowed_native_ack');
    await key('source-native-ack').check();
    ensure(await key('source-promote').getAttribute('aria-disabled') === 'false', 'independent_acks_cannot_promote');
    const geometry = await page.locator('.sw-app-settings-source-form').evaluate(form => {
      const rect = form.closest('dialog').getBoundingClientRect();
      const controls = [...form.querySelectorAll('input,select,button')].filter(node => node.getClientRects().length && (() => {
        const r = node.getBoundingClientRect(); return r.left < rect.left - 1 || r.right > rect.right + 1;
      })()).length;
      const text = [...form.querySelectorAll('.sw-app-settings-source-target,.sw-app-settings-ack span')].filter(node => node.getClientRects().length && node.scrollWidth > node.clientWidth + 1).length;
      const dialog = form.closest('dialog'); return { controls, text, dialogOverflow: dialog.scrollWidth > dialog.clientWidth + 1 };
    }); ensure(geometry.controls === 0 && geometry.text === 0 && !geometry.dialogOverflow, 'activation_form_horizontal_overflow');
    await page.getByLabel('Адрес входа Сот', { exact: true }).scrollIntoViewIfNeeded();
    await page.screenshot({ path: 'output/playwright/source-activation-' + width + '.png' });
    if (width === 320) { await review.scrollIntoViewIfNeeded(); await page.screenshot({ path: 'output/playwright/source-activation-review-320.png' }); }
    await key('source-promote').click(); await key('pending-retry').waitFor({ state: 'visible' });
    ensure(await page.locator('#qa-results').getAttribute('data-source-alias-active') === 'true', 'accepted_switch_did_not_activate_alias');
    // Leaving discards only the unsent form; the unknown durable request stays.
    await leaveDraft();
    await page.reload();
    await page.getByRole('button', { name: 'Открыть', exact: true }).click();
    await key('pending-retry').click(); await key('pending-retry').waitFor({ state: 'hidden' });
    ensure(await page.locator('#qa-results').getAttribute('data-prepare-calls') === '0', 'reload_created_new_preparation');
    ensure(await page.locator('#qa-results').getAttribute('data-target-count') === '2'
      && await page.locator('#qa-results').getAttribute('data-receipt-count') === '1', 'lost_ack_duplicated_switch');
    await page.getByRole('button', { name: 'Закрыть', exact: true }).click();
    rows.push({ width, longOriginAndRussianName: width === 320, inactiveThroughCheck: true, independentAcks: 2, lostAckReloadExactRecovery: true, extraPrepareAfterReload: 0, horizontalOverflow: false });
  }
  await open('oldcaps');
  ensure(await key('source-check').getAttribute('aria-disabled') === 'true' && !await key('source-entry-domain').isEnabled(), 'old_server_activation_not_blocked');
  await page.getByText('Обновите Соты, чтобы включать адрес вместе с защищённым источником. Текущий адрес и источник не изменены.', { exact: true }).waitFor();
  ensure(await page.locator('#qa-results').getAttribute('data-prepare-calls') === '0', 'old_server_received_new_action'); await leaveDraft();
  await open('atomic'); await page.getByLabel('Доступ после переключения', { exact: true }).selectOption('restricted');
  ensure(!await key('source-entry-domain').isVisible(), 'restricted_activation_offered'); await leaveDraft();
  await open('noalias'); await key('source-entry-addresses').click();
  ensure(await page.getByRole('tab', { name: 'Доступ и публикация', exact: true }).getAttribute('aria-selected') === 'true', 'reserve_link_did_not_open_access');
  ensure(await page.locator('#qa-results').getAttribute('data-prepare-calls') === '0' && await page.locator('#qa-results').getAttribute('data-promote-calls') === '0', 'reserve_link_dispatched_mutation');
  await page.getByRole('tab', { name: 'Размещение', exact: true }).click();
  await page.locator('[data-settings-key="source-editor"] > summary').click();
  ensure(await page.getByLabel('Порт проекта', { exact: true }).inputValue() === '5321'
    && await page.getByLabel('Подключение', { exact: true }).inputValue() === 'soty.selected-human-embed.v2', 'reserve_link_lost_source_draft');
  await leaveDraft();
  return { ok: true, actualComponent: true, syntheticBackend: true, productionWrites: 0, rows,
    oldCapabilityBlocked: true, restrictedActivationHidden: true, nameNavigationPreservesDraftWithoutMutation: true };
}
