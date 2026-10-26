// Own localhost fixture only. Run with playwright-cli run-code --filename this-file.
async page => {
  const ensure = (value, code) => { if (!value) throw Error(code); };
  const fixture = page.url();
  ensure(/^http:\/\/(?:localhost|127\.0\.0\.1):\d+\/src\/world\/app-source\.test\.html(?:\?.*)?$/u.test(fixture), 'own_source_fixture_required');
  const labels = ['Подключение', 'Компьютер с приложением', 'Порт проекта', 'Начальная страница', 'Доступ после переключения',
    'Отдельный сайт после переключения', 'Адрес отдельного сайта'];
  const rows = [];
  for (const width of [390, 1024]) {
    await page.setViewportSize({ width, height: 844 });
    await page.goto(fixture);
    await page.locator('#qa-mode').selectOption('initial');
    await page.getByRole('button', { name: 'Сбросить только стенд', exact: true }).click();
    await page.getByRole('button', { name: 'Открыть', exact: true }).click();
    const profile = page.getByLabel('Подключение', { exact: true });
    await profile.waitFor({ state: 'visible' });
    const before = await page.locator('.sw-app-settings-source-form input, .sw-app-settings-source-form select').evaluateAll(controls =>
      Object.fromEntries(controls.map(control => [control.dataset.settingsKey, control.id])));
    for (const label of labels.slice(0, 5)) {
      const control = page.getByLabel(label, { exact: true });
      ensure(await control.count() === 1 && await control.isVisible(), 'unique_visible_field_' + label);
      const id = await control.getAttribute('id');
      await page.locator('label[for="' + id + '"] > .sw-field-label').click();
      ensure(await control.evaluate(node => document.activeElement === node), 'caption_did_not_focus_' + label);
    }
    await profile.focus();
    for (const key of ['source-device', 'source-devices-refresh', 'source-port', 'source-path', 'source-audience']) {
      await page.keyboard.press('Tab');
      ensure(await page.locator('[data-settings-key="' + key + '"]').evaluate(node => document.activeElement === node), 'tab_order_' + key);
    }
    await profile.selectOption('soty.selected-human-embed.v2');
    await page.getByLabel('Порт проекта', { exact: true }).fill('5321');
    await page.getByLabel('Адрес отдельного сайта', { exact: true }).selectOption({ label: 'https://demo.apps.fixture.invalid' });
    for (const label of labels) ensure(await page.getByLabel(label, { exact: true }).count() === 1, 'selected_profile_label_' + label);
    await page.getByRole('button', { name: 'Проверить', exact: true }).click();
    const sourceAck = page.locator('[data-settings-key="source-public-ack"]'), nativeAck = page.locator('[data-settings-key="source-native-ack"]');
    await sourceAck.waitFor({ state: 'visible' });
    ensure(await sourceAck.isEnabled() && await nativeAck.isEnabled(), 'fixture_preparation_not_ready');
    for (const control of [sourceAck, nativeAck]) {
      const caption = await control.evaluate(node => document.getElementById(node.getAttribute('aria-labelledby'))?.textContent);
      ensure(caption && await page.getByLabel(caption, { exact: true }).count() === 1, 'exposure_ack_not_labeled');
    }
    await sourceAck.focus(); await page.keyboard.press('Space');
    ensure(await sourceAck.isChecked() && !await nativeAck.isChecked(), 'source_ack_changed_native_ack');
    await page.keyboard.press('Space'); await page.keyboard.press('Tab');
    ensure(await nativeAck.evaluate(node => document.activeElement === node), 'native_ack_not_next_tab');
    await page.keyboard.press('Space');
    ensure(await nativeAck.isChecked() && !await sourceAck.isChecked(), 'native_ack_changed_source_ack');
    await page.keyboard.press('Space');
    const state = await page.locator('.sw-app-settings-source-form').evaluate(form => {
      const controls = [...form.querySelectorAll('input,select')], dialog = form.closest('dialog'), rect = dialog.getBoundingClientRect();
      return { ids: Object.fromEntries(controls.map(control => [control.dataset.settingsKey, control.id])),
        idsUnique: controls.every(control => control.id && document.querySelectorAll('[id="' + control.id + '"]').length === 1),
        overflowing: controls.filter(control => control.getClientRects().length && (() => { const r = control.getBoundingClientRect(); return r.left < rect.left - 1 || r.right > rect.right + 1; })()).map(control => control.dataset.settingsKey),
        viewportWidth: innerWidth, documentWidth: document.documentElement.scrollWidth };
    });
    ensure(state.idsUnique && JSON.stringify(state.ids) === JSON.stringify(before), 'control_identity_changed_after_render');
    ensure(!state.overflowing.length && state.documentWidth <= state.viewportWidth, 'source_form_horizontal_overflow');
    await profile.scrollIntoViewIfNeeded();
    await page.screenshot({ path: 'output/playwright/source-fields-' + width + '.png' });
    rows.push({ width, namedFields: labels.length, independentKeyboardAcks: 2, stableUniqueControls: Object.keys(state.ids).length,
      keyboardTabOrder: true, captionClickFocus: true, horizontalOverflow: false });
    await page.getByRole('button', { name: 'Закрыть', exact: true }).click();
    await page.getByRole('button', { name: 'Выйти без сохранения', exact: true }).click();
  }
  await page.locator('#qa-mode').selectOption('unbound');
  await page.getByRole('button', { name: 'Сбросить только стенд', exact: true }).click();
  await page.getByRole('button', { name: 'Открыть', exact: true }).click();
  await page.getByLabel('Подключение', { exact: true }).selectOption('soty.selected-human-embed.v2');
  await page.getByLabel('Порт проекта', { exact: true }).fill('5321');
  await page.getByRole('button', { name: 'Проверить', exact: true }).click();
  await page.getByText('Добавьте именной адрес для входа Сот в разделе «Доступ и публикация», включите его, затем проверьте источник.', { exact: true }).waitFor({ state: 'visible' });
  ensure(await page.locator('#qa-results').getAttribute('data-prepare-calls') === '1', 'unbound_address_prepare_replayed');
  ensure(await page.locator('#qa-results').getAttribute('data-promote-calls') === '0', 'unbound_address_promoted');
  ensure(await page.getByLabel('Порт проекта', { exact: true }).inputValue() === '5321', 'unbound_address_lost_draft');
  await page.getByRole('button', { name: 'Закрыть', exact: true }).click();
  await page.getByRole('button', { name: 'Выйти без сохранения', exact: true }).click();
  return { ok: true, actualComponent: true, fixtureOnly: true, productionWrites: 0, sourcePromotions: 0,
    simulated403FriendlyMessage: true, simulated403PrepareCalls: 1, simulated403Promotions: 0, rows };
}
