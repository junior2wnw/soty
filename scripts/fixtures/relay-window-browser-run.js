// Run against relay-window-browser-fixture.mjs using playwright-cli run-code --filename.
async page => {
  const root = page.url().replace(/\/fixture(?:[?#].*)?$/, '');
  const context = page.context();
  const check = async (frame, expected, button = '#read') => {
    await frame.locator('#subject').evaluate(element => { element.textContent = 'pending'; });
    await frame.locator(button).click();
    await frame.locator('#subject').filter({ hasText: expected }).waitFor({ timeout: 10000 });
    if (await frame.locator('#subject').textContent() !== expected) throw Error('unexpected app result');
  };
  const state = async () => page.evaluate(async () => (await fetch('/fixture/state')).json());
  await page.goto(root + '/fixture');
  await page.locator('#alice').click();
  const alice = page.frameLocator('#alice-frame'), bob = page.frameLocator('#bob-frame');
  await check(alice, 'alice:ALICE_DATA');
  await page.locator('#bob').click();
  await check(bob, 'bob:BOB_DATA');
  await check(alice, 'alice:ALICE_SAVED', '#write');
  await check(bob, 'bob:BOB_DATA');

  const anotherTab = await context.newPage();
  await anotherTab.goto(root + '/fixture');
  await anotherTab.locator('#bob').click();
  await check(anotherTab.frameLocator('#bob-frame'), 'bob:BOB_DATA');
  await check(alice, 'alice:ALICE_SAVED');
  const aliceFrame = await (await page.locator('#alice-frame').elementHandle()).contentFrame();
  await Promise.all([page.waitForEvent('framenavigated', { predicate: frame => frame === aliceFrame }),
    alice.locator('#read').evaluate(() => location.reload())]);
  await check(alice, 'alice:ALICE_SAVED');

  // Reloading via the shell closes the old window grant before a new one is issued.
  await Promise.all([page.waitForEvent('framenavigated', { predicate: frame => frame === aliceFrame }), page.locator('#alice').click()]);
  await check(alice, 'alice:ALICE_SAVED');

  const beforeRestart = await state();
  const restartStatus = await page.evaluate(async () => (await fetch('/fixture/restart', { method: 'POST' })).status);
  if (restartStatus !== 200) throw Error('fixture Root restart failed');
  await check(alice, 'alice:ALICE_SAVED');
  const afterRead = await state();
  if (afterRead.protectedCalls !== beforeRestart.protectedCalls + 1) throw Error('restart read was dispatched more than once');
  await check(bob, 'Вход недоступен', '#write');
  const afterRefusedWrite = await state();
  if (afterRefusedWrite.protectedCalls !== afterRead.protectedCalls || afterRefusedWrite.writes !== afterRead.writes) throw Error('restart write was dispatched');
  await check(bob, 'bob:BOB_SAVED', '#write');
  const afterWrite = await state();
  if (afterWrite.writes !== afterRead.writes + 1) throw Error('new user action did not commit exactly once');
  await check(bob, 'bob:BOB_SAVED', '#write');
  if ((await state()).writes !== afterWrite.writes) throw Error('the same request id created a duplicate write');

  const standalone = await context.newPage();
  await standalone.goto(root + '/fixture');
  await standalone.evaluate(async () => { const result = await fetch('/fixture/launch?who=alice'); location.assign((await result.json()).url); });
  await standalone.locator('#read').waitFor({ timeout: 10000 });
  const beforeStandalone = await state();
  await standalone.locator('#read').click();
  await standalone.locator('#subject').filter({ hasText: 'Вход недоступен' }).waitFor({ timeout: 10000 });
  const afterStandalone = await state();
  if (afterStandalone.protectedCalls !== beforeStandalone.protectedCalls) throw Error('standalone dispatched identity');

  const revokeStatus = await page.evaluate(async () => (await fetch('/fixture/revoke-bob')).status);
  if (revokeStatus !== 200) throw Error('fixture revoke failed');
  const beforeRevoked = await state();
  await check(bob, 'Вход недоступен');
  const afterRevoked = await state();
  if (afterRevoked.protectedCalls !== beforeRevoked.protectedCalls) throw Error('revoked identity reached app');
  await check(alice, 'alice:ALICE_SAVED');
  await standalone.close(); await anotherTab.close();
  return { twoFrames: true, twoTabs: true, isolatedWriteReadback: true, reload: true, shellReopen: true,
    rootRestartReadRecovered: true, restartWriteNotReplayed: true, requestIdReusedWithoutDuplicate: true,
    standaloneRefusedBeforeUpstream: true, revokedRefusedBeforeUpstream: true, survivingAccount: true };
}
