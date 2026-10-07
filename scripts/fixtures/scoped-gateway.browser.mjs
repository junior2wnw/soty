// Run this expression through pinned playwright-cli run-code, in the isolated
// fixture browser. No mocked routes, tokens, headers or direct database writes.
async (page) => {
  const checks = {}, check = (value, name) => { if(!value)throw new Error('scoped_browser_'+name);checks[name]=true; };
  const app = () => page.frameLocator('iframe[title="Планировщик · Synthetic project"]');
  async function action(name) {
    const routes={'Открыть тестовый проект':'grant','Перезапустить исходное приложение':'restart-source','Отозвать тестовый доступ':'revoke'};
    const fixtureOrigin=await page.evaluate(()=>location.origin);
    const response=page.waitForResponse(fixtureOrigin+'/__fixture/'+routes[name]).catch(()=>{throw new Error('scoped_browser_fixtureAck_'+routes[name]);});
    await page.bringToFront();const button=page.getByRole('button',{name,exact:true});await button.focus();await button.press('Enter');const acknowledged=await response;
    check(acknowledged.status()===200,'fixtureAction');
    await page.waitForFunction(()=>document.querySelector('#result')?.getAttribute('data-status')!=='pending');
    check(await page.locator('#result').getAttribute('data-status')!=='fail','fixtureAction');
  }
  async function readInApp(action) { const frame=await(await page.locator('iframe').elementHandle()).contentFrame();return frame.evaluate(action); }
  async function profileAction(name,id) {
    await page.bringToFront();const button=page.getByRole('button',{name,exact:true});await button.focus();await button.press('Enter');
    await page.waitForFunction(id=>document.querySelector('#result')?.getAttribute('data-action')===id&&!document.getElementById(id)?.disabled,id);
    check(await page.locator('#result').getAttribute('data-status')!=='fail','profileAction');
  }
  async function login(nativeExpected) {
    const opened=page.context().waitForEvent('page');await app().getByRole('button',{name:'Войти через Соты',exact:true}).click();const popup=await opened;
    await popup.getByRole('button',{name:'Войти',exact:true}).click({timeout:15000});
    if(nativeExpected)await popup.getByRole('button',{name:'Разрешить только это пространство',exact:true}).click({timeout:15000});
    await popup.getByRole('heading',{name:'Профиль подключён',exact:true}).waitFor({timeout:15000});
    check(await popup.getByRole('link',{name:'Вернуться в Соты',exact:true}).count()===1,'fixedCompletion');
    await app().getByRole('region',{name:'Непрерывная временная шкала',exact:true}).waitFor({timeout:15000});
    await popup.close();await page.bringToFront();
  }
  await action('Открыть тестовый проект');await login(true);checks.firstNativeConsent=true;
  await app().getByRole('button',{name:'Добавить на шкалу',exact:true}).click();
  const dialog=app().getByRole('dialog',{name:'Добавить на шкалу',exact:true});await dialog.getByRole('textbox',{name:'Название',exact:true}).fill('Синтетический объект browser runner');
  await dialog.getByRole('button',{name:'Добавить на шкалу',exact:true}).click();
  await app().getByRole('button',{name:'Синтетический объект browser runner',exact:true}).waitFor();
  check(await readInApp(async()=>{for(let attempt=0;attempt<20;attempt++){const response=await fetch('/api/embed/state');if(response.ok&&(await response.json()).entities.some(value=>value.title==='Синтетический объект browser runner'))return true;await new Promise(done=>setTimeout(done,100));}return false;}),'realWriteCommitted');
  await page.evaluate(()=>{window.__scopedQaFrame=document.querySelector('iframe');});
  await page.getByRole('button',{name:'Сообщить проблему',exact:true}).click();await page.keyboard.press('Escape');
  check(await page.evaluate(()=>window.__scopedQaFrame===document.querySelector('iframe')&&document.activeElement?.getAttribute('aria-label')==='Сообщить проблему'),'iframeAndFocus');
  await action('Перезапустить исходное приложение');
  const retained=await readInApp(async()=>{const response=await fetch('/api/embed/state'),state=await response.json();const object=state.entities.find(value=>value.title==='Синтетический объект browser runner');return response.status===200&&state.workspaces.length===1&&object?.plan?.start===null;});check(retained,'realUndatedRetained');
  const denied=await readInApp(async()=>{const legacy=await fetch('/api/state'),foreign=await fetch('/api/embed/entities',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({workspaceId:'not-selected',title:'Must not create'})});return legacy.status>=400&&foreign.status===403;});check(denied,'privateAndLegacyDenied');
  await page.setViewportSize({width:390,height:844});
  check(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth&&[...document.querySelectorAll('.sa-toolbar button')].filter(el=>{const r=el.getBoundingClientRect();return r.width>0&&r.height>0&&(r.width<43.5||r.height<43.5);}).length===0),'mobileBounds');
  await page.screenshot({path:'output/playwright/scoped-gateway-runner-mobile.png',scale:'css'});
  await action('Открыть тестовый проект');await login(false);checks.repeatNoNativeConsent=true;
  await profileAction('Подготовить второй тестовый профиль','second');
  check(await page.locator('iframe').count()===0,'profileSetupClosesPreviousStage');
  await action('Открыть тестовый проект');await login(false);
  await profileAction('Сменить профиль в другой вкладке','switch');
  check(await page.locator('iframe').count()===0,'crossTabSwitchClosesStage');
  await profileAction('Вернуть исходный тестовый профиль','restore');
  await action('Открыть тестовый проект');await login(false);
  await action('Отозвать тестовый доступ');
  const revoked=await readInApp(async()=>{const ready=await fetch('/api/embed/session-status'),read=await fetch('/api/embed/state');return ready.status===403&&read.status===403;});check(revoked,'freshRevoke');
  const proof=await page.evaluate(async()=>{const response=await fetch('/__fixture/proof');return response.json();});check(proof.otherPrivateObjects===1,'otherProjectUntouched');
  return {synthetic:true,productionHttpsValidated:false,checks};
}
