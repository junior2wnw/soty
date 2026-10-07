// Separate fresh temp fixture/browser. Real lost Source ACK after COMMIT and
// consumed Root mapping, then explicit UI-only readonly receipt recovery.
async (page) => {
  const checks = {}, check = (value, name) => { if(!value)throw new Error('ordinary_recovery_' + name);checks[name]=true; };
  const app=()=>page.frameLocator('iframe');
  await page.getByRole('button',{name:'Открыть новое приложение A',exact:true}).click();
  await app().getByRole('button',{name:'Войти через Соты',exact:true}).waitFor();
  await page.evaluate(async()=>{window.__recoveryFrame=document.querySelector('iframe');const r=await fetch('/__fixture/drop-callback',{method:'POST',headers:{'content-type':'application/json'},body:'{}'});if(!r.ok)throw new Error('fixture_drop_failed');});
  let opened=page.context().waitForEvent('page');await app().getByRole('button',{name:'Войти через Соты',exact:true}).click();let popup=await opened;
  await popup.getByRole('checkbox',{name:'Подключить этот выбранный ресурс с моими текущими правами'}).check();
  await popup.getByRole('button',{name:'Войти через Соты',exact:true}).click();
  const failingResponse=popup.waitForResponse(response=>response.url().includes('/api/embed/callback?'));
  await popup.getByRole('button',{name:'Войти',exact:true}).click({timeout:20000});
  const failed=await failingResponse;
  check([502,503].includes(failed.status()),'actualUnknownAck');
  // Root's isolated failure document may replace the popup's CDP target.
  // Reacquire the actual visible callback page, never relax COOP for the test.
  popup=page.context().pages().find(candidate=>candidate!==page&&candidate.url().includes('/api/embed/callback?'))??popup;
  await popup.getByRole('heading',{name:'Приложение недоступно',exact:true}).waitFor({timeout:15000});
  // Browser reload uses the consumed original Root map and is rejected. No
  // new Source exchange/link/grant is performed by reload.
  const reload=await popup.reload();check(reload.status()===403,'consumedRootMap');await popup.close();
  await page.bringToFront();await page.getByRole('button',{name:'Перезапустить приложения',exact:true}).click();
  await page.waitForFunction(()=>document.querySelector('#result')?.dataset.status!=='pending');
  opened=page.context().waitForEvent('page');await app().getByRole('button',{name:'Войти через Соты',exact:true}).click();popup=await opened;
  await popup.getByRole('heading',{name:'Вход подтверждён',exact:true}).waitFor({timeout:20000});
  check(await popup.getByRole('button',{name:'Войти',exact:true}).count()===0,'noNewIdentityExchange');
  await popup.getByRole('link',{name:'Завершить подключение',exact:true}).click();
  await popup.getByRole('heading',{name:'Приложение подключено',exact:true}).waitFor();
  await app().getByRole('textbox',{name:'Название записи',exact:true}).waitFor({timeout:20000});
  check(await page.evaluate(()=>document.querySelector('iframe')===window.__recoveryFrame),'sameOriginalIframe');
  const proof=await page.evaluate(async()=>{const r=await fetch('/__fixture/proof');return r.json();}),realm=proof.realms[0];
  check(realm.interactions===1&&realm.sessions===1&&realm.principals===1&&realm.links===1&&realm.consents===1,'oneDurableIntentReceipt');
  check(proof.oauthCounts['token:200']===1&&!proof.oauthCounts['token:429'],'tokenExchangeExactlyOnce');
  await popup.close();await page.bringToFront();
  return{synthetic:true,basic300NoResume:true,productionHttpsValidated:false,checks,oauthCounts:proof.oauthCounts};
}
