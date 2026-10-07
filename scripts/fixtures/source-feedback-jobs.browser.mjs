// Actual HTTPS Root UI + installed Source channel. NEW EMPTY Native policy
// only; no claim of an existing-owner login or production engine/model.
async(page)=>{
  const checks={},check=(value,name)=>{if(!value)throw new Error('source_jobs_browser_'+name);checks[name]=true;},app=()=>page.frameLocator('iframe');
  await page.getByRole('button',{name:'Открыть новое приложение A',exact:true}).click();
  await app().getByRole('button',{name:'Войти через Соты',exact:true}).waitFor();
  const opened=page.context().waitForEvent('page');await app().getByRole('button',{name:'Войти через Соты',exact:true}).click();const popup=await opened;
  await popup.getByRole('checkbox',{name:'Подключить этот выбранный ресурс с моими текущими правами'}).check();
  await popup.getByRole('button',{name:'Войти через Соты',exact:true}).click();
  await popup.getByRole('button',{name:'Войти',exact:true}).click({timeout:20000});
  await popup.getByRole('heading',{name:'Вход подтверждён',exact:true}).waitFor({timeout:20000});
  await popup.getByRole('link',{name:'Завершить подключение',exact:true}).click();
  await popup.getByRole('heading',{name:'Приложение подключено',exact:true}).waitFor({timeout:15000});
  await app().getByRole('button',{name:'Сообщить проблему',exact:true}).waitFor({timeout:20000});check(true,'actualNativeAndRootUi');
  await popup.close();await page.bringToFront();
  await app().getByRole('button',{name:'Сообщить проблему',exact:true}).click();
  await app().getByRole('textbox',{name:'Что произошло?',exact:true}).fill('Synthetic private image issue');
  await app().getByLabel('Материал к обращению',{exact:true}).setInputFiles('C:/Users/Junio/.codex/worktrees/soty-source-app-sdk/соты/output/playwright/source-job-synthetic.png');
  await app().getByRole('button',{name:'Отправить',exact:true}).click();
  await app().getByText('Обращение получено',{exact:true}).waitFor();check(true,'actualRetainedPng');
  await app().getByRole('button',{name:'Обработка материалов обращения',exact:true}).click();
  const consent=app().getByRole('button',{name:'Разрешить распознавание изображения',exact:true});await consent.waitFor();
  check(await consent.isDisabled(),'explicitReporterConsent');
  await app().getByRole('checkbox',{name:'Разрешаю локальное распознавание изображения выбранных материалов этого обращения.',exact:true}).check();
  await consent.click();await app().getByText('Разрешение автора сохранено для выбранных материалов и цели.',{exact:true}).waitFor();check(true,'reporterConsentSaved');
  await app().getByRole('button',{name:'Сохранить разрешение владельца',exact:true}).click();
  await app().getByText('Разрешение владельца сохранено. Обработчик пока недоступен; обработка не запускалась.',{exact:true}).waitFor();check(true,'nativeOwnerGrantSaved');
  check(await app().getByRole('button',{name:'Обработать обращение',exact:true}).isDisabled(),'notReadyNoStart');
  await page.setViewportSize({width:390,height:1000});
  const bounds=await app().locator('#processing-panel').evaluate(element=>({overflow:document.documentElement.scrollWidth>innerWidth,
    buttons:[...element.querySelectorAll('button')].map(button=>({height:button.getBoundingClientRect().height,width:button.getBoundingClientRect().width}))}));
  check(!bounds.overflow&&bounds.buttons.every(value=>value.height>=44&&value.width<=390),'mobileBounds');
  await page.screenshot({path:'output/playwright/source-feedback-jobs-mobile.png',fullPage:true});
  const proof=await page.evaluate(async()=>{const response=await fetch('/__fixture/proof');return response.json();}),board=proof.realms.find(value=>value.realm==='board');
  check(board.format===3&&board.tickets===1&&board.processingConsents===1&&board.processingGrants===1&&board.jobs===1&&board.processorReceipts===0,'realSqlNoProcessing');
  check(proof.sourceCookiesInjected===false&&proof.humanCookiesInjected===false,'noInjectedSessions');
  return{checks,source3:board.format,counts:{tickets:board.tickets,consents:board.processingConsents,grants:board.processingGrants,jobs:board.jobs,results:board.processorReceipts},
    oauthCounts:proof.oauthCounts,modelCalls:0,sourceJobsReady:false,ownerEvidence:'NEW-empty-policy plus verified OIDC then current Native SQL owner; existing-owner proven separately via installed HTTP'};
}
