// Actual admitted Apps7 stage and installed channel; synthetic Source requester
// asks only for media selection. This does not implement a Native feedback queue.
async(page)=>{
  const checks={},check=(value,name)=>{if(!value)throw new Error('scoped_capture_'+name);checks[name]=true;};
  const app=()=>page.frameLocator('iframe[title="Планировщик · Synthetic project"]');
  async function action(name,id,route){
    await page.bringToFront();const origin=await page.evaluate(()=>location.origin);
    const ack=route?page.waitForResponse(origin+'/__fixture/'+route):null;
    const button=page.getByRole('button',{name,exact:true});await button.focus();await button.press('Enter');
    if(ack)check((await ack).status()===200,'actualHelper');
    await page.waitForFunction(id=>document.querySelector('#result')?.getAttribute('data-action')===id&&!document.getElementById(id)?.disabled,id);
    check(await page.locator('#result').getAttribute('data-status')!=='fail','actualHelper');
  }
  async function login(first){
    const opened=page.context().waitForEvent('page');await app().getByRole('button',{name:'Войти через Соты',exact:true}).click();const popup=await opened;
    await popup.getByRole('button',{name:'Войти',exact:true}).click();
    if(first)await popup.getByRole('button',{name:'Разрешить только это пространство',exact:true}).click();
    await popup.getByRole('heading',{name:'Профиль подключён',exact:true}).waitFor();
    await app().getByRole('region',{name:'Непрерывная временная шкала',exact:true}).waitFor();await popup.close();await page.bringToFront();
  }
  async function sourceRequest(requestId,sourceId='planner.selected-workspace'){
    const frame=await(await page.locator('iframe').elementHandle()).contentFrame(),parentOrigin=await page.evaluate(()=>location.origin);
    await frame.evaluate(async({requestId,sourceId,parentOrigin})=>{
      const read=await fetch('/api/embed/state');if(!read.ok)throw new Error('fixture_native_acl');const state=await read.json();
      const channel=new MessageChannel();window.__captureReceipt={pending:true};
      channel.port1.onmessage=event=>{const value=event.data;window.__captureReceipt={type:value.type,code:value.code??null,count:value.attachments?.length??0,
        kind:value.attachments?.[0]?.kind??null,hasPrivate:['actor','handle','slot','resources','subject'].some(key=>Object.hasOwn(value,key))};channel.port1.close();};
      channel.port1.start();parent.postMessage({schema:'soty.feedback.capture.v1',type:'capture_request',requestId,sourceId,
        projectId:state.workspaces[0].id,contextRevision:1,kind:'image'},parentOrigin,[channel.port2]);
    },{requestId,sourceId,parentOrigin});return frame;
  }
  await action('Открыть тестовый проект','start','grant');await login(true);
  await page.setViewportSize({width:390,height:844});
  const original=await page.locator('iframe').elementHandle();const frame=await sourceRequest('capture-actual-one');
  const dialog=page.getByRole('dialog',{name:'Скриншот для обращения',exact:true});await dialog.waitFor();
  check(await dialog.evaluate(element=>{const rect=element.getBoundingClientRect();return rect.width<=innerWidth&&rect.left>=0&&rect.right<=innerWidth&&[...element.querySelectorAll('button')].filter(button=>!button.hidden).every(button=>{const bounds=button.getBoundingClientRect();return bounds.width>=43.5&&bounds.height>=43.5;});}),'mobileDialogBounds');
  check(await dialog.getByText('Отправку обращения вы подтвердите в проекте.',{exact:false}).count()===1,'explicitRecipient');
  await dialog.locator('input[type=file]').setInputFiles('output/playwright/scoped-capture-selected.png');
  await dialog.getByRole('img',{name:'Выбранный скриншот',exact:true}).waitFor();
  await page.screenshot({path:'output/playwright/scoped-gateway-capture-mobile.png',scale:'css'});
  await dialog.getByRole('button',{name:'Использовать вложение',exact:true}).click();
  await frame.waitForFunction(()=>window.__captureReceipt?.type==='capture_result');
  const receipt=await frame.evaluate(()=>window.__captureReceipt);
  check(receipt.count===1&&receipt.kind==='image'&&!receipt.hasPrivate,'realPngSelectedNoPrivateFields');
  check(await page.evaluate(element=>element===document.querySelector('iframe'),original),'iframeRetained');
  await action('Подготовить второй тестовый профиль','second');await action('Открыть тестовый проект','start','grant');await login(false);
  await sourceRequest('capture-before-switch');await dialog.waitFor();
  // A profile switch is performed in the public SDK from another client. The
  // modal blocks helper buttons, so this test clicks no hidden recipient/UI.
  const switchClick=page.evaluate(async()=>{const {createConnectClient}=await import('/modules/connect/browser/index.mjs');
    const client=createConnectClient({projectId:'soty',endpoint:'/api/connect/rpc',dbName:'soty-connect-v1'});
    try{const state=await client.getLocalState();const other=state.profiles.find(profile=>profile.accountId!==state.accountId);if(!other)throw new Error('fixture_second');await client.switchProfile(other.accountId);}finally{client.dispose();}});
  await switchClick;await page.waitForFunction(()=>document.querySelectorAll('iframe').length===0&&document.querySelectorAll('dialog[open]').length===0);
  checks.profileSwitchCancelsPreview=true;
  return {synthetic:true,actualRootSlot:true,nativeFeedbackSubmission:false,productionHttpsValidated:false,checks};
}
