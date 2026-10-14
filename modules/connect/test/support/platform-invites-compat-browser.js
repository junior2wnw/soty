async page => {
 const check=(condition,message)=>{if(!condition)throw Error(message)};
 const before=await page.evaluate(async()=> (await fixtureClient.extension('platform.invites.list')).invitations.filter(x=>x.redeemedAt).length);
 const link=await page.evaluate(()=>{const entry=Object.entries(sessionStorage).find(([key])=>key.startsWith('connect.platform-invites:')); const data=JSON.parse(entry[1]);return location.origin+'/#soty-invite='+Object.values(data.links)[0];});
 await page.goto(link);await page.getByRole('heading',{name:'Пригласить в Соты'}).waitFor();
 check(!page.url().includes('soty-invite'),'known fragment was retained');
 const after=await page.evaluate(async()=> (await fixtureClient.extension('platform.invites.list')).invitations.filter(x=>x.redeemedAt).length);
 check(before===after,'known account spent invite');
 await page.route('**/api/connect/rpc',async route=>{const body=route.request().postDataJSON();if(body.op==='challenge'&&body.args.operation.startsWith('platform.'))await route.fulfill({status:400,contentType:'application/json',body:JSON.stringify({ok:false,error:{code:'unsupported_operation'}})});else await route.continue();});
 await page.getByRole('button',{name:'Обновить приглашения'}).click();await page.getByText('Приглашения в Соты появятся после обновления сервера.').waitFor();
 check(await page.getByRole('button',{name:'Создать приглашение',exact:true}).count()===0,'old server has fake controls');
 await page.unroute('**/api/connect/rpc');await page.getByRole('button',{name:'Повторить загрузку приглашений'}).click();await page.getByRole('heading',{name:'Пригласить в Соты'}).waitFor();
 await page.setViewportSize({width:1280,height:900});await page.screenshot({path:'output/playwright/platform-invitations-desktop.png'});
 return {knownAccountDoesNotSpend:true,fragmentCleared:true,oldServerUnsupportedNoControls:true,recoveryRendered:true};
}
