async page => {
 const check=(condition,message)=>{if(!condition)throw Error(message)};
 await page.getByRole('heading',{name:'Пригласить в Соты'}).waitFor();
 for(let i=0;i<3;i++){await page.getByRole('button',{name:'Создать приглашение',exact:true}).click();await page.getByText(`Доступно ${2-i} из 3 приглашений`).waitFor();}
 check(await page.getByRole('button',{name:'Создать приглашение',exact:true}).isDisabled(),'fourth UI button enabled');
 const denied=await page.evaluate(async()=>{try{await fixtureClient.extension('platform.invites.issue',{requestId:crypto.randomUUID(),token:btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(32)))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/g,'')});return false}catch(e){return e.code==='platform_invite_quota_exhausted'}});
 check(denied,'fourth server request admitted');
 await page.getByRole('button',{name:'Показать ссылку и QR'}).first().click();
 await page.getByRole('img',{name:'QR приглашения в Соты'}).waitFor();
 const link=await page.getByRole('textbox',{name:'Приглашение в Соты',exact:true}).inputValue();
 check(link.includes('#soty-invite=')&&!link.includes('?'),'token is not fragment only');
 await page.reload();await page.getByText('Доступно 0 из 3 приглашений').waitFor();
 check(await page.getByRole('button',{name:'Показать ссылку и QR'}).count()===3,'links lost across reload');
 await page.getByText('Вход в Соты',{exact:true}).click();
 await page.getByRole('checkbox',{name:'Только по приглашению'}).check();
 await page.getByRole('button',{name:'Сохранить режим входа'}).click();
 await page.waitForFunction(async()=> (await fixtureClient.extension('platform.admission.get')).inviteOnly);
 const context=await page.context().browser().newContext();const guest=await context.newPage();
 await guest.goto(link);await guest.getByRole('heading',{name:'Пригласить в Соты'}).waitFor();
 check(!guest.url().includes('soty-invite'),'fragment remains');
 check(await guest.getByRole('button',{name:'Сохранить режим входа'}).count()===0,'nonadmin controls visible');
 const guestDenied=await guest.evaluate(async()=>{try{await fixtureClient.extension('platform.admission.update',{requestId:'denied_owner',expectedRevision:1,inviteOnly:false,defaultQuota:3});return false}catch(e){return e.code==='platform_admin_required'}});
 check(guestDenied,'nonadmin update allowed');
 await page.getByRole('button',{name:'Обновить приглашения'}).click();await page.getByText(/^Использовано/).waitFor();
 await page.getByRole('button',{name:'Отозвать приглашение'}).first().click();await page.getByText('Доступно 1 из 3 приглашений').waitFor();
 check(await page.getByText(/^Отозвано/).count()===1,'revoke not rendered');
 await page.setViewportSize({width:390,height:844});
 check(await page.evaluate(()=>document.documentElement.scrollWidth<=390),'page horizontal overflow');
 await page.screenshot({path:'output/playwright/platform-invitations-mobile.png'});
 await context.close();
 return {passed:true,created:3,deniedFourth:true,qr:true,reloadPreserved:true,fragmentCleared:true,redeemed:true,adminDenied:true,revoke:true,mobileWidth:390};
}
