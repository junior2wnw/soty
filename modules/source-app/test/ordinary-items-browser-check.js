async page => {
  const need=(value,message)=>{if(!value)throw Error(message);};
  const count=()=>page.evaluate(async()=>(await(await fetch('/__fixture/state')).json()).calls);
  const pending=()=>page.evaluate(()=>sessionStorage.getItem('ordinary-items-pending:'+'a'.repeat(64)));
  const title=()=>page.getByLabel('Название записи');
  const add=()=>page.getByRole('button',{name:'Добавить запись'});
  const retry=()=>page.getByRole('button',{name:'Проверить результат'});
  await title().waitFor();await title().fill('Учебная запись');await add().click();await retry().waitFor();
  need((await count()).A.length===1&&Boolean(await pending()),'missing first saved reference');
  await retry().click();await retry().waitFor();need((await count()).A.length===1,'unknown receipt repeated the write');
  await page.reload();await retry().waitFor();need((await count()).A.length===1&&Boolean(await pending()),'reload lost the reference');
  need(await title().inputValue()==='Учебная запись'&&await title().isEnabled()&&!await title().isEditable(),'reload lost the copyable draft');
  await page.evaluate(()=>{document.cookie='fixture_profile=B; path=/';});await page.reload();await title().waitFor();
  need(await title().isEditable()&&await title().inputValue()===''&&Boolean(await pending()),'another profile inherited or erased the pending write');
  await title().fill('Запись второго профиля');await page.evaluate(()=>{document.cookie='fixture_profile=A; path=/';});await add().click();
  await page.waitForFunction(()=>document.querySelector('#state').textContent.includes('Профиль изменился'));
  need((await count()).A.length===1&&(await count()).B.length===0,'a changed profile was submitted');
  await page.evaluate(async()=>{await fetch('/__fixture/commit');});await page.reload();await retry().waitFor();await retry().click();
  await page.waitForFunction(()=>document.querySelector('#state').textContent==='Запись добавлена');
  need(await pending()===null&&(await count()).A.length===1&&await page.evaluate(()=>sessionStorage.getItem('ordinary-items-draft:'+'a'.repeat(64)))===null,
    'committed receipt did not clear safely');
  await page.evaluate(()=>{Storage.prototype.setItem=()=>{throw Error('storage blocked');};});
  await title().fill('Другая запись');await add().click();
  await page.waitForFunction(()=>document.querySelector('#state').textContent.includes('Запись не отправлена'));
  need((await count()).A.length===1,'write was sent without a saved reference');
  need(await page.evaluate(async()=>(await(await fetch('/__fixture/state')).json()).feedbackContextRequests)===0,'items depended on feedback context');
  return {unknownRetained:true,reloadRetained:true,profilesSeparated:true,confirmedCleared:true,blockedStorageNoWrite:true};
}
