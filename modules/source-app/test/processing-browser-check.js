async page => {
  const need=(value,message)=>{if(!value)throw Error(message);};
  const open=()=>page.getByRole('button',{name:'Обработка материалов обращения'}).click();
  const checkbox=()=>page.getByRole('checkbox');
  const consent=()=>page.getByRole('button',{name:'Разрешить распознавание изображения'});
  const retry=()=>page.getByRole('button',{name:'Проверить результат'});
  const state=()=>page.evaluate(async()=>(await(await fetch('/__fixture/state')).json()));
  const pending=()=>page.evaluate(()=>sessionStorage.getItem('processing-pending:'+'a'.repeat(64)));
  await open();await checkbox().check();await consent().click();
  await retry().waitFor();need((await state()).calls.A.length===1,'first request missing');
  await retry().click();await retry().waitFor();need((await state()).calls.A.length===1,'unknown receipt repeated the write');
  need(Boolean(await pending()),'missing pending reference');
  await page.reload();await open();await retry().waitFor();need((await state()).calls.A.length===1,'reload repeated the write');
  await page.evaluate(()=>{document.cookie='fixture_profile=B; path=/';});await open();await checkbox().waitFor();
  need(Boolean(await pending())&&(await state()).calls.B.length===0,'switch lost another profile reference');
  await checkbox().check();await page.evaluate(()=>{document.cookie='fixture_profile=A; path=/';});await consent().click();
  await page.waitForFunction(()=>document.querySelector('#processing-state').textContent.includes('Профиль изменился'));
  need((await state()).calls.A.length===1&&(await state()).calls.B.length===0,'a changed profile was submitted');
  await page.evaluate(async()=>{await fetch('/__fixture/commit');});
  await open();await retry().waitFor();await retry().click();
  await page.waitForFunction(()=>document.querySelector('#processing-state').textContent==='Предыдущее разрешение подтверждено.');
  need(await pending()===null&&(await state()).calls.A.length===1,'confirmed receipt did not clear safely');
  await page.evaluate(()=>{Storage.prototype.setItem=()=>{throw Error('storage blocked');};});
  await checkbox().check();await consent().click();
  await page.waitForFunction(()=>document.querySelector('#processing-state').textContent.includes('Разрешение не отправлено'));
  need((await state()).calls.A.length===1,'write was sent without durable tab reference');
  return {unknownRetained:true,reloadRetained:true,profilesSeparated:true,confirmedCleared:true,blockedStorageNoWrite:true};
}
