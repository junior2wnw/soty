import {createSourceWorkspaceClient} from '/assets/source-sdk.js';
const $=id=>document.getElementById(id),client=createSourceWorkspaceClient();
const state=$('state'),content=$('content'),retry=$('retry'),form=$('add'),save=$('save-state'),more=$('more'),pageState=$('page-state');
const id=()=>crypto.randomUUID();let busy=false,unresolved=false,pendingKey=null,draftKey=null,nextCursor=null,currentScope=null;
const draftState=$('draft-state'),title=$('title');
const validDraft=value=>typeof value==='string'&&value.length<=240&&value.isWellFormed()&&!/[\u0000-\u001f\u007f]/u.test(value);
function scopeDraft(scope){const key='reading-draft:'+scope;if(key===draftKey)return;draftKey=key;title.value='';draftState.textContent='';
  try{const saved=sessionStorage.getItem(key);if(validDraft(saved))title.value=saved;}catch{draftState.textContent='Черновик на этом устройстве недоступен.';}}
title.addEventListener('input',()=>{if(!draftKey)return;try{if(!validDraft(title.value))throw Error();sessionStorage.setItem(draftKey,title.value);draftState.textContent='Черновик сохранён на этом устройстве.';}
  catch{draftState.textContent='Не удалось сохранить черновик на этом устройстве.';}});
function clearDraft(){try{sessionStorage.removeItem(draftKey);draftState.textContent='';}catch{draftState.textContent='Запись сохранена, но черновик на устройстве не очищен.';}}
const message=error=>error?.message==='pending_storage_unavailable'?'Не удалось сохранить запрос на устройстве. Добавление не отправлено.':
  error?.status===401||error?.status===403?'Доступ к этому профилю закрыт. Вернитесь в Соты и проверьте профиль.':
  error?.status===503?'Приложение временно занято. Попробуйте ещё раз.':'Не удалось получить данные. Повторите подключение.';
function remember({requestId,contextRef,inputDigest}){try{if(!pendingKey)throw Error();sessionStorage.setItem(pendingKey,JSON.stringify({requestId,contextRef,inputDigest}));}catch{throw Error('pending_storage_unavailable');}}
function forget(){try{sessionStorage.removeItem(pendingKey);}catch{}}
async function refresh(cursor=null){const result=await client.query({requestId:id(),input:{operation:'reading.list',...(cursor?{cursor}:{})}});
  if(typeof result.storageScope!=='string'||!result.storageScope.length||result.storageScope.length>160||cursor&&result.storageScope!==currentScope
    ||!Array.isArray(result.items)||!(result.nextCursor===null||typeof result.nextCursor==='string'&&result.nextCursor.length<=193))throw Error('scope_invalid');
  currentScope=result.storageScope;pendingKey='reading-pending:'+result.storageScope;
  scopeDraft(result.storageScope);
  const list=$('items');if(!cursor)list.replaceChildren();for(const item of result.items){const li=document.createElement('li');li.textContent=item.title;list.append(li);}
  $('empty').hidden=list.children.length>0;nextCursor=result.nextCursor;more.hidden=!nextCursor;pageState.textContent='';}
async function connect(){if(busy)return;busy=true;retry.hidden=true;content.hidden=true;state.textContent='Подключаем ваш профиль…';
  try{await client.connect({requestId:id()});await refresh();unresolved=false;let prior;try{prior=JSON.parse(sessionStorage.getItem(pendingKey));}catch{}
    if(prior){
      if(typeof client.recoverReceipt!=='function'){unresolved=true;save.textContent='Предыдущая запись требует проверки в совместимой версии приложения.';}
      else{
      try{const receipt=await client.recoverReceipt(prior);if(receipt.outcome==='committed'){forget();save.textContent='Предыдущая запись сохранена.';}
        else{unresolved=true;save.textContent='Результат предыдущей записи пока неизвестен. Запрос не повторялся.';}}
      catch{unresolved=true;save.textContent='Результат предыдущей записи пока неизвестен. Запрос не повторялся.';}}
    }
    form.querySelector('button').disabled=unresolved;retry.hidden=!unresolved;retry.textContent=unresolved?'Проверить сохранение':'Повторить подключение';
    content.hidden=false;state.textContent='Ваши записи доступны только вашему профилю.';
  }catch(error){state.textContent=message(error);retry.hidden=false;}finally{busy=false;}}
form.addEventListener('submit',async event=>{event.preventDefault();if(busy||unresolved)return;busy=true;form.querySelector('button').disabled=true;
  try{const intent=await client.prepareInvoke({requestId:id(),input:{operation:'reading.add',title:$('title').value}});remember(intent);
    await client.invoke(intent);forget();clearDraft();title.value='';save.textContent='Сохранено';await refresh();}
  catch(error){unresolved=error?.effectUnknown===true;save.textContent=unresolved?'Проверяем результат. Повторное добавление не отправлено.':message(error);retry.hidden=false;retry.textContent=unresolved?'Проверить сохранение':'Повторить подключение';}
  finally{busy=false;form.querySelector('button').disabled=unresolved;}});
retry.addEventListener('click',connect);window.addEventListener('pagehide',()=>client.dispose(),{once:true});void connect();
more.addEventListener('click',async()=>{if(busy||!nextCursor)return;busy=true;more.disabled=true;pageState.textContent='';
  try{await refresh(nextCursor);}catch(error){pageState.textContent=message(error);}finally{busy=false;more.disabled=false;}});
