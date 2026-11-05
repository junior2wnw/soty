import {createSourceWorkspaceClient,createSotyAppWork} from '/assets/source-sdk.js';

const $=id=>document.getElementById(id),id=()=>crypto.randomUUID();
const client=createSourceWorkspaceClient(),title=$('title'),form=$('add'),submit=$('submit'),retry=$('retry');
const parentOrigin=document.querySelector('meta[name="soty-parent-origin"]').content;
const widgetDisplay=bindGlassWidgetContext(document.querySelector('main'),{parentOrigin});
let scope=null,draftKey=null,pendingKey=null,draftVersion=id(),pending=null,nextCursor=null;
let busy=false,unresolved=false,connectFlight=null,submitFlight=null,work=null;
const validText=value=>typeof value==='string'&&value.length<=240&&value.isWellFormed()&&!/[\u0000-\u001f\u007f]/u.test(value);
const validVersion=value=>typeof value==='string'&&/^[a-f0-9-]{36}$/u.test(value);
function changed(){submit.disabled=busy||unresolved||!scope||!client.context()||!title.value.trim();form.setAttribute('aria-busy',String(busy));work?.changed();}
function draftScope(value){
 if(scope===value)return;
 scope=value;draftKey='reading-widget:draft:'+scope;pendingKey='reading-widget:pending:'+scope;
 title.value='';draftVersion=id();$('draft-state').textContent='';
 try{const saved=JSON.parse(sessionStorage.getItem(draftKey));
  if(saved&&validText(saved.value)&&validVersion(saved.version)){title.value=saved.value;draftVersion=saved.version;}}
 catch{$('draft-state').textContent='Черновик на этом устройстве недоступен.';}
}
function persistDraft(){
 if(!draftKey)return;
 try{if(!validText(title.value))throw Error();sessionStorage.setItem(draftKey,JSON.stringify({version:draftVersion,value:title.value}));
  $('draft-state').textContent=title.value?'Черновик на этом устройстве':'';}
 catch{$('draft-state').textContent='Не удалось сохранить черновик на устройстве.';}
}
function clearCommittedDraft(version){
 // The user may type the next item while this request is in flight.
 if(draftVersion!==version)return;
 title.value='';draftVersion=id();$('draft-state').textContent='';
 try{sessionStorage.removeItem(draftKey);}catch{$('draft-state').textContent='Запись сохранена; локальный черновик не очищен.';}
}
title.addEventListener('input',()=>{draftVersion=id();persistDraft();changed();});
function remember(intent,version){
 const {requestId,contextRef,inputDigest}=intent;
 const value={requestId,contextRef,inputDigest,draftVersion:version};
 try{if(!pendingKey)throw Error();sessionStorage.setItem(pendingKey,JSON.stringify(value));}
 catch{throw Error('pending_storage_unavailable');}
 pending=value;
}
function forget(){pending=null;try{sessionStorage.removeItem(pendingKey);}catch{}}
const message=error=>error?.message==='pending_storage_unavailable'?'Запрос не отправлен: не удалось сохранить его на устройстве.':
 [401,403].includes(error?.status)?'Доступ закрыт. Вернитесь в Соты и проверьте профиль.':
 error?.status===503?'Приложение временно недоступно. Попробуйте ещё раз.':'Не удалось получить данные. Повторите подключение.';
async function refresh(cursor=null){
 const result=await client.query({requestId:id(),input:{operation:'reading.list',...(cursor?{cursor}:{})}});
 if(typeof result.storageScope!=='string'||!result.storageScope.length||result.storageScope.length>160
  ||cursor&&result.storageScope!==scope||!Array.isArray(result.items)
  ||!(result.nextCursor===null||typeof result.nextCursor==='string'&&result.nextCursor.length<=193)
  ||result.items.some(item=>typeof item?.id!=='string'||typeof item.title!=='string'||item.title.length>240))throw Error('reading_response_invalid');
 draftScope(result.storageScope);
 const list=$('items');if(!cursor)list.replaceChildren();
 for(const item of result.items){const row=document.createElement('li');row.textContent=item.title;list.append(row);}
 nextCursor=result.nextCursor;$('more').hidden=!nextCursor;$('empty').hidden=list.children.length>0;
 $('count').value=String(list.children.length)+(nextCursor?'+':'');
 $('latest').textContent=list.firstElementChild?.textContent??'Место для следующей хорошей идеи';
 $('page-state').textContent='';
}
async function recover(){
 let saved;try{saved=JSON.parse(sessionStorage.getItem(pendingKey));}catch{throw Error('pending_storage_unavailable');}
 if(!saved){pending=null;unresolved=false;return;}
 unresolved=true;pending=saved;
 if(typeof saved.requestId!=='string'||!validVersion(saved.draftVersion)
  ||typeof saved.contextRef!=='string'||typeof saved.inputDigest!=='string')throw Error('pending_invalid');
 const receipt=await client.recoverReceipt({requestId:saved.requestId,contextRef:saved.contextRef,inputDigest:saved.inputDigest});
 if(receipt.outcome!=='committed')throw Error('pending_unknown');
 clearCommittedDraft(saved.draftVersion);forget();unresolved=false;$('save-state').textContent='Запись сохранена.';
}
function connect(){
 if(connectFlight)return connectFlight;if(busy)return Promise.resolve();
 connectFlight=(async()=>{busy=true;retry.hidden=true;changed();$('state').textContent='Подключаем ваш список…';
  try{await client.connect({requestId:id()});await refresh();
   try{await recover();}catch{unresolved=true;$('save-state').textContent='Результат предыдущего добавления пока неизвестен. Запрос не повторялся.';}
   $('content').hidden=false;$('state').textContent='';retry.hidden=!unresolved;retry.textContent=unresolved?'Проверить сохранение':'Повторить';}
  catch(error){$('content').hidden=true;$('count').value='—';$('latest').textContent='Список недоступен';
   $('state').textContent=message(error);retry.hidden=false;retry.textContent='Повторить подключение';}
  finally{busy=false;connectFlight=null;changed();}})();return connectFlight;
}
function save(){
 if(submitFlight)return submitFlight;if(busy||unresolved||!scope||!title.value.trim())return Promise.resolve();
 const text=title.value,version=draftVersion;
 submitFlight=(async()=>{busy=true;changed();$('save-state').textContent='Сохраняем…';
  try{const intent=await client.prepareInvoke({requestId:id(),input:{operation:'reading.add',title:text}});
   remember(intent,version);await client.invoke(intent);forget();unresolved=false;clearCommittedDraft(version);
   $('save-state').textContent='Сохранено';await refresh();}
  catch(error){unresolved=error?.effectUnknown===true;
   $('save-state').textContent=unresolved?'Проверяем результат. Повторное добавление не отправлено.':message(error);
   retry.hidden=false;retry.textContent=unresolved?'Проверить сохранение':'Повторить подключение';}
  finally{busy=false;submitFlight=null;changed();}})();return submitFlight;
}
form.addEventListener('submit',event=>{event.preventDefault();if(form.reportValidity())void save();});
retry.addEventListener('click',()=>void connect());
$('more').addEventListener('click',async()=>{if(busy||!nextCursor)return;busy=true;changed();$('more').disabled=true;
 try{await refresh(nextCursor);}catch(error){$('page-state').textContent=message(error);}
 finally{busy=false;$('more').disabled=false;changed();}});
if(window.parent!==window){
 work=createSotyAppWork({parentOrigin,
  isDirty:()=>!!title.value||!!pending||!!submitFlight,
  save:async()=>{await connectFlight;await submitFlight;if(unresolved)await connect();
   if(unresolved)throw Error('reading_save_unknown');await save();if(unresolved)throw Error('reading_save_unknown');}});
}
window.addEventListener('pagehide',()=>{widgetDisplay.dispose();work?.dispose();client.dispose();},{once:true});
void connect();
