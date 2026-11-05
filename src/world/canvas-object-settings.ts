import {button,el,labeledField} from './dom';
import {pinDialogSubmit,type WorldDialog} from './dialogs';
import type {DirectoryApi} from './field-directory';
import type {CanvasObject,CanvasMask} from '../../modules/canvas-objects/contract.mjs';
import type {CanvasProjection,CanvasWidgetView} from './canvas-field-content';
import {mountCanvasObjectSecurity,type CanvasObjectSecurityActions} from './canvas-object-security';
import {CANVAS_VAULT_NOTE_TYPE,type CanvasVaultClient} from './canvas-vault-client.mjs';
import './canvas-object-settings.css';

interface Options {
  api:DirectoryApi;accountId:string;objectId:string;title:string;projection:CanvasProjection|null;isCurrent():boolean;
  onChanged():Promise<void>;onOpen():void;onMove?():void;onRemovePlacement?():void;beforeChange?():Promise<void>;
  security?:CanvasObjectSecurityActions;
  vault?:CanvasVaultClient;
}
const message=(error:unknown)=>{
  const code=String((error as {code?:unknown})?.code??'');
  return /revision_conflict/u.test(code)?'Объект изменён в другом окне. Закройте настройки и откройте снова.'
    :/account_changed|authentication|access_denied|revoked/u.test(code)?'Доступ изменился. Откройте полотно заново.'
    :/recipient/u.test(code)?'Выберите хотя бы одного получателя.'
    :code==='canvas_unsaved_work'?'Сначала сохраните изменения в открытых объектах.'
    :'Не удалось сохранить. Повторите — продолжим то же действие.';
};
const sticker:CanvasMask={shape:'polygon',points:[{x:.06,y:.04},{x:.93,y:0},{x:1,y:.86},{x:.79,y:1},{x:0,y:.92}]};

/** Private copies are opened deliberately. A view/copy never overwrites the
 * current Source, and each export rechecks the actual current SDK grant. */
export function mountCanvasRecoveryCopies(host:HTMLElement,options:{vault:CanvasVaultClient;objectId:string;isCurrent():boolean}){
  const details=el('details','co-recovery'),summary=el('summary','','Копии на этом устройстве'),list=el('div','co-recovery-list'),status=el('p','co-status');
  const preview=el('textarea','co-recovery-preview'),copy=button('Копировать текст','copy','sw-button-quiet'),retry=button('Повторить','refresh','sw-button-quiet');preview.readOnly=true;preview.hidden=copy.hidden=retry.hidden=true;preview.setAttribute('aria-label','Прежняя запись');status.setAttribute('role','status');
  details.append(summary,status,retry,list,preview,copy);host.append(details);let disposed=false,busy=false,loaded=false,generation=0,selected:{archiveId:string;operationId:string}|null=null;const urls=new Set<string>();
  const current=()=>!disposed&&options.isCurrent()&&options.vault.isUnlocked(options.objectId);
  function scrub(){generation++;selected=null;preview.value='';preview.hidden=copy.hidden=true;list.replaceChildren();status.textContent='';for(const url of urls)URL.revokeObjectURL(url);urls.clear();details.open=false;details.hidden=true;}
  details.hidden=!options.vault.isUnlocked(options.objectId);
  const unsubscribe=options.vault.subscribe((id,reason)=>{if(id!==options.objectId)return;if(reason!=='unlocked')scrub();else if(!disposed&&options.isCurrent()){loaded=false;details.hidden=false;}});
  const text=(record:{sourceIntent:Record<string,any>|null})=>{const note=record.sourceIntent;return[note?.title,note?.body,...(Array.isArray(note?.items)?note.items.map((item:{text:string;done:boolean})=>`${item.done?'☑':'☐'} ${item.text}`):[])].filter(Boolean).join('\n\n');};
  async function entry(id:{archiveId:string;operationId:string}){
    if(!current())throw Error('canvas_access_denied');const ticket=generation,values=await options.vault.creationRecoveries(options.objectId,{archiveId:id.archiveId});
    if(!current()||ticket!==generation)throw Error('canvas_access_denied');const record=values.flatMap(value=>value.records).find(value=>value.operationId===id.operationId);
    if(!record?.sourceIntent)throw Error('vault_native_recovery_unavailable');return record;
  }
  function fail(){if(current()){loaded=false;retry.hidden=false;status.textContent='Не удалось открыть копию. Она сохранена; повторите позже.';}}
  async function reveal(id:{archiveId:string;operationId:string},file:boolean){
    if(!current()||busy)return;busy=true;const ticket=generation;status.textContent='Открываем…';
    try{const record=await entry(id);if(!current()||ticket!==generation)return;
      if(file){const blob=record.sourceIntent?.blob;if(!(blob instanceof Blob))throw Error('vault_native_recovery_unavailable');
        const url=URL.createObjectURL(blob),anchor=el('a');urls.add(url);anchor.href=url;anchor.download=String(record.sourceIntent?.name||(typeof File==='function'&&blob instanceof File?blob.name:'Копия')).replace(/[\\/\u0000-\u001f]/gu,'_').slice(0,180);anchor.hidden=true;details.append(anchor);anchor.click();anchor.remove();
        setTimeout(()=>{if(urls.delete(url))URL.revokeObjectURL(url);},30000);status.textContent='Копия готова';
      }else{selected=id;preview.value=text(record);preview.hidden=copy.hidden=false;status.textContent='Текущая запись остаётся прежней';}
    }catch{fail();}finally{busy=false;}
  }
  async function load(){
    if(!current()||busy||loaded)return;busy=true;retry.hidden=true;const ticket=generation;status.textContent='Ищем копии…';
    try{const values=await options.vault.creationRecoveries(options.objectId);if(!current()||ticket!==generation)return;list.replaceChildren();
      for(const value of values)for(const record of value.records){const note=record.kind==='note'&&typeof record.sourceIntent?.body==='string',file=record.sourceIntent?.blob instanceof Blob;if(!note&&!file)continue;
        const id={archiveId:value.archiveId,operationId:record.operationId},action=button(file?'Скачать прежний файл':'Посмотреть прежнюю запись',file?'download':'external','sw-button-quiet',()=>{void reveal(id,file);});list.append(action);
      }
      loaded=true;status.textContent=list.childElementCount?'':'Прежних копий нет';
    }catch{fail();}finally{busy=false;}
  }
  details.addEventListener('toggle',()=>{if(details.open)void load();});
  retry.addEventListener('click',()=>{void load();});
  copy.addEventListener('click',()=>{if(!current()||!selected||busy)return;const id=selected,ticket=generation;busy=true;
    void(async()=>{try{const record=await entry(id);if(!current()||ticket!==generation)return;const value=text(record);
      if(navigator.clipboard?.writeText)await navigator.clipboard.writeText(value);else{preview.value=value;preview.focus();preview.select();if(!document.execCommand('copy'))throw Error('copy_unavailable');}
      if(current()&&ticket===generation)status.textContent='Текст скопирован';
    }catch{fail();}finally{busy=false;}})();
  });
  return{element:details,dispose(){if(disposed)return;disposed=true;unsubscribe();scrub();details.remove();}};
}

/** These controls mutate the canonical object, never copied canvas metadata. */
export function mountCanvasObjectSettings(dialog:WorldDialog,options:Options){
  const abort=new AbortController(),signal=abort.signal;let disposed=false,busy=false,object:CanvasObject|null=null;
  let pending:{op:string;args:Record<string,unknown>}|null=null,securityBusy=false,securityPending=false;
  let security:ReturnType<typeof mountCanvasObjectSecurity>|undefined;
  let copies:ReturnType<typeof mountCanvasRecoveryCopies>|undefined;
  let unsubscribeVault:(()=>void)|undefined;
  const current=()=>!disposed&&options.isCurrent();
  const form=el('form','co-settings'),state=el('p','co-status','Открываем настройки…'),error=el('p','co-error');error.setAttribute('role','alert');error.hidden=true;state.setAttribute('role','status');
  const quick=el('div','co-quick'),open=button('Открыть','external','sw-button-quiet',()=>{dialog.close();options.onOpen();});quick.append(open);
  if(options.onMove)quick.append(button('Переместить','sliders','sw-button-quiet',()=>{dialog.close();options.onMove?.();}));
  if(options.onRemovePlacement)quick.append(button('Убрать с поля','close','sw-button-quiet',()=>{dialog.close();options.onRemovePlacement?.();}));
  const fields=el('fieldset');fields.hidden=true;
  const shape=el('select');shape.setAttribute('aria-label','Форма объекта');for(const [value,label]of [['rect','Карточка'],['ellipse','Круг'],['hex','Сота'],['sticker','Стикер'],['keep','Свой контур']] as const){const option=el('option','',label);option.value=value;shape.append(option);}
  const size=el('input'),sizeValue=el('output');size.type='range';size.min='8';size.max='128';size.step='1';size.setAttribute('aria-label','Размер шрифта');size.addEventListener('input',()=>{sizeValue.textContent=`${size.value} px`;},{signal});const fontRow=labeledField('Шрифт',size);fontRow.append(sizeValue);const view=options.projection?.view; fontRow.hidden=!(view?.kind==='note'||view?.kind==='vault'&&view.contentType===CANVAS_VAULT_NOTE_TYPE);
  const audience=el('select');audience.setAttribute('aria-label','Кому видно');for(const [value,label]of [['private','Только мне'],['public','Всем'],['restricted','Выбранным людям и комнатам']] as const){const option=el('option','',label);option.value=value;audience.append(option);}
  const recipients=el('div','co-recipients'),recipientChecks=new Map<string,{input:HTMLInputElement;kind:'account'|'community';id:string}>();recipients.hidden=true;
  audience.addEventListener('change',()=>{recipients.hidden=audience.value!=='restricted';},{signal});
  const privacy=el('details'),privacyTitle=el('summary','','Видимость на полотне');privacy.append(privacyTitle);
  const pageAccess=el('div','co-page-access'),pageStatus=el('p','co-status'),onlyMe=button('Оставить только мне','lock','sw-button-quiet');pageAccess.hidden=true;pageAccess.append(pageStatus,onlyMe);privacy.append(pageAccess);
  const own=el('input'),others=el('input');own.type=others.type='checkbox';
  for(const [control,label]of [[own,'Скрыть с моего полотна'],[others,'Не показывать другим']] as const){const row=el('label','co-check');row.append(control,el('span','',label));privacy.append(row);}
  const refreshSource=button('Обновить источник приложения','refresh','sw-button-quiet');refreshSource.hidden=true;refreshSource.dataset.fieldControl='';
  fields.append(labeledField('Форма',shape),fontRow,labeledField('Кому видно',audience),recipients,privacy,refreshSource);form.append(quick,state,error,fields);dialog.body.append(form);
  const save=button('Сохранить',undefined,'sw-button-primary');save.disabled=true;pinDialogSubmit(dialog,form,save);
  const paint=()=>{fields.disabled=busy||!!pending||securityBusy;save.disabled=busy||!object||securityBusy||securityPending;save.textContent=busy?'Сохраняем…':pending?'Повторить':'Сохранить';};
  async function perform(command:{op:string;args:Record<string,unknown>}){
    if(busy||securityBusy||securityPending||!current())return;busy=true;pending=command;error.hidden=true;paint();
    try{await options.beforeChange?.();if(!current())return;await options.api.request(command.op,command.args);if(!current())return;pending=null;await options.onChanged();if(current())dialog.close();}
    catch(reason){if(current()){error.textContent=message(reason);error.hidden=false;}}
    finally{busy=false;if(current())paint();}
  }
  function command():{op:string;args:Record<string,unknown>}{
    if(!object)throw new Error('canvas_owner_required');
    const selected=[...recipientChecks.values()].filter(value=>value.input.checked),restricted=audience.value==='restricted';
    if(restricted&&!selected.length)throw Object.assign(new Error('recipients'),{code:'canvas_recipient_required'});
    const mask=shape.value==='keep'?object.presentation.mask:shape.value==='sticker'?sticker:{shape:shape.value} as CanvasMask;
    return{op:'canvas.objects.update',args:{expectedAccountId:options.accountId,requestId:crypto.randomUUID(),objectId:object.objectId,expectedRevision:object.revision,
      presentation:{mask,...(fontRow.hidden?object.presentation.fontSize===undefined?{}:{fontSize:object.presentation.fontSize}:{fontSize:Number(size.value)})},
      visibility:{audience:audience.value,accountIds:restricted?selected.filter(value=>value.kind==='account').map(value=>value.id):[],communityIds:restricted?selected.filter(value=>value.kind==='community').map(value=>value.id):[]},hidden:{owner:own.checked,others:others.checked}}};
  }
  form.addEventListener('submit',event=>{event.preventDefault();if(busy||!current())return;try{void perform(pending??command());}catch(reason){error.textContent=message(reason);error.hidden=false;}},{signal});
  refreshSource.addEventListener('click',()=>{if(busy||!object)return;void(async()=>{try{
    const widget=(await options.api.request<{widget:{widgetId:string;revision:number}}>('canvas.widgets.get',{expectedAccountId:options.accountId,widgetId:object!.binding.ref})).widget;
    if(current())await perform({op:'canvas.widgets.refresh',args:{expectedAccountId:options.accountId,widgetId:widget.widgetId,expectedRevision:widget.revision,requestId:crypto.randomUUID()}});
  }catch(reason){if(current()){error.textContent=message(reason);error.hidden=false;}}})();},{signal});
  onlyMe.addEventListener('click',()=>{if(!current()||!object||busy)return;void perform({op:'canvas.objects.update',args:{expectedAccountId:options.accountId,requestId:crypto.randomUUID(),objectId:object.objectId,expectedRevision:object.revision,visibility:{audience:'private',accountIds:[],communityIds:[]},revokePublication:true}});},{signal});
  function addRecipient(kind:'account'|'community',id:string,label:string,checked:boolean){const key=`${kind}:${id}`;if(recipientChecks.has(key))return;const input=el('input');input.type='checkbox';input.checked=checked;const row=el('label','co-check');row.append(input,el('span','',label));recipients.append(row);recipientChecks.set(key,{input,kind,id});}
  void(async()=>{try{
    const result=await options.api.request<{object:CanvasObject}>('canvas.objects.get',{expectedAccountId:options.accountId,objectId:options.objectId});if(!current())return;
    object=result.object;fields.hidden=false;state.textContent='';state.hidden=true;
    if(options.security&&object.ownerId===options.accountId){const host=el('div');dialog.body.append(host);security=mountCanvasObjectSecurity(host,{actions:options.security,isCurrent:current,...(options.beforeChange?{beforeChange:options.beforeChange}:{}),
      onBusyChange:value=>{securityBusy=value;paint();},onPendingChange:value=>{securityPending=value;paint();},onDone:async()=>{await options.onChanged();if(current())dialog.close();}});}
    if(options.vault&&object.protection.kind==='password'&&['soty.notes','soty.materials'].includes(object.binding.providerId))copies=mountCanvasRecoveryCopies(dialog.body,{vault:options.vault,objectId:object.objectId,isCurrent:current});
    // status() itself reports a locked SDK state. Reading it again in response
    // would turn an already closed object into a self-sustaining request loop.
    if(options.vault)unsubscribeVault=options.vault.subscribe((id,reason)=>{if(id===options.objectId&&reason!=='unlocked'&&!(reason==='locked'&&security?.isLoadingStatus()))void security?.refresh();});
    shape.value=object.presentation.mask.shape==='polygon'?'keep':object.presentation.mask.shape;
    size.value=String(object.presentation.fontSize??16);sizeValue.textContent=`${size.value} px`;audience.value=object.visibility.audience;recipients.hidden=audience.value!=='restricted';own.checked=object.hidden.owner;others.checked=object.hidden.others;
    const widget=options.projection?.view?.kind==='widget'?options.projection.view as CanvasWidgetView:null;refreshSource.hidden=widget?.state!=='needs-refresh'||!widget.canRefresh;
    const results=await Promise.allSettled([options.api.request<{contacts:{peerAccountId:string;label:string}[]}>('contacts.list',{}),options.api.request<{communities:{communityId:string;name:string;permissions?:{canManage?:boolean}}[]}>('world.community.list',{}),options.api.request<{page:{enabled:boolean;audience:string;selection?:{items:{entity:{kind:string;id:string};expectedAccessRevision?:number}[]}}|null}>('canvas.pages.get',{expectedAccountId:options.accountId})]);if(!current())return;
    if(results[2].status==='fulfilled'){const page=results[2].value.page,selected=page?.enabled&&page.audience!=='private'&&(page.selection?page.selection.items.some(item=>item.entity.kind==='object'&&item.entity.id===object!.objectId&&item.expectedAccessRevision===(object!.accessRevision??object!.revision)):object.visibility.audience==='public'&&!object.hidden.others);
      if(selected){const nativePage=Boolean(page.selection)&&object.ownerId===options.accountId&&['soty.notes','soty.materials','soty.vault'].includes(object.binding.providerId)&&!object.hidden.others;pageAccess.hidden=false;pageStatus.textContent=nativePage?'Этот объект также выбран для вашей открытой страницы.':'Этот объект выбран для страницы. Доступ посетителей к окну и приложению проверяется отдельно.';const option=[...audience.options].find(item=>item.value==='private');if(option&&nativePage)option.textContent='Лично + выбранная страница';}}
    if(results[0].status==='fulfilled')for(const contact of results[0].value.contacts)addRecipient('account',contact.peerAccountId,contact.label,object.visibility.accountIds.includes(contact.peerAccountId));
    if(results[1].status==='fulfilled')for(const community of results[1].value.communities)if(community.permissions?.canManage)addRecipient('community',community.communityId,community.name,object.visibility.communityIds.includes(community.communityId));
    // Preserve confirmed recipients even if an optional directory is offline.
    for(const id of object.visibility.accountIds)addRecipient('account',id,'Сохранённый получатель',true);
    for(const id of object.visibility.communityIds)addRecipient('community',id,'Сохранённая комната',true);
    if(!recipientChecks.size)recipients.append(el('p','sw-muted','Сначала добавьте контакт или создайте комнату.'));paint();
  }catch(reason){if(!current())return;state.textContent=String((reason as {code?:string})?.code)==='canvas_owner_required'?'Этот объект принадлежит другому человеку.':'Не удалось открыть настройки. Попробуйте ещё раз.';}})();
  return{hasUnsavedChanges:()=>security?.hasUnsavedChanges()===true,dispose(){disposed=true;abort.abort();unsubscribeVault?.();security?.dispose();copies?.dispose();}};
}

export function mountCanvasHiddenObjects(dialog:WorldDialog,options:Pick<Options,'api'|'accountId'|'isCurrent'|'onChanged'>){
  let disposed=false,busy=false,offset:number|null=0;const list=el('div','co-hidden-list'),status=el('p','co-status','Ищем скрытые объекты…'),more=button('Показать ещё');more.hidden=true;dialog.body.append(list,status,more);
  const current=()=>!disposed&&options.isCurrent();
  async function load(){if(busy||offset===null||!current())return;busy=true;more.disabled=true;try{
    const result=await options.api.request<{objects:CanvasObject[];nextOffset:number|null}>('canvas.objects.list',{expectedAccountId:options.accountId,state:'active',offset});if(!current())return;
    for(const object of result.objects)if(object.hidden.owner){const row=el('div','co-hidden-row'),restore=button('Вернуть','undo'),requestId=crypto.randomUUID();row.append(el('span','',object.binding.providerId==='soty.notes'?'Записка':object.binding.providerId==='soty.apps'?'Окно приложения':'Скрытый материал'),restore);list.append(row);
      restore.addEventListener('click',()=>{restore.disabled=true;void options.api.request('canvas.objects.update',{expectedAccountId:options.accountId,objectId:object.objectId,expectedRevision:object.revision,requestId,hidden:{...object.hidden,owner:false}}).then(async()=>{if(current()){row.remove();await options.onChanged();status.textContent=list.childElementCount?'':'Все объекты возвращены';}}).catch(reason=>{if(current()){status.textContent=message(reason);restore.disabled=false;}});});}
    offset=result.nextOffset;more.hidden=offset===null;status.textContent=list.childElementCount?'':offset===null?'Скрытых объектов нет':'Продолжаем поиск…';if(!list.childElementCount&&offset!==null)queueMicrotask(()=>{void load();});
  }catch(reason){if(current())status.textContent=message(reason);}finally{busy=false;if(current())more.disabled=false;}}
  more.addEventListener('click',()=>{void load();});void load();return{dispose(){disposed=true;}};
}
