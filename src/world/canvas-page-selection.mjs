import {convertFieldDocumentToV2,fieldEntityKey} from '../../modules/field/contract.mjs';

const check=(value,code='canvas_page_response_invalid')=>{if(!value)throw Object.assign(new Error(code),{code});};
const active=current=>check(current(),'canvas_page_cancelled');
const pin=value=>value?.accessRevision??value?.revision;
const titleFor=view=>view?.kind==='widget'?view.app.name:view?.kind==='note'?view.note?.title||'Запись':view?.kind==='image'?view.material?.name||'Фотография':view?.kind==='video'?view.material?.name||'Видео':view?.kind==='audio'?view.material?.name||'Аудио':'Под паролем';
function visitorAccess({object,view,app,accountId,password=false,available=true}){
 const native=object?.ownerId===accountId&&['soty.notes','soty.materials','soty.vault'].includes(object.binding?.providerId);
 const owned=object?.ownerId===accountId,application=app??(view?.kind==='widget'?view.app:null),closed=application?.audience==='private'||application?.audience===null;
 const appAction=application?.ownerAccountId===accountId&&typeof application.appId==='string'?{kind:'app',id:application.appId}:application?.ownerAccountId===accountId&&typeof application.id==='string'?{kind:'app',id:application.id}:undefined;
 if(!available)return{visitorAccess:'unavailable',hint:'Источник недоступен. Обновите выбор или уберите его.'};
 if(object&&object.visibility?.audience!=='public'&&!native)return{visitorAccess:'blocked',hint:closed?'Выбрано, но окно остаётся личным. Приложение тоже закрыто для посетителей.':'Выбрано, но без личного доступа посетители не увидят окно.',...(owned?{accessTarget:{kind:'object',id:object.objectId}}:{})};
 if(closed)return{visitorAccess:'blocked',hint:'Посетители не увидят окно: приложение закрыто. Выбор на странице его не открывает.',...(appAction?{accessTarget:appAction}:{})};
 if(password)return{visitorAccess:'locked',hint:'Гости увидят замок. Пароль не раскрывает личный доступ.'};
 if(native&&object.visibility?.audience==='private')return{visitorAccess:'page-only',hint:'Показывать только на этой странице'};
 if(view?.kind==='widget')return{visitorAccess:'conditional',hint:'Доступ к приложению сохраняется. Выбор не даёт гостям вход в приложение.'};
 if(application)return{visitorAccess:'conditional',hint:'Доступ к приложению сохраняется. Проверьте, как его видят гости.'};
 if(object?.visibility?.audience==='public')return{visitorAccess:'normal',hint:'Объект доступен по своей настройке видимости.'};
 return{visitorAccess:'unknown',hint:'Доступ посетителей не подтверждён. Проверьте предпросмотр.'};
}
export const canvasPageSelectionKey=items=>JSON.stringify([...items].map(item=>[item.shortcutId,fieldEntityKey(item.entity),item.expectedAccessRevision??null]).sort(([a],[b])=>a.localeCompare(b)));

/** Canonical placements only. Discovery and owner app lists cannot silently
 * turn into personal placements or a publication. Missing selected rows stay
 * explicit until the owner removes them or obtains a fresh Source projection. */
export async function loadCanvasPageChoices({api,accountId,isCurrent=()=>true,prepare}={}){
 active(isCurrent);await prepare?.();active(isCurrent);
 const envelope=await api.request('world.field.get',{expectedAccountId:accountId});active(isCurrent);
 check(Number.isSafeInteger(envelope?.revision)&&envelope.revision>=0);
 const field=convertFieldDocumentToV2(envelope.document),refs=[...new Map(field.shortcuts.filter(item=>['object','app'].includes(item.entity.kind)).map(item=>[fieldEntityKey(item.entity),item.entity])).values()];
 const projections=new Map(),objects=new Map(),apps=new Map();
 const objectIds=refs.filter(item=>item.kind==='object').map(item=>item.id),appIds=refs.filter(item=>item.kind==='app').map(item=>item.id);
 for(let offset=0;offset<objectIds.length;offset+=32){
  const batch=objectIds.slice(offset,offset+32),result=await api.request('canvas.objects.project',{expectedAccountId:accountId,objectIds:batch});active(isCurrent);
  check(Array.isArray(result?.items));for(const item of result.items)projections.set(item.objectId,item);
  // Management metadata supplies privacy and the provider. Foreign permitted
  // placements still use the projection's access revision, never an owner actor.
  for(let at=0;at<batch.length;at+=8){await Promise.all(batch.slice(at,at+8).map(async objectId=>{
   try{const value=await api.request('canvas.objects.get',{expectedAccountId:accountId,objectId});active(isCurrent);objects.set(objectId,value.object);}
   catch(error){active(isCurrent);if(!/owner_required|access_denied|unavailable/u.test(String(error?.code??'')))throw error;}
  }));active(isCurrent);}
 }
 for(let offset=0;offset<appIds.length;offset+=60){const result=await api.request('apps.directory.resolve',{expectedAccountId:accountId,appIds:appIds.slice(offset,offset+60)});active(isCurrent);check(Array.isArray(result?.items));for(const item of result.items)if(item.available&&item.app)apps.set(item.ref.id,item.app);}
 const choices=field.shortcuts.filter(item=>['object','app'].includes(item.entity.kind)).map(item=>{
   if(item.entity.kind==='app'){const app=apps.get(item.entity.id);return{shortcutId:item.shortcutId,entity:item.entity,title:app?.name??'Недоступное приложение',kind:'app',mask:{shape:'hex'},available:!!app,defaultSelected:app?.access==='public',...visitorAccess({app,accountId,available:!!app}),snippet:''};}
  const value=projections.get(item.entity.id),object=objects.get(item.entity.id),view=value?.view,revision=pin(value),available=value?.available===true&&Number.isSafeInteger(revision)&&revision>=1&&object?.hidden?.others!==true&&object?.state!=='removed';
   const password=value?.protection?.kind==='password';
  return{shortcutId:item.shortcutId,entity:item.entity,title:value?.available?titleFor(view):'Недоступный объект',kind:password?'locked':view?.kind??'object',mask:value?.presentation?.mask??{shape:'rect'},available,
   ...(Number.isSafeInteger(revision)&&revision>=1?{expectedAccessRevision:revision}:{}),defaultSelected:available&&object?.visibility?.audience==='public'&&!object.hidden.others,
    ...visitorAccess({object,view,accountId,password,available}),snippet:view?.kind==='note'?view.note?.preview??'':''};
 });
 return{fieldRevision:envelope.revision,choices,excludedCount:field.shortcuts.length-choices.length};
}

export function createCanvasPageSelection(snapshot,selectedIds){
 const selected=new Set(selectedIds),items=[];
 for(const choice of snapshot.choices)if(selected.delete(choice.shortcutId)){
  check(choice.available,'canvas_page_selection_unavailable');
  items.push({shortcutId:choice.shortcutId,entity:{...choice.entity},...(choice.entity.kind==='object'?{expectedAccessRevision:choice.expectedAccessRevision}:{})});
 }
 check(!selected.size,'canvas_page_selection_unavailable');
 return{schema:'soty.canvas-page-selection.v1',fieldRevision:snapshot.fieldRevision,items};
}
