import {button,el,labeledField} from './dom';
import {icon} from './icons';
import {pinDialogSubmit,type WorldDialog} from './dialogs';
import type {DirectoryApi} from './field-directory';
import type {CanvasPage,CanvasPageSelection} from '../../modules/canvas-pages/server.mjs';
import {canvasPageLink} from './canvas-page-client.mjs';
import {loadCanvasPageChoices,createCanvasPageSelection,canvasPageSelectionKey,type CanvasPageChoiceSnapshot} from './canvas-page-selection.mjs';
import './canvas-page-share.css';

export interface CanvasPageShareOptions {api:DirectoryApi;accountId:string;isCurrent():boolean;prepare?:()=>Promise<void>;onPreview(pageId:string):void;onOpenAppAccess?:(appId:string)=>Promise<void>;onOpenObjectAccess?:(objectId:string)=>Promise<void>}
export function mountCanvasPageShare(dialog:WorldDialog,options:CanvasPageShareOptions){
 const abort=new AbortController(),signal=abort.signal,current=()=>!signal.aborted&&options.isCurrent();
 const form=el('form','cp-share'),status=el('p','cp-share-status','Открываем настройки…'),error=el('p','co-error');error.hidden=true;error.setAttribute('role','alert');status.setAttribute('role','status');
 const fields=el('fieldset'),audience=el('select');fields.hidden=true;audience.setAttribute('aria-label','Доступ к полотну');
 for(const[value,label]of [['private','Только мне'],['unlisted','По ссылке'],['public','Всем']] as const){const option=el('option','',label);option.value=value;audience.append(option);}
 const explanation=el('p','sw-muted','Выберите, что увидят гости. Новые объекты остаются личными.'),count=el('strong','cp-share-count'),choices=el('div','cp-share-choices'),notice=el('p','cp-share-notice');
 const selection=el('section','cp-share-selection'),selectionHead=el('div','cp-share-selection-head'),refresh=button('Обновить выбор','refresh','sw-button-quiet');selectionHead.append(count,refresh);selection.append(selectionHead,choices,notice);
 const link=el('input');link.type='url';link.readOnly=true;link.setAttribute('aria-label','Ссылка на полотно');
 const actions=el('div','cp-share-actions'),copy=button('Копировать','copy','sw-button-quiet'),preview=button('Как видят гости','external','sw-button-quiet');actions.append(copy,preview);
 const qr=el('canvas','cp-share-qr');qr.setAttribute('role','img');qr.setAttribute('aria-label','QR-код ссылки на полотно');
 const result=el('section','cp-share-result');result.hidden=true;result.append(link,actions,qr);
  const takeCurrent=button('Взять опубликованный выбор',undefined,'sw-button-quiet cp-share-take-current');takeCurrent.hidden=true;
  fields.append(labeledField('Доступ',audience),explanation,selection);form.append(status,error,fields,takeCurrent,result);dialog.body.append(form);
 const save=button('Сохранить',undefined,'sw-button-primary');save.disabled=true;pinDialogSubmit(dialog,form,save);
 let page:CanvasPage|null=null,loaded=false,busy=false,choiceLoading=false,pending:Record<string,unknown>|null=null,qrGeneration=0,loadGeneration=0,selectionAvailable=false;
 let snapshot:CanvasPageChoiceSnapshot|null=null,baselineKey='',selected=new Map<string,CanvasPageSelection['items'][number]>();
  let draftRevision=0,baselineAudience='private',draftGeneration=0,accessBusy=false;
  let conflict:{requestId:string;expectedRevision:number;freshRevision:number|null}|null=null;
 const mode=()=>page?.enabled&&page.audience!=='private'?page.audience:'private';
 const selectedItems=()=>[...selected.values()];
  const changed=()=>audience.value!==baselineAudience||audience.value!=='private'&&canvasPageSelectionKey(selectedItems())!==baselineKey;
  const visitorWarnings=()=>snapshot?.choices.filter(choice=>selected.has(choice.shortcutId)&&['blocked','conditional','unknown'].includes(choice.visitorAccess)).length??0;
 function selectedIssue(){return!snapshot||[...selected].some(([id,entry])=>{const choice=snapshot!.choices.find(item=>item.shortcutId===id);return!choice?.available||choice.expectedAccessRevision!==entry.expectedAccessRevision||choice.entity.kind!==entry.entity.kind||choice.entity.id!==entry.entity.id;});}
 const paint=()=>{
   fields.disabled=busy||accessBusy||!!pending||!loaded;refresh.disabled=busy||accessBusy||choiceLoading||!!pending;selection.hidden=audience.value==='private';choices.inert=choiceLoading;
   save.disabled=!loaded||busy||accessBusy||!pending&&(!changed()||!!conflict&&conflict.freshRevision===null||audience.value!=='private'&&(choiceLoading||!selectionAvailable||selectedIssue()));
   save.textContent=busy?'Сохраняем…':pending?'Повторить':conflict&&conflict.freshRevision!==null?'Сохранить мой выбор':audience.value==='private'&&mode()!=='private'?'Закрыть доступ':'Сохранить';
   takeCurrent.hidden=!conflict;takeCurrent.disabled=busy||accessBusy||choiceLoading||!!pending;
   const warnings=visitorWarnings();count.textContent=`Выбрано: ${selected.size}${warnings?` · Проверьте доступ: ${warnings}`:''}`;
  if(choiceLoading)notice.textContent='Загружаем объекты…';
  else if(selectionAvailable&&snapshot){const missing=[...selected].some(([id])=>!snapshot!.choices.find(item=>item.shortcutId===id)?.available),stale=!missing&&selectedIssue();
    notice.textContent=missing?'Выбранный объект недоступен. Уберите его или обновите выбор.':stale?'Сота или её доступ изменились. Снимите отметку и выберите её заново.':!selected.size?'На странице будет только ваше имя — без объектов.':warnings?'Выбор не открывает закрытые окна и приложения. Проверьте подсказки у отмеченных объектов.':snapshot.excludedCount?'Личные разговоры, комнаты и устройства остаются у вас.':'';
  }
 };
 function present(){
  result.hidden=!page?.enabled||page.audience==='private';
  if(result.hidden){link.value='';qr.width=qr.height=0;qrGeneration++;return;}
  link.value=canvasPageLink(page!.pageId,location.origin);const generation=++qrGeneration;qr.hidden=false;
  void import('qrcode').then(({default:QRCode})=>{if(current()&&generation===qrGeneration)return QRCode.toCanvas(qr,link.value,{width:140,margin:1,errorCorrectionLevel:'M'});}).catch(()=>{if(current()&&generation===qrGeneration)qr.hidden=true;});
 }
 function renderChoices(){
  choices.replaceChildren();if(!snapshot)return;
  const byId=new Map(snapshot.choices.map(item=>[item.shortcutId,item]));
   for(const [id]of selected)if(!byId.has(id)){const row=el('label','cp-share-choice is-unavailable'),input=el('input');input.type='checkbox';input.checked=true;input.setAttribute('aria-label','Убранный с полотна объект');row.append(input,el('span','','Объект убран с полотна'));choices.append(row);input.addEventListener('change',()=>{draftGeneration++;selected.delete(id);row.remove();paint();},{signal});}
   for(const choice of snapshot.choices){
    const row=el('div',`cp-share-choice${choice.available?'':' is-unavailable'}`),label=el('label','cp-share-choice-content'),input=el('input'),tile=el('span','cp-share-tile'),title=el('strong','',choice.title),hint=el('span','cp-share-choice-hint',choice.hint);input.type='checkbox';input.checked=selected.has(choice.shortcutId);input.disabled=!choice.available&&!input.checked;input.setAttribute('aria-label',choice.title);row.dataset.visitorAccess=choice.visitorAccess;
   tile.dataset.shape=choice.mask.shape;tile.dataset.kind=choice.kind;tile.append(icon(({note:'note',image:'image',video:'video',audio:'mic',locked:'shield',widget:'app',app:'app'} as Record<string,string>)[choice.kind]??'layers'));
   if(choice.snippet)tile.append(el('span','cp-share-snippet',choice.snippet));
    label.append(input,tile,title,hint);row.append(label);choices.append(row);
    const target=choice.accessTarget,openAccess=target?.kind==='app'?options.onOpenAppAccess:target?.kind==='object'?options.onOpenObjectAccess:undefined;
    if(choice.available&&target&&openAccess){const action=button(target.kind==='app'?'Доступ приложения…':'Доступ окна…',undefined,'sw-button-quiet cp-share-access',()=>{
      if(!current()||busy||accessBusy||choiceLoading||pending)return;accessBusy=true;paint();
      let focusWatch:AbortController|undefined,intervened=false,mayReturn=false;
      const retireFocusWatch=()=>focusWatch?.abort();signal.addEventListener('abort',retireFocusWatch,{once:true});
      void Promise.resolve().then(()=>{if(current())return openAccess(target.id);}).then(async()=>{
        if(!current())return;
        mayReturn=document.activeElement===document.body||document.activeElement===dialog.element;
        focusWatch=new AbortController();
        for(const type of ['pointerdown','keydown','focusin'])document.addEventListener(type,()=>{intervened=true;},{capture:true,signal:focusWatch.signal});
        await load(true);
      }).catch(()=>{if(current()){error.textContent='Не удалось проверить доступ. Ваш выбор сохранён в этой форме.';error.hidden=false;}}).finally(()=>{
        focusWatch?.abort();signal.removeEventListener('abort',retireFocusWatch);accessBusy=false;if(!current())return;paint();
        // Settings close while the parent fields are disabled. Refresh replaces
        // the old row, so restore to its current action only after that read.
        const top=[...document.querySelectorAll('dialog[open]')].at(-1);
        if(mayReturn&&!intervened&&top===dialog.element&&(document.activeElement===document.body||document.activeElement===dialog.element)){
          const fresh=snapshot?.choices.find(item=>item.shortcutId===choice.shortcutId),sameEntity=fresh?.entity.kind===choice.entity.kind&&fresh.entity.id===choice.entity.id;
          const replacement=sameEntity?[...choices.querySelectorAll<HTMLElement>('[data-access-shortcut]')].find(node=>node.dataset.accessShortcut===choice.shortcutId):undefined;
          (replacement??audience).focus({preventScroll:true});
        }
      });
     });action.type='button';action.dataset.accessShortcut=choice.shortcutId;row.append(action);}
    input.addEventListener('change',()=>{draftGeneration++;if(input.checked&&choice.available)selected.set(choice.shortcutId,{shortcutId:choice.shortcutId,entity:choice.entity,...(choice.expectedAccessRevision===undefined?{}:{expectedAccessRevision:choice.expectedAccessRevision})});else selected.delete(choice.shortcutId);input.disabled=!choice.available&&!input.checked;paint();},{signal});
  }
  if(!snapshot.choices.length&&!selected.size)choices.append(el('p','sw-muted','Добавьте запись, фотографию или приложение на своё полотно.'));
 }
 async function load(preserve=false){
   if(busy||choiceLoading||pending||!current())return;const wasLoaded=loaded,keepDraft=preserve&&loaded&&changed(),editGeneration=draftGeneration;choiceLoading=true;const ticket=++loadGeneration,loadingCurrent=()=>current()&&ticket===loadGeneration;paint();error.hidden=true;
  try{
   const value=await options.api.request<{page:CanvasPage|null;selectionAvailable?:boolean}>('canvas.pages.get',{expectedAccountId:options.accountId});if(!loadingCurrent())return;
    page=value.page;selectionAvailable=value.selectionAvailable===true;loaded=true;fields.hidden=false;
    if(conflict&&(page?.revision??0)>conflict.expectedRevision)conflict.freshRevision=page!.revision;
    if(!wasLoaded){audience.value=mode();draftRevision=page?.revision??0;baselineAudience=mode();if(page?.selection)selected=new Map(page.selection.items.map(item=>[item.shortcutId,item]));baselineKey=canvasPageSelectionKey(page?.selection?.items??selectedItems());}present();paint();
   if(selectionAvailable){
    const next=await loadCanvasPageChoices({api:options.api,accountId:options.accountId,isCurrent:loadingCurrent,...(options.prepare?{prepare:options.prepare}:{})});if(!loadingCurrent())return;snapshot=next;
     if(!keepDraft&&draftGeneration===editGeneration){audience.value=mode();selected=new Map((page?.selection?.items??snapshot.choices.filter(item=>item.defaultSelected).map(item=>({shortcutId:item.shortcutId,entity:item.entity,...(item.expectedAccessRevision===undefined?{}:{expectedAccessRevision:item.expectedAccessRevision})}))).map(item=>[item.shortcutId,item]));draftRevision=page?.revision??0;baselineAudience=mode();baselineKey=canvasPageSelectionKey(page?.selection?.items??selectedItems());conflict=null;}
     else if(conflict&&(page?.revision??0)>conflict.expectedRevision)conflict.freshRevision=page!.revision;
     renderChoices();
   }else{snapshot=null;choices.replaceChildren();notice.textContent='Выбор объектов станет доступен после обновления сервера. Закрыть текущую ссылку можно сейчас.';}
   status.textContent=mode()==='private'?'Ваше полотно остаётся личным':'Опубликован только сохранённый выбор';
  }catch(reason){if(loadingCurrent()){error.textContent=String((reason as {code?:unknown})?.code)==='canvas_publication_not_ready'?'Расстановка ещё не сохранена на сервере. Дождитесь подключения и обновите выбор.':'Не удалось загрузить объекты. Обновите выбор; текущая ссылка не изменена.';error.hidden=false;}}
  finally{if(loadingCurrent()){choiceLoading=false;paint();}}
 }
  audience.addEventListener('change',()=>{draftGeneration++;paint();},{signal});refresh.addEventListener('click',()=>{void load(true);},{signal});takeCurrent.addEventListener('click',()=>{if(current()&&!busy&&!accessBusy&&!pending)void load(false);},{signal});
 copy.addEventListener('click',()=>{if(!current()||!link.value)return;
  void(async()=>{try{if(navigator.clipboard?.writeText)await navigator.clipboard.writeText(link.value);else{link.focus();link.select();if(!document.execCommand('copy'))throw Error('copy_unavailable');}if(current())status.textContent='Ссылка скопирована';}catch{if(current()){link.focus();link.select();status.textContent='Скопируйте выделенную ссылку';}}})();
 },{signal});
 preview.addEventListener('click',()=>{if(current()&&page?.enabled&&page.audience!=='private'){const id=page.pageId;dialog.close({restoreFocus:false});options.onPreview(id);}},{signal});
 form.addEventListener('submit',event=>{event.preventDefault();if(!current()||busy||!loaded||!pending&&!changed())return;
   if(accessBusy||!pending&&conflict&&conflict.freshRevision===null)return;
   if(!pending){
   try{if(audience.value!=='private'&&(choiceLoading||!selectionAvailable||selectedIssue()))return;
     pending={expectedAccountId:options.accountId,requestId:crypto.randomUUID(),expectedRevision:conflict?.freshRevision??draftRevision,enabled:audience.value!=='private',audience:audience.value,...(audience.value==='private'?{}:{selection:createCanvasPageSelection(snapshot!,selected.keys())})};conflict=null;
   }catch{error.textContent='Обновите выбор объектов перед публикацией.';error.hidden=false;return;}
  }
  const command=pending;loadGeneration++;choiceLoading=false;busy=true;error.hidden=true;paint();
  void options.api.request<{current:CanvasPage}>('canvas.pages.put',command).then(value=>{
    if(!current())return;page=value.current;pending=null;audience.value=mode();draftRevision=page.revision;baselineAudience=mode();baselineKey=canvasPageSelectionKey(page.selection?.items??selectedItems());status.textContent=mode()==='private'?'Доступ закрыт':visitorWarnings()?'Выбор сохранён. Доступ к некоторым окнам зависит от настроек.':selected.size?'Выбор сохранён':'Страница опубликована без объектов';present();
  }).catch(reason=>{if(!current())return;const code=String(reason?.code??'');
    if(['canvas_page_revision_conflict','canvas_page_selection_stale','canvas_page_selection_unavailable'].includes(code)){if(code==='canvas_page_revision_conflict')conflict={requestId:String(command.requestId),expectedRevision:Number(command.expectedRevision),freshRevision:null};pending=null;error.textContent='Полотно или доступ изменились. Обновите выбор — ваши отметки останутся. Затем можно явно сохранить свой выбор или взять опубликованный.';}
   else error.textContent='Ответ не получен. Повторите — продолжим то же действие.';error.hidden=false;
  }).finally(()=>{busy=false;if(current())paint();});
 },{signal});
 const ready=load();
 return{ready,dispose(){abort.abort();loadGeneration++;qrGeneration++;link.value='';qr.width=qr.height=0;choices.replaceChildren();selected.clear();snapshot=null;}};
}
