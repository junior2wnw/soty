import {createUnifiedField,type UnifiedField,type UnifiedFieldViewState,type FieldDirectoryEntity} from './unified-field';
import {createCanvasFieldContent,type CanvasProjection,type CanvasAppMount} from './canvas-field-content';
import {createCanvasPageClient,type CanvasPageView,type CanvasPageItem,type CanvasPageAppView} from './canvas-page-client.mjs';
import {fieldEntityKey,createFieldDocumentV2} from '../../modules/field/contract.mjs';
import type {DirectoryApi} from './field-directory';
import type {FieldNode} from './unified-field-layout.mjs';
import type {FieldObjectView} from './native-field-objects';
import {button,el,iconButton} from './dom';
import {createCanvasPageLive} from './canvas-page-live.mjs';
import './canvas-page-screen.css';

interface Options {
 pageId:string;api?:DirectoryApi;accountId?:string;isCurrent():boolean;onHome():void;
 onOpenApp?:(entry:CanvasPageAppView['entry'])=>void;mountApp?:CanvasAppMount;
 viewState?:UnifiedFieldViewState;onViewStateChange?:(value:UnifiedFieldViewState)=>void;
 guestPreview?:boolean;homeLabel?:string;
}
export interface CanvasPageScreen {element:HTMLElement;ready:Promise<void>;dispose():void;refresh():Promise<void>;flush():Promise<void>;hasUnsavedChanges():boolean;hasUnconfirmedWork():boolean;readRuntimeEpoch():number}

/** A visitor uses the existing field engine, without a commit port. Source
 * reads keep page membership and the visitor's real authority on every call. */
export function createCanvasPageScreen(options:Options):CanvasPageScreen {
 const root=el('section','uf-screen cp-screen'),scene=el('div','uf-scene-host'),dock=el('header','cp-dock'),label=el('strong','cp-title','Полотно');
 const status=el('p','cp-status','Открываем полотно…');status.setAttribute('role','status');
 const retry=button('Повторить','refresh','sw-button-quiet');retry.hidden=true;
 const scale=button('100%',undefined,'cp-scale');scale.setAttribute('aria-label','Показать всё полотно');scale.title='Показать всё';
 dock.append(iconButton(options.homeLabel??(options.accountId?'Моё полотно':'Открыть свои Соты'),'grid',options.onHome),label,scale);
 root.append(scene,dock,status,retry);
 const lifetime=new AbortController(),current=()=>!lifetime.signal.aborted&&options.isCurrent();
 const client=createCanvasPageClient({pageId:options.pageId,...(options.api&&options.accountId?{api:options.api,accountId:options.accountId}:{}),isCurrent:current,signal:lifetime.signal});
 const projections=new Map<string,CanvasProjection>(),items=new Map<string,CanvasPageItem>();let engine:UnifiedField|null=null,dead=false;
 const denied=():never=>{throw Object.assign(new Error('canvas_page_read_only'),{code:'canvas_page_read_only'});};
 const api:DirectoryApi={async request<T>(op:string,args:Record<string,unknown>={}):Promise<T>{
  if(!current())return denied();
  if(op==='canvas.objects.content')return await client.content(String(args.objectId),String(args.part),args.index as number|undefined) as T;
  if(options.api&&options.accountId&&['canvas.vault.status','canvas.vault.unlock','canvas.vault.relock'].includes(op))return options.api.request<T>(op,{...args,expectedAccountId:options.accountId});
  return denied();
 }};
 const content=createCanvasFieldContent({api,...(options.api?{nativeCacheApi:options.api}:{}),accountId:options.accountId??'public_viewer',isCurrent:current,allowUnlock:!!options.accountId,projection:id=>projections.get(id)??null,
  ...(options.mountApp?{mountApp:options.mountApp}:{}),onObjectChanged:()=>refresh()});
 function itemFor(node:FieldNode){return items.get(fieldEntityKey(node.entity.entity));}
 function ordinary(node:FieldNode):boolean{
  const item=itemFor(node);return !!item&&(item.view.kind==='app'||item.view.kind==='widget'&&(!options.mountApp||!item.view.runtimeAllowed));
 }
 function card(node:FieldNode):FieldObjectView|null{
  const value=itemFor(node);if(!value)return null;
  const host=el('article','cp-app-card'),title=el('strong'),open=button(!options.accountId&&value.protection.kind==='password'?'Войти в Соты':'Открыть','external','sw-button-primary');open.dataset.fieldControl='';host.append(title,open);
  const update=(next:FieldNode)=>{const item=itemFor(next);title.textContent=item?.view.kind==='app'||item?.view.kind==='widget'?item.view.app.name:item?.protection.kind==='password'?'Под паролем':'Приложение';};
  open.addEventListener('click',()=>{if(!current())return;const item=itemFor(node);
   if(item?.view.kind==='app'||item?.view.kind==='widget')options.onOpenApp?.(item.view.entry);else if(!options.accountId)options.onHome();
  },{signal:lifetime.signal});
  let disposed=false;update(node);
  return{element:host,update,setVisible(){},activate(){open.focus();},async flush(){},hasUnsavedChanges:()=>false,dispose(){if(disposed)return;disposed=true;title.textContent='';host.replaceChildren();host.remove();}};
 }
 function apply(view:CanvasPageView){
  if(!current())return;items.clear();projections.clear();
  const entities:FieldDirectoryEntity[]=view.items.map(item=>{
   const key=fieldEntityKey(item.entity);items.set(key,item);
   const sourceView='canEdit'in item.view?{...item.view,canEdit:false}:item.view;
   if(item.entity.kind==='object')projections.set(item.entity.id,{objectId:item.entity.id,available:true,presentation:item.presentation,protection:item.protection,...(item.canUnlock===undefined?{}:{canUnlock:item.canUnlock}),view:sourceView as NonNullable<CanvasProjection['view']>});
   const title=item.view.kind==='app'||item.view.kind==='widget'?item.view.app.name:'title'in item.view?item.view.title:'Под паролем';
   return{entity:item.entity,title:title??'Объект',source:'public',presentation:item.presentation.mask};
  });
  label.textContent=options.guestPreview?'Вид для гостей':view.banner.kind==='profile'?view.banner.profile.displayName:'Полотно';status.hidden=view.items.length>0;status.textContent='Здесь пока ничего нет';retry.hidden=true;
  if(engine)engine.update({document:view.field,entities,acceptRemote:true});
  else{
   engine=createUnifiedField({accountId:'page_'+options.pageId,document:view.field,entities,readableObjectFocus:true,...(options.viewState?{viewState:options.viewState}:{}),
    ...(options.onViewStateChange?{onViewStateChange:options.onViewStateChange}:{}),
    objectKey:node=>ordinary(node)?`page-card:${fieldEntityKey(node.entity.entity)}`:content.objectKey(node),
    createObject:node=>ordinary(node)?card(node):content.createObject(node),
    onActivate:(_entity,shortcutId)=>{const node=scene.querySelector<HTMLElement>(`[data-shortcut-id="${CSS.escape(shortcutId??'')}"]`);node?.querySelector<HTMLElement>('button,input,textarea,audio,video')?.focus();},
    onStateChange:summary=>{scale.querySelector('span')!.textContent=`${Math.round(summary.camera.scale*100)}%`;},
   });scene.replaceChildren(engine.element);
  }
 }
 function clear(reason:unknown){
  projections.clear();items.clear();engine?.update({document:createFieldDocumentV2(),entities:[],acceptRemote:true});
  const code=String((reason as {code?:unknown})?.code??'');
  status.textContent=/unavailable|revoked|authentication|account_changed|PROFILE_CHANGED/u.test(code)?'Полотно недоступно':'Не удалось обновить полотно';status.hidden=false;retry.hidden=false;
 }
 const live=createCanvasPageLive({client,apply,onUnavailable:clear,isCurrent:current});
 async function refresh(){await live.refresh();}
 retry.addEventListener('click',()=>{retry.disabled=true;void refresh().catch(()=>{}).finally(()=>{retry.disabled=false;});},{signal:lifetime.signal});
 scale.addEventListener('click',()=>engine?.fitOverview(),{signal:lifetime.signal});
 const ready=live.start().catch(()=>{});
 return{element:root,ready,refresh,async flush(){await engine?.flush();await content.flush();},hasUnsavedChanges:()=>engine?.hasUnsavedChanges()===true||content.hasUnsavedChanges(),
 hasUnconfirmedWork:()=>content.hasUnconfirmedWork(),readRuntimeEpoch:()=>content.readRuntimeEpoch(),
  dispose(){if(dead)return;dead=true;live.dispose();lifetime.abort();content.dispose();engine?.destroy();engine=null;projections.clear();items.clear();root.replaceChildren();root.remove();}};
}
