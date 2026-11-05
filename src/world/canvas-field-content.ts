import {createNativeFieldObjects,type FieldObjectView,type NativeFieldSource} from './native-field-objects';
import type {FieldNode} from './unified-field-layout.mjs';
import type {DirectoryApi} from './field-directory';
import type {CanvasPresentation} from '../../modules/canvas-objects/contract.mjs';
import type {AnyFieldEntityRef} from '../../modules/field/contract.mjs';
import type {NoteMetadata} from '../../modules/notes/browser/state.mjs';
import type {MaterialMetadata} from '../../modules/materials/browser/client.mjs';
import type {WorldAppRecord} from './types';
import type {AppStageHandle} from './app-stage';
import {closeAppRuntime,type AppRuntimeCloseRequest} from './app-runtime-close.mjs';
import {createDialog,type WorldDialog} from './dialogs';
import {createCanvasVaultClient,CANVAS_VAULT_NOTE_TYPE,type CanvasVaultClient} from './canvas-vault-client.mjs';
import {createCanvasVaultView,createCanvasSealingView,type CanvasVaultView} from './canvas-vault-renderer';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
import {initials,button,el} from './dom';
import {createFieldObjectOverview} from './field-object-overview';
import {createNativeCacheTransition,type NativeCacheTransition} from './native-cache-transition.mjs';
import './canvas-field-content.css';

export interface CanvasWidgetView {
  schema:'soty.canvas-widget-view.v1';kind:'widget';widgetId:string;instanceId:string;state:string;
  app:Pick<WorldAppRecord,'appId'|'name'|'status'|'ownerAccountId'|'entry'>;entry:{appId:string;domainId:string;origin:string;path:string};
  source:{revision:number;digest:string;profile:string};expectedSource:{revision:number;digest:string;profile:string};
  runtimeAllowed:boolean;canRefresh:boolean;
}
export type CanvasAppMount=(host:HTMLElement,value:CanvasWidgetView,isCurrent:()=>boolean)=>AppStageHandle;
interface CanvasNativeView {schema:string;kind:'note'|'image'|'audio'|'video';providerId:string;title?:string;canEdit?:boolean;note?:NoteMetadata;material?:MaterialMetadata;state?:string}
export interface CanvasLockView{schema:'soty.canvas-object-lock.v1';state:'locked'|'sealing';kind?:never}

export interface CanvasProjection {
  ownerRepresents?:AnyFieldEntityRef[];
  objectId:string;available:boolean;hidden?:boolean;revision?:number;canUnlock?:boolean;presentation?:CanvasPresentation;protection?:{kind:'none'|'password'};
  view?:CanvasNativeView|CanvasWidgetView|CanvasVaultView|CanvasLockView;
}
const denied=():never=>{throw Object.assign(new Error('Canvas source is no longer available'),{code:'canvas_access_denied'});};
interface CanvasRuntimeView extends FieldObjectView {
  runtimeMounted():boolean;canYieldRuntime():boolean;yieldRuntime():void;
  renewHostSession(reason:'reset'):Promise<boolean>;
}
interface CanvasVaultFocusView extends FieldObjectView {
  canContinueObjectFocus?(node:FieldNode,previousKey:string,nextKey:string):boolean;
}

/** A separate scoped API per placement. Even a public media read carries its
 * current object ID, never a copied owner identity or an old authority token. */
export function createCanvasFieldContent(options:{api:DirectoryApi;nativeCacheApi?:DirectoryApi;nativeTransition?:NativeCacheTransition;accountId:string;isCurrent():boolean;projection(id:string):CanvasProjection|null;mountApp?:CanvasAppMount;onObjectChanged?:(objectId:string)=>Promise<void>;vault?:CanvasVaultClient;allowUnlock?:boolean}){
  const factories=new Set<ReturnType<typeof createNativeFieldObjects>>(),widgetViews=new Set<CanvasRuntimeView>(),vaultViews=new Set<CanvasVaultFocusView>(),protectedWidgets=new Map<FieldObjectView,string>();let disposed=false;
  // Session UI intent survives lightweight provider virtualization, not reload.
  const closedPlacements=new Set<string>();
  let runtimeEpoch=0;
  // Bound live documents, not canvas objects. Activated work is never evicted.
  const runtimeLimit=8;
  function admitRuntime(requester:CanvasRuntimeView,explicit:boolean):boolean{
    const running=[...widgetViews].filter(view=>view.runtimeMounted());
    if(running.length<runtimeLimit)return true;
    const idle=explicit?running.find(view=>view!==requester&&view.canYieldRuntime()):undefined;
    if(!idle)return false;idle.yieldRuntime();return true;
  }
  const unlockProjections=new Map<string,{projection:CanvasProjection|null;generation:number}>();
  let nativeTransition:NativeCacheTransition;
  const vault=options.vault??createCanvasVaultClient({api:options.api,accountId:options.accountId,projectId:'soty',isCurrent:()=>!disposed&&options.isCurrent(),onChange:async objectId=>{
    if(vault.isUnlocked(objectId))await nativeTransition.resumeAfterUnlock(objectId);
    if(!disposed&&options.isCurrent())await options.onObjectChanged?.(objectId);
  }});
  nativeTransition=options.nativeTransition??createNativeCacheTransition({api:options.nativeCacheApi??options.api,accountId:options.accountId,projectId:'soty',isCurrent:()=>!disposed&&options.isCurrent(),vault,
    onChange:value=>{if(!disposed&&options.isCurrent())void Promise.resolve().then(()=>options.onObjectChanged?.(value.objectId)).catch(()=>{});}});
  function refreshBeforeUnlockCleanup(objectId:string){
    if(disposed||!options.isCurrent())return;
    // Start the host's read-generation fence synchronously, before async local
    // recovery. Parallel pre-unlock Page replies must not become new lock DTOs.
    try{void Promise.resolve(options.onObjectChanged?.(objectId)).catch(()=>{});}catch{}
  }
  const vaultSubscription=vault.subscribe((objectId,reason)=>{if(reason==='unlocked'){
    unlockProjections.set(objectId,{projection:options.projection(objectId),generation:vault.generation(objectId)});
    refreshBeforeUnlockCleanup(objectId);
    if(options.vault)void nativeTransition.resumeAfterUnlock(objectId).then(()=>{if(!disposed&&options.isCurrent())return options.onObjectChanged?.(objectId);}).catch(()=>{});
    return;
  }unlockProjections.delete(objectId);for(const[view,id]of [...protectedWidgets])if(id===objectId)view.dispose();});
  function projection(node:FieldNode):CanvasProjection|null{
    if(disposed||!options.isCurrent()||node.entity.entity.kind!=='object')return null;
    const value=options.projection(node.entity.entity.id);
    return value?.available&&value.protection?.kind==='none'&&value.view?value:null;
  }
  function source(value:CanvasProjection|null):NativeFieldSource|null{
    const view=value?.view;
    if(!view||view.kind===undefined||view.kind==='widget'||view.kind==='vault')return null;
    if(view?.providerId==='soty.notes'&&view.kind==='note'&&view.note)return{kind:'note',noteId:view.note.noteId,summary:view.note,readOnly:!view.canEdit,...(value?.presentation?.fontSize===undefined?{}:{fontSize:value.presentation.fontSize})};
    if(view?.providerId==='soty.materials'&&['image','audio','video'].includes(view.kind)&&view.material)return{kind:view.kind as 'image'|'audio'|'video',materialId:view.material.materialId,summary:view.material};
    return null;
  }
  function protectedProjection(node:FieldNode):CanvasProjection|null{
    if(disposed||!options.isCurrent()||node.entity.entity.kind!=='object')return null;
    const value=options.projection(node.entity.entity.id);return value?.available&&value.hidden!==true&&value.protection?.kind==='password'&&value.view?value:null;
  }
  const canUnlock=(value:CanvasProjection)=>options.allowUnlock!==false&&value.canUnlock!==false;
  function protectedNativeView(value:CanvasProjection|null):CanvasVaultView|null{
    const view=value?.view;if(!view||view.kind===undefined||view.kind==='widget')return null;if(view.kind==='vault')return view;
    if(view.providerId==='soty.notes'&&view.kind==='note'&&view.note)return{schema:'soty.canvas-vault-view.v1',kind:'vault',contentType:CANVAS_VAULT_NOTE_TYPE,byteLength:0,revision:view.note.revision,canEdit:view.canEdit===true};
    if(view.providerId==='soty.materials'&&view.material&&['image','audio','video'].includes(view.kind))return{schema:'soty.canvas-vault-view.v1',kind:'vault',contentType:view.material.mediaType,byteLength:view.material.size,revision:view.material.revision,canEdit:view.canEdit===true};
    return null;
  }
  const widgetKey=(value:CanvasProjection):string|null=>{const widget=value.view;if(!widget||!('kind'in widget)||widget.kind!=='widget')return null;
    return options.mountApp&&widget.runtimeAllowed?`soty.apps:${value.objectId}:${widget.instanceId}:${widget.expectedSource.revision}:${widget.expectedSource.digest}`:null;};
  function objectKey(node:FieldNode):string|null{
    const protectedValue=protectedProjection(node);
    if(protectedValue){const view=protectedValue.view!;
      const atUnlock=unlockProjections.get(protectedValue.objectId),oldLockedDto=atUnlock?.projection===protectedValue&&atUnlock.generation===vault.generation(protectedValue.objectId);
      // Directory/Page metadata records are immutable between refreshes. The
      // unchanged pre-unlock DTO cannot revoke a just-issued permit; a new one can.
      if(!oldLockedDto){if(view.kind===undefined&&vault.isUnlocked(protectedValue.objectId))vault.invalidate(protectedValue.objectId,'projection');unlockProjections.delete(protectedValue.objectId);}
      if(canUnlock(protectedValue))nativeTransition.observeProtected(protectedValue.objectId);
      if(!canUnlock(protectedValue)||!vault.isUnlocked(protectedValue.objectId)||view.kind===undefined)return`vault:locked:${protectedValue.objectId}:${vault.generation(protectedValue.objectId)}:${canUnlock(protectedValue)?'unlock':'readonly'}`;
      const native=protectedNativeView(protectedValue);if(native)return`vault:content:${protectedValue.objectId}:${native.contentType}:${vault.generation(protectedValue.objectId)}`;
      const key=widgetKey(protectedValue);return key?`vault:${key}:${vault.generation(protectedValue.objectId)}`:null;
    }
    if(node.entity.entity.kind==='object'&&vault.isUnlocked(node.entity.entity.id))vault.invalidate(node.entity.entity.id,'projection');
    const value=projection(node);if(!value)return null;
    if(value.view?.kind===undefined&&value.view?.state==='sealing')return`vault:sealing:${value.objectId}`;
    if(value.view&&'kind'in value.view&&value.view.kind==='widget')return widgetKey(value);
    const current=source(value);if(!current)return null;
    if((value.view as CanvasNativeView).canEdit===true)nativeTransition.remember(value.objectId,{providerId:current.kind==='note'?'soty.notes':'soty.materials',ref:current.kind==='note'?current.noteId:current.materialId,ownerId:options.accountId});
    return `${(value.view as CanvasNativeView).providerId}:${value.objectId}:${current.kind==='note'?current.noteId:current.materialId}`;
  }
  function continueLockedFocus(node:FieldNode,prior:CanvasProjection,key:string,previousKey:string,nextKey:string):boolean{
    const prefix=`vault:locked:${prior.objectId}:`;
    if(node.entity.entity.kind!=='object'||node.entity.entity.id!==prior.objectId||previousKey!==key||!key.startsWith(prefix)||!key.endsWith(':unlock'))return false;
    const previousGeneration=Number(key.slice(prefix.length,-':unlock'.length)),generation=vault.generation(prior.objectId);
    if(!Number.isSafeInteger(previousGeneration)||previousGeneration<0||(generation!==previousGeneration&&generation!==previousGeneration+1))return false;
    const value=protectedProjection(node);
    if(!value||value.objectId!==prior.objectId||!canUnlock(value)||!vault.isUnlocked(value.objectId)
      ||!Number.isSafeInteger(prior.revision)||prior.revision!==value.revision)return false;
    const before=prior.view!,after=value.view!;
    // A redacted lock has no old Source pin. Its fresh authorized projection
    // owns admission; a previously known provider must keep its exact identity.
    if(before.kind==='widget'){
      if(after.kind!=='widget'||before.widgetId!==after.widgetId||before.instanceId!==after.instanceId||before.app.appId!==after.app.appId
        ||!(['appId','domainId','origin','path'] as const).every(field=>before.entry[field]===after.entry[field])
        ||!(['revision','digest','profile'] as const).every(field=>before.source[field]===after.source[field]&&before.expectedSource[field]===after.expectedSource[field]))return false;
    }else if(before.kind!==undefined){
      const oldNative=protectedNativeView(prior),newNative=protectedNativeView(value);
      if(!oldNative||!newNative||oldNative.contentType!==newNative.contentType)return false;
    }
    if(after.kind===undefined&&value!==prior)return false;
    return objectKey(node)===nextKey&&protectedProjection(node)===value&&vault.isUnlocked(value.objectId)&&vault.generation(value.objectId)===generation;
  }
  function canContinueObjectFocus(node:FieldNode,previousKey:string,nextKey:string):boolean{
    if(disposed||!options.isCurrent())return false;
    return [...vaultViews].some(view=>view.canContinueObjectFocus?.(node,previousKey,nextKey)===true);
  }
  function createObject(node:FieldNode):FieldObjectView|null{
    const key=objectKey(node),objectId=node.entity.entity.id;if(!key)return null;
    if(key.startsWith('vault:sealing:')){
      const view=createCanvasSealingView(node,{isCurrent:()=>!disposed&&options.isCurrent()&&projection(node)?.view?.kind===undefined,...(options.onObjectChanged?{onObjectChanged:options.onObjectChanged}:{})});
      const wrapped={...view,dispose(){view.dispose();vaultViews.delete(wrapped);}};vaultViews.add(wrapped);return wrapped;
    }
    const protectedValue=protectedProjection(node);
    if(protectedValue&&(key.startsWith('vault:locked:')||protectedNativeView(protectedValue))){
      const view=createCanvasVaultView(node,{client:vault,accountId:options.accountId,objectId,isCurrent:()=>protectedProjection(node)!==null,
        ...(!canUnlock(protectedValue)?{}:{canReveal:()=>{const value=nativeTransition.snapshot(objectId);return value?.applicable===false||value?.server==='protected'&&value.device==='complete';},
          beforeReveal:async()=>{const value=await nativeTransition.resumeAfterUnlock(objectId);return value.applicable===false||value.server==='protected'&&value.device==='complete';}}),
        projection:()=>{const value=protectedProjection(node);if(!value)return null;const native=protectedNativeView(value);return{...(value.presentation?{presentation:value.presentation}:{}),view:native??value.view as CanvasLockView};},...(options.onObjectChanged?{onObjectChanged:options.onObjectChanged}:{}),...(!canUnlock(protectedValue)?{allowUnlock:false,...(options.allowUnlock!==false?{lockedMessage:'Содержимое закрыто. Попросите владельца открыть доступ.'}:{})}:options.allowUnlock===undefined?{}:{allowUnlock:options.allowUnlock})});
      const wrapped:CanvasVaultFocusView={...view,
        canContinueObjectFocus:(next,previous,replacement)=>vaultViews.has(wrapped)&&next.shortcutId===node.shortcutId&&continueLockedFocus(next,protectedValue,key,previous,replacement),
        dispose(){view.dispose();vaultViews.delete(wrapped);}};vaultViews.add(wrapped);return wrapped;
    }
    const current=()=>!disposed&&options.isCurrent()&&objectKey(node)===key;
    const renderProjection=(next=node)=>projection(next)??(vault.isUnlocked(objectId)?protectedProjection(next):null);
    const read=()=>{const value=renderProjection();if(!current()||!value?.view)denied();return value!.view!;};
    const api:DirectoryApi={async request<T>(operation:string,args:Record<string,unknown>={}):Promise<T>{
      const value=read();
      if(value.kind===undefined||value.kind==='widget'||value.kind==='vault')return denied();
      if(operation==='notes.get'&&value.note&&args.noteId===value.note.noteId)return options.api.request<T>('canvas.objects.content',{expectedAccountId:options.accountId,objectId,part:'note'});
      if(operation==='notes.put'&&value.note&&value.canEdit&&args.noteId===value.note.noteId)return options.api.request<T>(operation,args);
      if(operation==='materials.get'&&value.material&&args.materialId===value.material.materialId)return options.api.request<T>('canvas.objects.content',{expectedAccountId:options.accountId,objectId,part:'metadata'});
      if(operation==='materials.read'&&value.material&&args.materialId===value.material.materialId)return options.api.request<T>('canvas.objects.content',{expectedAccountId:options.accountId,objectId,part:'chunk',index:args.index});
      return denied();
    }};
    if((renderProjection()?.view as CanvasWidgetView|undefined)?.kind==='widget'){
      const root=document.createElement('article');root.className='cf-widget';root.setAttribute('aria-label','Виджет приложения');
      const header=document.createElement('header'),title=document.createElement('span'),controls=document.createElement('span'),host=document.createElement('div');
      header.className='cf-widget-header';title.className='cf-widget-title';controls.className='cf-widget-controls';controls.dataset.fieldControl='';
      host.className='cf-widget-content';host.dataset.fieldControl='';header.append(title,controls);root.append(header,host);
      const close=document.createElement('button');close.type='button';close.className='sw-button sw-button-quiet cf-widget-legacy-close';close.textContent='Закрыть окно';
      close.setAttribute('aria-label','Закрыть окно приложения');close.title='Закрыть окно приложения';close.dataset.fieldControl='';close.hidden=true;controls.append(close);
      let dead=false,stage:AppStageHandle|null=null,visible=false,presentation:'overview'|'glance'|'interactive'='glance';
      const notice=document.createElement('p');notice.className='cf-widget-notice';notice.setAttribute('role','status');notice.hidden=true;root.append(notice);
      let activated=false,closed=closedPlacements.has(node.shortcutId),closing=false,menuAttached=false,standby='',closeEpoch=0,closeDialog:WorldDialog|null=null;
      const live=()=>!dead&&current();
      let identityNode=node;
      const identity=createFieldObjectOverview(root,{isCurrent:live,readIdentity:()=>{const value=renderProjection(identityNode)?.view as CanvasWidgetView|undefined;return value?.kind==='widget'?{title:value.app.name,mark:initials(value.app.name)}:null;}});
      const update=(next:FieldNode)=>{const value=renderProjection(next)?.view as CanvasWidgetView|undefined;if(!live()||value?.kind!=='widget')return;
        const inset=nativeFieldSafeArea(next.presentation??next.entity.presentation??{shape:'rect'},next.width,next.height);for(const[side,value]of Object.entries(inset))root.style.setProperty(`--cf-safe-${side}`,`${value}px`);
        identityNode=next;identity.update();title.textContent=value.app.name;root.setAttribute('aria-label',`Виджет: ${value.app.name}`);stage?.updateApp(value.app);};
      function showStandby(text:string){if(stage||standby===text)return;standby=text;const box=document.createElement('div'),message=document.createElement('p'),open=document.createElement('button');box.className='cf-widget-standby';
        message.textContent=text;message.setAttribute('role','status');open.type='button';open.className='sw-button sw-button-quiet';open.textContent='Открыть окно';open.dataset.fieldControl='';
        open.addEventListener('click',()=>{if(!live())return;beginWork();mount(true);});box.append(message,open);host.replaceChildren(box);}
      function closeNotice(text:string){notice.textContent=text;notice.hidden=!text||presentation!=='interactive';}
      function beginWork(){closeEpoch++;closeDialog?.close({restoreFocus:false});activated=true;closed=false;closedPlacements.delete(node.shortcutId);closing=false;close.disabled=false;closeNotice('');}
      function releaseRuntime(){if(stage)runtimeEpoch++;closeEpoch++;closeDialog?.close({restoreFocus:false});stage?.dispose();stage=null;activated=false;closing=false;menuAttached=false;close.disabled=false;close.hidden=true;host.replaceChildren();standby='';}
      async function requestClose(request:AppRuntimeCloseRequest){
        if(!live()||!stage||closing||!request.isCurrent())return;
        const previous=stage,intent=++closeEpoch;closing=true;close.disabled=true;
        const intentCurrent=()=>!(intent!==closeEpoch||!live()||stage!==previous||!request.isCurrent());
        try{await closeAppRuntime({stage:previous,isCurrent:intentCurrent,confirmUnknown:request.confirmUnknown,
          onFailure:()=>closeNotice('Сохранение не подтверждено. Окно остаётся открытым.'),
          commit(){closed=true;closedPlacements.add(node.shortcutId);releaseRuntime();closeNotice('');showStandby('Окно закрыто');}});}
        finally{if(intent===closeEpoch){closing=false;close.disabled=false;}}
      }
      function confirmLegacyClose(intentCurrent:()=>boolean):Promise<boolean>{
        if(!intentCurrent()||closeDialog)return Promise.resolve(false);
        return new Promise(resolve=>{
          let accepted=false;
          const dialog=createDialog('Закрыть окно?',()=>{if(closeDialog===dialog)closeDialog=null;resolve(accepted&&intentCurrent());},
            {isCurrent:intentCurrent,resolve:()=>close});closeDialog=dialog;
          const keep=button('Оставить открытым','back','sw-button-primary',()=>dialog.close());
          const discard=button('Закрыть без сохранения','close','sw-button-quiet',()=>{accepted=intentCurrent();dialog.close({restoreFocus:false});});
          dialog.body.append(el('p','','Несохранённое внутри окна может пропасть. Оставить окно открытым?'),keep,discard);keep.focus();
        });
      }
      function mount(explicit=false){if(stage||!live()||!visible||presentation==='overview')return;
        if(closed){showStandby('Окно закрыто');return;}
        if(!admitRuntime(widget,explicit)){showStandby(`Открыто много окон (${runtimeLimit}). Закройте одно из них.`);return;}
        const value=read() as CanvasWidgetView;host.replaceChildren();standby='';
        stage=options.mountApp!(host,value,live);runtimeEpoch++;stage.setWidgetMode?.(presentation==='interactive'?'interactive':'glance');
        const owner={isCurrent:live,resolve:()=>root,requestClose};menuAttached=stage.mountRuntimeControls?.(controls,owner)===true;
        close.hidden=menuAttached||presentation!=='interactive';void stage.ready.catch(()=>{});}
      close.addEventListener('click',()=>{
        if(menuAttached||close.hidden||!stage)return;
        const previous=stage,frame=host.querySelector('iframe');
        const changed=()=>{closeEpoch++;closing=false;close.disabled=false;closeDialog?.close({restoreFocus:false});};
        frame?.addEventListener('load',changed);
        void requestClose({isCurrent:()=>live()&&stage===previous&&host.querySelector('iframe')===frame,confirmUnknown:confirmLegacyClose})
          .finally(()=>frame?.removeEventListener('load',changed));
      });
      const widget:CanvasRuntimeView={element:root,overview:identity.element,interaction:'focus',focusControls:controls,update,
        setVisible(value){visible=value;if(value)mount(activated);},activate(){beginWork();mount(true);host.querySelector<HTMLElement>('iframe,button')?.focus();},
        setPresentation(value){const enter=value==='interactive'&&presentation!=='interactive';presentation=value;if(enter)beginWork();
          if(value!=='interactive'){closeEpoch++;closing=false;close.disabled=false;closeDialog?.close({restoreFocus:false});closeNotice('');}
          identity.setPresentation(value);stage?.setWidgetMode?.(value==='interactive'?'interactive':'glance');close.hidden=menuAttached||!stage||value!=='interactive';notice.hidden=!notice.textContent||value!=='interactive';mount(enter);},
        async flush(){await stage?.flush();},hasUnsavedChanges:()=>stage?.hasUnsavedChanges()===true,hasUnconfirmedWork:()=>stage?.hasUnconfirmedWork()===true,
        retainWhileHidden:()=>live()&&activated&&stage!==null,runtimeMounted:()=>stage!==null,
        canYieldRuntime:()=>live()&&!activated&&!!stage&&!stage.hasUnsavedChanges()&&!stage.hasUnconfirmedWork(),
        yieldRuntime(){releaseRuntime();showStandby('Окно готово к открытию');},
        renewHostSession:reason=>live()&&stage?.renewHostSession?stage.renewHostSession(reason):Promise.resolve(false),
        dispose(){if(dead)return;dead=true;if(stage)runtimeEpoch++;closeEpoch++;closeDialog?.close({restoreFocus:false});identity.dispose();stage?.dispose();stage=null;host.replaceChildren();title.textContent='';root.removeAttribute('aria-label');root.remove();widgetViews.delete(widget);protectedWidgets.delete(widget);}};
      update(node);widgetViews.add(widget);if(protectedValue)protectedWidgets.set(widget,objectId);return widget;
    }
    const factory=createNativeFieldObjects({api,accountId:options.accountId,projectId:'soty',isCurrent:current,resolve:next=>source(projection(next)),resolveOverview:next=>{
      const view=projection(next)?.view;if(!current()||!view||view.kind===undefined||view.kind==='vault'||view.kind==='widget')return null;
      return{title:view.title||({note:'Записка',image:'Фотография',audio:'Аудиозапись',video:'Видео'}[view.kind]),symbol:{note:'note',image:'image',audio:'music',video:'camera'}[view.kind]};
    }});
    const view=factory.createObject(node);if(!view){factory.dispose();return null;}factories.add(factory);
    return {...view,dispose(){view.dispose();factory.dispose();factories.delete(factory);}};
  }
  return{objectKey,createObject,canContinueObjectFocus,vault,nativeTransition,
    hasUnconfirmedWork:()=>[...widgetViews].some(view=>view.hasUnconfirmedWork?.()===true),readRuntimeEpoch:()=>runtimeEpoch,
    async renewHostSessions(reason:'reset'){if(disposed||!options.isCurrent())return;await Promise.all([...widgetViews].map(view=>view.renewHostSession(reason)));},
    hasUnsavedChanges:()=>nativeTransition.hasUnsavedChanges?.()===true,async flush(){await nativeTransition.flush?.();},dispose(){if(disposed)return;disposed=true;vaultSubscription();unlockProjections.clear();closedPlacements.clear();for(const view of [...widgetViews])view.dispose();for(const view of [...vaultViews])view.dispose();for(const factory of factories)factory.dispose();factories.clear();if(!options.vault)vault.dispose();if(!options.nativeTransition)nativeTransition.dispose();}};
}
