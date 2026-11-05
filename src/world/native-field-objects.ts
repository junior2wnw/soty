import type {FieldNode} from './unified-field-layout.mjs';
import {nativeFieldRef,nativeNoteFontSize,mayRefreshNativeNote,nativeNoteStatus} from './native-field-objects-state.mjs';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
import {createFieldObjectOverview,type FieldObjectIdentity,type FieldObjectPresentation} from './field-object-overview';
import {createDraftStore,createNoteSession,blankNote,noteErrorCode,type Note,type NoteMetadata,type DraftStore,type NoteSession,type SessionState} from '../../modules/notes/browser/state.mjs';
import {createNoteCache,readNoteWithCache,type NoteCache} from '../../modules/notes/browser/cache.mjs';
import {readMaterialBlob,type MaterialApi,type MaterialMetadata} from '../../modules/materials/browser/client.mjs';
import './native-field-objects.css';

/** Provider port for the universal field host. Providers own content, the host owns placement/masks. */
export interface FieldObjectView {
  element:HTMLElement;
  /** Current, provider-owned identity; the host keeps it outside the content mask. */
  overview?:HTMLElement;
  /** Cross-origin app controls become active in a readable focus. The canvas owns the passive preview. */
  interaction?:'focus';
  /** Host-owned controls may join the readable focus bar; content stays put. */
  focusControls?:HTMLElement;
  update(node:FieldNode):void;
  setVisible(visible:boolean):void;
  /** Presentation of the same connected provider; never changes its Source viewport. */
  setPresentation?(mode:'overview'|'glance'|'interactive'):void;
  /** Reference viewport for this provider's glance; placement and user settings stay untouched. */
  glanceSize?():{width:number;height:number}|null;
  /** Explicit readable-focus presentation; never inferred from DOM focus or size. */
  setFocused?(focused:boolean):void;
  activate?():void;
  flush():Promise<void>;
  hasUnsavedChanges():boolean;
  /** Retain a legacy app's unknown local state without declaring it dirty. */
  hasUnconfirmedWork?():boolean;
  /** A user-activated, admitted runtime owns volatile state even after saving. */
  retainWhileHidden?():boolean;
  dispose():void;
}
export type NativeFieldSource={kind:'note';noteId:string;summary?:NoteMetadata;fontSize?:number;readOnly?:boolean}
  |{kind:'image'|'audio'|'video';materialId:string;summary?:MaterialMetadata};
export interface NativeFieldObjectsOptions {
  api:MaterialApi;accountId:string;projectId:string;isCurrent:()=>boolean;
  /** Fresh ACL-aware provider metadata, never FieldDocument body/URL/base64. */
  resolve?:(node:FieldNode)=>NativeFieldSource|null;
  /** Identity from the same fresh projection; never derive it from editor/body/cache. */
  resolveOverview?:(node:FieldNode)=>FieldObjectIdentity|null;
  onNoteChange?:(note:Note)=>void;
  onNoteFork?:(note:Note)=>void;
  store?:DraftStore;cache?:NoteCache;
}
export interface NativeFieldObjects {
  createObject(node:FieldNode):FieldObjectView|null;
  objectKey(node:FieldNode):string|null;
  flush():Promise<void>;
  hasUnsavedChanges():boolean;
  dispose():void;
}
const make=<K extends keyof HTMLElementTagNameMap>(tag:K,className='',text=''):HTMLElementTagNameMap[K]=>{
  const element=document.createElement(tag);element.className=className;if(text)element.textContent=text;return element;
};
const control=<T extends HTMLElement>(element:T):T=>{element.dataset.fieldControl='';return element;};
function safeAreaFor(root:HTMLElement): (node:FieldNode)=>void {
  let previous='';return node=>{
    const mask=node.presentation??node.entity.presentation??{shape:'rect'},key=JSON.stringify([mask,node.width,node.height]);if(key===previous)return;previous=key;
    const inset=nativeFieldSafeArea(mask,node.width,node.height);
    for(const[side,value]of Object.entries(inset))root.style.setProperty(`--nf-safe-${side}`,`${value}px`);
  };
}
const errorText=(error:unknown):string=>{
  const code=noteErrorCode(error);
  return /account_changed|authentication|denied|revoked/u.test(code)?'Доступ изменился. Откройте полотно заново.'
    :/not_found|deleted/u.test(code)?'Материал недоступен':/local_quota/u.test(code)?'Недостаточно места для черновика'
    :/local_unavailable/u.test(code)?'Черновик не сохранён на устройстве':/revision_conflict/u.test(code)?'На другом устройстве появилась новая версия'
    :/media|decode|NotSupported/u.test(code)?'Этот браузер не смог открыть материал':'Не удалось загрузить. Повторите попытку.';
};

export function createNativeFieldObjects(options:NativeFieldObjectsOptions):NativeFieldObjects {
  const store=options.store??createDraftStore(options.accountId,options.projectId),cache=options.cache??createNoteCache(options.accountId,options.projectId);
  const views=new Set<FieldObjectView>();let disposed=false;
  const current=()=>!disposed&&options.isCurrent();
  function sourceFor(node:FieldNode):NativeFieldSource|null {
    if(options.resolve)return options.resolve(node);
    const ref=nativeFieldRef(node.entity.entity);
    return ref?.provider==='notes'?{kind:'note',noteId:ref.id}:null;
  }
  function noteView(initial:FieldNode,initialSource:Extract<NativeFieldSource,{kind:'note'}>):FieldObjectView {
    let source=initialSource,session:NoteSession|null=null,readOnlyNote:Note|null=null,visible=false,dead=false,request=0,flight:Promise<void>|null=null,stale=false,lastNotifiedRevision=0;
    const root=make('article','nf-object nf-note');root.setAttribute('aria-label','Записка');root.dataset.nativeProvider='notes';const applySafeArea=safeAreaFor(root);
    const header=make('div','nf-header'),kind=make('span','nf-kind','Записка'),status=make('span','nf-status');status.setAttribute('role','status');status.setAttribute('aria-live','polite');header.append(kind,status);
    const title=control(make('textarea','nf-title')),body=control(make('textarea','nf-body'));
    title.rows=1;title.maxLength=160;title.placeholder='Название';title.setAttribute('aria-label','Название записки');title.disabled=true;
    body.maxLength=100000;body.placeholder='О чём думаете?';body.setAttribute('aria-label','Текст записки');body.disabled=true;body.spellcheck=true;
    const checklist=make('div','nf-checklist'),message=make('p','nf-message'),actions=make('div','nf-recovery');message.setAttribute('role','status');message.hidden=true;
    const retry=control(make('button','nf-retry','Повторить')),copy=control(make('button','nf-retry','Сохранить копию')),latest=control(make('button','nf-retry','Версия на сервере')),keep=control(make('button','nf-retry','Сохранить мой текст'));
    for(const button of [retry,copy,latest,keep]){button.type='button';button.hidden=true;}actions.append(retry,copy,latest,keep);
    const content=make('div','nf-note-content');content.append(title,body,checklist);root.append(header,content,message,actions);
    const active=()=>current()&&!dead;
    let identityNode=initial,localProtection=false;
    const overview=options.resolveOverview?createFieldObjectOverview(root,{isCurrent:active,readIdentity:()=>localProtection?{title:'Под паролем',symbol:'lock'}:options.resolveOverview!(identityNode)}):null;
    const readOnly=()=>source.readOnly===true;
    const editingApi:MaterialApi={request<T>(method:string,args?:Record<string,unknown>):Promise<T>{
      if(method==='notes.put'&&(!active()||readOnly()))return Promise.reject(new Error('notes_read_only'));
      return options.api.request<T>(method,args);
    }};
    const focused=()=>root.contains(document.activeElement);
    const itemControls=new Set<HTMLInputElement>();
    function applyReadOnly(note?:Note) {
      title.readOnly=body.readOnly=readOnly();root.dataset.readOnly=String(readOnly());
      if(note)title.disabled=body.disabled=note.state==='trashed';
      for(const field of itemControls)field.disabled=readOnly()||note?.state==='trashed';
    }
    function mayRefresh() {
      return readOnly()?Boolean(readOnlyNote&&!focused()&&Number.isSafeInteger(source.summary?.revision)&&source.summary!.revision>readOnlyNote.revision)
        :mayRefreshNativeNote(session?.state()??null,focused(),source.summary?.revision);
    }
    function showError(error:unknown){if(!active())return;message.textContent=errorText(error);message.hidden=false;retry.hidden=false;status.textContent='Не загружено';}
    function paint(state:SessionState) {
      if(dead||readOnly())return;
      localProtection=!!state.localProtection;
      if(state.localProtection){
        overview?.clear();overview?.update();
        // A protection transition wins over focus/caret preservation.
        title.value=body.value='';title.disabled=body.disabled=true;checklist.replaceChildren();itemControls.clear();
        root.dataset.saveState='protection-pending';status.textContent=state.localProtection==='sealed'?'Под паролем':'Перенос не завершён';
        message.textContent=state.dirty?'Прежний черновик скрыт. Введите пароль объекта на полотне, чтобы завершить перенос на этом устройстве.':'Откройте объект по паролю на полотне.';message.hidden=false;
        retry.hidden=copy.hidden=latest.hidden=keep.hidden=true;return;
      }
      if(!active())return;
      const restoring=root.dataset.saveState==='protection-pending';
      status.textContent=nativeNoteStatus(state,stale&&!state.confirmedAt);root.dataset.saveState=state.localError?'error':state.conflict?'conflict':state.dirty?'local':'saved';root.dataset.color=state.note.color;
      if((restoring||document.activeElement!==title)&&title.value!==state.note.title)title.value=state.note.title;
      if((restoring||document.activeElement!==body)&&body.value!==state.note.body)body.value=state.note.body;
      if(restoring)renderItems(state.note);
      applyReadOnly(state.note);
      message.textContent=state.localError?errorText({code:state.localError}):state.conflict?'Ваш текст сохранён отдельным черновиком. Выберите версию.':state.error?errorText({code:state.error}):'';message.hidden=!message.textContent;
      retry.hidden=!state.error||state.conflict;copy.hidden=!state.conflict||!options.onNoteFork;latest.hidden=keep.hidden=!state.conflict;
      if(!state.dirty&&state.note.revision>lastNotifiedRevision){lastNotifiedRevision=state.note.revision;options.onNoteChange?.(state.note);}
    }
    function renderItems(note:Note) {
      checklist.replaceChildren();itemControls.clear();
      for(const item of note.items) {
        const row=make('label','nf-check'),checkbox=control(make('input')),text=control(make('input','nf-check-text'));checkbox.type='checkbox';checkbox.checked=item.done;text.value=item.text;text.maxLength=1000;checkbox.setAttribute('aria-label',item.text||'Отметить пункт');text.setAttribute('aria-label','Текст пункта');
        checkbox.disabled=text.disabled=readOnly()||note.state==='trashed';itemControls.add(checkbox);itemControls.add(text);
        const edit=(patch:Partial<typeof item>)=>{if(readOnly())return;if(session)session.edit({items:session.state().note.items.map(value=>value.id===item.id?{...value,...patch}:value)});};
        checkbox.addEventListener('change',()=>{if(readOnly())return;edit({done:checkbox.checked});});text.addEventListener('input',()=>{if(readOnly())return;edit({text:text.value});checkbox.setAttribute('aria-label',text.value||'Отметить пункт');});row.append(checkbox,text);checklist.append(row);
      }
    }
    function install(note:Note,draft?:Parameters<typeof createNoteSession>[0]['draft']) {
      if(readOnly()){
        session?.dispose();session=null;readOnlyNote=structuredClone(note);title.value=note.title;body.value=note.body;renderItems(note);applyReadOnly(note);
        root.dataset.saveState='readonly';root.dataset.color=note.color;status.textContent='Только чтение';message.textContent='';message.hidden=true;retry.hidden=copy.hidden=latest.hidden=keep.hidden=true;return;
      }
      readOnlyNote=null;
      session?.dispose();session=createNoteSession({api:editingApi,accountId:options.accountId,store,cache,note,...(draft?{draft}:{}),onChange:paint});
      title.value=session.state().note.title;body.value=session.state().note.body;renderItems(session.state().note);paint(session.state());
    }
    async function load(useDraft=true,force=false) {
      if(!active()||!visible&&!force)return;if(flight)return flight;
      const sequence=++request;
      flight=(async()=>{
        try {
          if(readOnly()){
            // Foreign object content never enters this viewer's personal Notes cache or draft/outbox.
            const result=await options.api.request<{note:Note}>('notes.get',{expectedAccountId:options.accountId,noteId:source.noteId});
            if(!active()||sequence!==request||!readOnly()||readOnlyNote&&focused()&&!force)return;
            install(result.note);return;
          }
          if(useDraft) {
            const drafts=await store.list();if(!active()||sequence!==request)return;
            const draft=drafts.find(value=>value.note.noteId===source.noteId&&(value.generation>value.committedGeneration||value.pending||value.conflict));
            if(draft){if(cache.canRead&&!await cache.canRead(source.noteId))throw new Error('notes_native_protection_pending');if(!active()||sequence!==request)return;stale=true;install(draft.note,draft);return;}
          }
          const result=await readNoteWithCache({api:options.api,accountId:options.accountId,noteId:source.noteId,cache,allowStale:useDraft,active:()=>active()&&sequence===request});
          if(!active()||sequence!==request)return;
          // A fresh response is not permission to clobber edits made while it was in flight.
          if(session&&(session.state().dirty||focused())&&!force)return;
          stale=result.source==='cache';install(result.note);
        }catch(error){if(active()&&sequence===request)showError(error);}
        finally{if(sequence===request)flight=null;}
      })();return flight;
    }
    title.addEventListener('input',()=>{if(readOnly())return;if(session?.state().localProtection){title.value='';return;}session?.edit({title:title.value});});body.addEventListener('input',()=>{if(readOnly())return;if(session?.state().localProtection){body.value='';return;}session?.edit({body:body.value});});
    title.addEventListener('keydown',event=>{if(readOnly())return;if(event.key==='Enter'&&!event.isComposing){event.preventDefault();body.focus();}});
    root.addEventListener('keydown',event=>{if(readOnly())return;if((event.ctrlKey||event.metaKey)&&event.key.toLowerCase()==='s'){event.preventDefault();void session?.flush().catch(showError);}});
    root.addEventListener('focusout',()=>{queueMicrotask(()=>{if(active()&&mayRefresh())void load(false);});});
    retry.addEventListener('click',()=>{if(readOnly())void load(false,true);else if(session)void session.retry();else void load();});
    latest.addEventListener('click',()=>{void (async()=>{try{if(!readOnly())await session?.flush();if(!active())return;await load(false,true);}catch(error){showError(error);}})();});
    keep.addEventListener('click',()=>{if(!active()||readOnly()||!session||keep.disabled)return;keep.disabled=true;void session.keepLocal().catch(showError).finally(()=>{keep.disabled=false;});});
    copy.addEventListener('click',()=>{void(async()=>{
      const previous=session;if(readOnly()||!previous||!options.onNoteFork)return;copy.disabled=true;
      try {
        await previous.flush();if(!active()||readOnly()||session!==previous)return;
        const original=previous.state().note,note={...blankNote(),title:original.title,body:original.body,items:structuredClone(original.items),color:original.color,pinned:original.pinned};
        const fork=createNoteSession({api:editingApi,accountId:options.accountId,store,cache,note});
        try{fork.edit({body:note.body});await fork.flush();if(!active()||readOnly())return;const state=fork.state();if(state.dirty||state.conflict||state.error)throw new Error(state.error||'notes_network_error');options.onNoteFork(state.note);}
        finally{fork.dispose();}
      }catch(error){showError(error);}finally{copy.disabled=false;}
    })();});
    function update(node:FieldNode) {
      identityNode=node;
      applySafeArea(node);
      if(!active()){overview?.clear();return;}
      const next=sourceFor(node);if(!next||next.kind!=='note'||next.noteId!==source.noteId){overview?.clear();return;}const modeChanged=(next.readOnly===true)!==readOnly();source=next;overview?.update();
      root.style.setProperty('--nf-font-size',`${nativeNoteFontSize(source.fontSize)}px`);
      if(modeChanged){request++;flight=null;session?.dispose();session=null;readOnlyNote=null;title.disabled=body.disabled=true;checklist.replaceChildren();itemControls.clear();copy.hidden=latest.hidden=retry.hidden=keep.hidden=true;}
      applyReadOnly(session?.state().note??readOnlyNote??undefined);
      if(!session&&!readOnlyNote){title.value=source.summary?.title??node.entity.title;body.value=source.summary?.preview??'';status.textContent=readOnly()?'Только чтение':'';}
      if(visible&&(modeChanged||mayRefresh()))void load(!readOnly(),modeChanged);
    }
    update(initial);
    const view:FieldObjectView={element:root,...(overview?{overview:overview.element,setPresentation:(mode:FieldObjectPresentation)=>overview.setPresentation(mode),glanceSize:()=>({width:320,height:240})}:{}),update,setVisible(value){visible=value;if(value&&!session&&!readOnlyNote)void load();},activate(){visible=true;void load().then(()=>{if(active())body.focus({preventScroll:true});});},
      async flush(){if(!readOnly())await session?.flush();},hasUnsavedChanges(){return !readOnly()&&(session?.hasUnsavedChanges()??false);},hasUnconfirmedWork(){const state=session?.state();return !readOnly()&&!!state&&(state.dirty||state.saving||state.conflict);},dispose(){if(dead)return;dead=true;overview?.dispose();request++;session?.dispose();session=null;readOnlyNote=null;title.value='';body.value='';checklist.replaceChildren();root.replaceChildren();root.textContent='';root.remove();views.delete(view);}};
    return view;
  }
  function mediaView(initial:FieldNode,initialSource:Exclude<NativeFieldSource,{kind:'note'}>):FieldObjectView {
    let source=initialSource,dead=false,visible=false,url='',flight:Promise<void>|null=null,controller:AbortController|null=null,loadedRevision=0;
    const root=make('article',`nf-object nf-${source.kind}`);root.dataset.nativeProvider='materials';root.setAttribute('aria-label',source.kind==='image'?'Фотография':source.kind==='video'?'Видео':'Аудиозапись');const applySafeArea=safeAreaFor(root);applySafeArea(initial);
    const header=make('div','nf-header'),name=make('span','nf-material-name',initial.entity.title),kind=make('span','nf-kind',source.kind==='image'?'Фото':source.kind==='video'?'Видео':'Голос');header.append(kind,name);
    const message=make('p','nf-message');message.setAttribute('role','status');message.hidden=true;
    const image=make('img','nf-image'),audio=control(source.kind==='video'?make('video','nf-video'):make('audio','nf-audio')),play=control(make('button','nf-play',source.kind==='image'?'Загрузить фотографию':'Воспроизвести'));play.type='button';
    image.alt=source.summary?.name??initial.entity.title;image.draggable=false;image.decoding='async';audio.controls=true;audio.preload='none';audio.autoplay=false;audio.hidden=true;audio.setAttribute('aria-label',source.kind==='video'?'Видео':'Голосовая запись');if(source.kind==='video')(audio as HTMLVideoElement).playsInline=true;
    if(source.kind==='image')root.append(image);else root.append(header,audio);root.append(message,play);
    const active=()=>current()&&!dead;
    let identityNode=initial;
    const overview=options.resolveOverview?createFieldObjectOverview(root,{isCurrent:active,readIdentity:()=>{const identity=options.resolveOverview!(identityNode);return identity?{...identity,visual:!!url&&(source.kind==='image'?image.complete&&image.naturalWidth>0:source.kind==='video'&&audio.readyState>=2)}:null;}}):null;
    function release(){if(url){audio.pause();audio.removeAttribute('src');audio.load();image.removeAttribute('src');URL.revokeObjectURL(url);url='';}loadedRevision=0;overview?.update();}
    async function load(explicitPlay=false) {
      if(!active())return;if(flight)return flight;if(url){if(explicitPlay)try{await audio.play();}catch{message.textContent='Нажмите воспроизведение на плеере';message.hidden=false;}return;}
      controller?.abort();controller=new AbortController();const signal=controller.signal;play.disabled=true;play.textContent='Загружаем…';root.setAttribute('aria-busy','true');message.hidden=true;
      flight=(async()=>{
        try {
          const result=await readMaterialBlob(options.api,{accountId:options.accountId,materialId:source.materialId,isCurrent:active,signal});
          if(!active()||signal.aborted)return;
          if(result.material.kind!==source.kind)throw new Error('materials_media_type');
          url=URL.createObjectURL(result.blob);loadedRevision=result.material.revision;
          if(source.kind==='image'){image.src=url;image.alt=result.material.name;}
          else {audio.src=url;audio.preload='metadata';audio.hidden=false;audio.load();if(explicitPlay)try{await audio.play();}catch{message.textContent='Нажмите воспроизведение на плеере';message.hidden=false;}}
          root.dataset.loaded='true';play.hidden=true;
          overview?.update();
        }catch(error){if(active()&&!signal.aborted){message.textContent=errorText(error);message.hidden=false;play.textContent='Повторить';play.hidden=false;}}
        finally{flight=null;if(active()){play.disabled=false;root.setAttribute('aria-busy','false');}}
      })();return flight;
    }
    image.addEventListener('error',()=>{if(active()){release();message.textContent='Не удалось показать фотографию';message.hidden=false;play.hidden=false;}});
    image.addEventListener('load',()=>{if(active())overview?.update();});audio.addEventListener('loadeddata',()=>{if(active())overview?.update();});
    audio.addEventListener('error',()=>{if(active()&&url){message.textContent='Этот браузер не смог воспроизвести запись';message.hidden=false;play.hidden=false;}});
    play.addEventListener('click',()=>{void load(source.kind!=='image');});
    function update(node:FieldNode) {identityNode=node;applySafeArea(node);if(!active()){overview?.clear();return;}const next=sourceFor(node);if(!next||next.kind==='note'||next.materialId!==source.materialId||next.kind!==source.kind){overview?.clear();return;}source=next;name.textContent=source.summary?.name??node.entity.title;if(source.summary&&loadedRevision&&source.summary.revision!==loadedRevision){release();if(visible&&source.kind==='image')void load();}overview?.update();}
    const view:FieldObjectView={element:root,...(overview?{overview:overview.element,setPresentation(mode:FieldObjectPresentation){overview.setPresentation(mode);if(source.kind==='video')audio.controls=mode!=='overview';}}:{}),update,setVisible(value){visible=value;if(value&&source.kind==='image')void load();else if(!value&&!url)controller?.abort();},activate(){visible=true;void load(source.kind!=='image');},async flush(){},hasUnsavedChanges(){return false;},dispose(){if(dead)return;dead=true;overview?.dispose();controller?.abort();release();name.textContent=image.alt='';root.replaceChildren();root.textContent='';root.remove();views.delete(view);}};
    return view;
  }
  return {createObject(node){if(!current())return null;const source=sourceFor(node);if(!source)return null;const view=source.kind==='note'?noteView(node,source):mediaView(node,source);views.add(view);return view;},
    objectKey(node){if(!current())return null;const source=sourceFor(node);return source?`${source.kind}:${source.kind==='note'?source.noteId:source.materialId}`:null;},
    async flush(){await Promise.all([...views].map(view=>view.flush()));},hasUnsavedChanges(){return [...views].some(view=>view.hasUnsavedChanges());},dispose(){if(disposed)return;disposed=true;for(const view of [...views])view.dispose();views.clear();}};
}
