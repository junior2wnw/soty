import {createDialog,pinDialogSubmit,type WorldDialog} from './dialogs';
import {el,button} from './dom';
import {createCanvasCreateFlow,canvasCreateFileKind,type CanvasCreateFlowOptions,type CanvasCreateFlow,type CanvasCreateResult,type CanvasCreateProgress} from './canvas-create-flow.mjs';
import type {CanvasMask} from '../../modules/canvas-objects/contract.mjs';
import type {AppDirectoryItem} from '../../modules/apps/server/directory.mjs';
import {mountCanvasVoiceCapture,type CanvasVoiceCapture} from './canvas-voice-capture';
import {mountAppConnectDialog} from './app-connect-dialog';
import './canvas-create-dialog.css';

type AppChoice={appId:string;name:string;entry?:{domainId:string;path:string}};
export interface CanvasCreateDialogOptions extends CanvasCreateFlowOptions {
  initial?:{kind:'note';body?:string}|({kind:'widget'}&AppChoice);
  onDone?:(result:CanvasCreateResult)=>void;
  onClose?:()=>void;
  onConnectApp?:()=>void;
  onConnectDevice?:()=>void;
  connectSelectedDevice?:string;
  onMore?:()=>void;
  parent?:HTMLElement;
}
export interface CanvasCreateDialog extends WorldDialog {flow:CanvasCreateFlow;dispose():void;hasUnsavedChanges():boolean;focus():void;selectConnectedApp(appId:string):Promise<boolean>}
export function canvasCreateErrorText(error:unknown):string {
  const code=String((error as {code?:string;message?:string})?.code||(error as Error)?.message||'');
  if(code==='canvas_create_media_type_unknown')return'Не удалось определить формат файла. Выберите PNG, JPEG, WebP, WebM, Ogg или WAV.';
  if(code==='canvas_create_media_type_unsupported')return'Этот формат пока не поддерживается. Подойдут PNG, JPEG, WebP, WebM, Ogg и WAV.';
  if(code==='canvas_create_file_size')return'Выберите непустой файл до 25 МБ.';
  if(code==='canvas_create_one_file')return'Выберите один файл.';
  if(code==='canvas_create_journal_quota')return'Недостаточно места для нового черновика. Завершите или удалите прежний.';
  if(/local_unavailable/u.test(code))return'Черновик не сохранён на устройстве. Освободите место и повторите.';
  if(/account_changed|scope_changed|authentication|access_denied|revoked/u.test(code))return'Доступ изменился. Откройте полотно заново.';
  if(/materials_(?:media|image|video|audio|file_hash|manifest_hash|chunk_invalid)/u.test(code))return'Не удалось прочитать файл. Проверьте его или выберите другой.';
  if(code==='canvas_create_placement_not_durable')return'Объект ещё не закреплён на полотне. Повторите размещение.';
  if(/quota|storage_limit/u.test(code))return'Недостаточно места в вашем пространстве. Освободите его и повторите.';
  return'Не удалось завершить создание. Нажмите «Повторить» — продолжим с сохранённого места.';
}

/** One composer. Source, metadata and placement stay in their existing authoritative services. */
export function openCanvasCreateDialog(options:CanvasCreateDialogOptions):CanvasCreateDialog {
  let disposed=false,busy=false,retryId:string|null=null,selectedFile:File|null=null,previewUrl='',app:AppChoice|null=options.initial?.kind==='widget'?options.initial:null;
  let mode:'note'|'file'|'widget'|'voice'|'connect'=app?'widget':'note',voiceBusy=false,voice:CanvasVoiceCapture|undefined,queryGeneration=0,searchTimer:ReturnType<typeof setTimeout>|undefined,connectionController:AbortController|undefined,connectingBusy=false;
  const controller=new AbortController(),signal=controller.signal;
  let flow:CanvasCreateFlow;
  const dialog=createDialog('Добавить на полотно',()=>{
    if(disposed)return;disposed=true;queryGeneration++;clearTimeout(searchTimer);controller.abort();connectionController?.abort();voice?.dispose();flow?.dispose();if(previewUrl)URL.revokeObjectURL(previewUrl);options.onClose?.();
  },undefined,options.parent);
  dialog.element.classList.add('cc-dialog');
  const form=el('form','cc-form'),modes=el('div','cc-modes'),noteMode=button('Записка','edit'),fileMode=button('Файл','attach'),voiceMode=button('Голос','bars'),widgetMode=button('Окно','grid');modes.setAttribute('role','group');modes.setAttribute('aria-label','Что добавить');modes.append(noteMode,fileMode,voiceMode,widgetMode);
  const voiceBox=el('div','cc-voice');voiceBox.hidden=true;
  const title=el('input','cc-title');title.placeholder='Название — необязательно';title.maxLength=160;title.setAttribute('aria-label','Название записки');
  const body=el('textarea','cc-body');body.placeholder='О чём думаете?';body.maxLength=100000;body.setAttribute('aria-label','Текст записки');body.value=options.initial?.kind==='note'?options.initial.body??'':'';
  const noteFields=el('div','cc-note-fields');noteFields.append(body);
  const picker=el('input');picker.type='file';picker.hidden=true;picker.accept='image/png,image/jpeg,image/webp,audio/webm,audio/ogg,audio/wav,video/webm';picker.setAttribute('aria-label','Выбрать фото, звук или видео');
  const fileBox=el('div','cc-file'),image=el('img','cc-preview'),fileName=el('span','cc-file-name'),changeFile=button('Выбрать другой файл','attach');image.alt='Предпросмотр выбранной фотографии';fileBox.hidden=true;fileBox.append(image,fileName,changeFile);
  const appsBox=el('div','cc-apps'),search=el('input','cc-app-search'),choice=el('div','cc-app-choice'),appResults=el('div','cc-app-results'),appStatus=el('p','cc-app-status');appsBox.hidden=true;search.type='search';search.placeholder='Найти приложение';search.setAttribute('aria-label','Найти доступное приложение');appResults.setAttribute('role','list');appStatus.setAttribute('role','status');
  const connectApp=button('Подключить своё','plus','cc-connect-app'),retryApps=button('Повторить поиск',undefined,'cc-app-retry');retryApps.hidden=true;connectApp.hidden=!options.onConnectApp&&!options.onConnectDevice;appsBox.append(choice,search,appStatus,appResults,retryApps,connectApp);
  const connectPanel=el('section','cc-connect-panel'),connectBack=button('Назад к окнам','back','cc-connect-back'),connectFields=el('div','cc-connect-fields');connectPanel.hidden=true;connectPanel.append(connectBack,connectFields);
  const settings=el('details','cc-settings'),settingsTitle=el('summary','','Оформление'),shape=el('select');shape.setAttribute('aria-label','Форма объекта');for(const [value,label]of [['rect','Карточка'],['ellipse','Круг'],['hex','Сота']] as const){const option=el('option','',label);option.value=value;shape.append(option);}shape.value='rect';settings.append(settingsTitle,title,shape);
  const dropHint=el('p','cc-drop-hint','Можно вставить или перетащить файл'),status=el('p','cc-status'),message=el('p','cc-error');status.setAttribute('role','status');status.setAttribute('aria-live','polite');message.setAttribute('role','alert');message.hidden=true;
  const fresh=button('Создать другое');fresh.classList.add('cc-fresh');fresh.hidden=true;
  const recovery=el('details','cc-recovery'),recoveryTitle=el('summary'),pendingList=el('div','cc-pending');recovery.hidden=true;recovery.append(recoveryTitle,pendingList);
  const more=button('Ещё','more','cc-more');more.hidden=!options.onMore;more.setAttribute('aria-label','Ещё');more.title='Ещё';
  form.append(modes,noteFields,picker,fileBox,voiceBox,appsBox,connectPanel,settings,dropHint,status,message,fresh,recovery);dialog.body.append(form);
  const header=dialog.element.querySelector('.sw-dialog-header');header?.insertBefore(more,header.querySelector('button'));
  const submit=button('На полотно','plus','sw-button-primary');pinDialogSubmit(dialog,form,submit);
  const current=()=>!disposed&&options.isCurrent();
  function progress(value:CanvasCreateProgress){if(!current())return;try{options.onProgress?.(value);}catch{/* Host observers do not interrupt creation. */}
    status.textContent=value.phase==='preparing'?'Подготавливаем файл…':value.phase==='source'?(value.total?`Загружаем ${Math.round((value.uploaded??0)/value.total*100)}%`:'Сохраняем…'):value.phase==='object'?'Подготавливаем карточку…':value.phase==='placement'?'Размещаем…':value.phase==='complete'?'Готово':value.localDurable?'Черновик сохранён':'Черновик не сохранён';}
  flow=createCanvasCreateFlow({...options,onProgress:progress});
  voice=mountCanvasVoiceCapture(voiceBox,{isCurrent:current,signal,onFile:file=>fileSelected(file),onRecordingChange:recording=>{voiceBusy=recording;if(current())paint();}});
  function showError(error:unknown){if(!current())return;message.textContent=canvasCreateErrorText(error);message.hidden=false;const value=error as {operationId?:string;localDurable?:boolean};
    if(value.operationId){retryId=value.operationId;fresh.hidden=false;status.textContent=value.localDurable?'Черновик сохранён на устройстве':'Черновик пока только на этом экране';}paint();void loadPending();}
  function paint(){
    noteFields.hidden=mode!=='note';title.hidden=mode!=='note';fileBox.hidden=mode!=='file';voiceBox.hidden=mode!=='voice';appsBox.hidden=mode!=='widget';dropHint.hidden=mode==='widget'||mode==='voice'||mode==='connect';
    connectPanel.hidden=mode!=='connect';settings.hidden=mode==='connect';modes.hidden=mode==='connect';more.hidden=mode==='connect'||!options.onMore;connectBack.disabled=connectingBusy;recovery.hidden=mode==='connect'||pendingList.children.length===0;
    for(const [element,active]of [[noteMode,mode==='note'],[fileMode,mode==='file'],[voiceMode,mode==='voice'],[widgetMode,mode==='widget']] as const){element.setAttribute('aria-pressed',String(active));element.disabled=busy||voiceBusy||!!retryId;}
    title.readOnly=body.readOnly=busy||!!retryId;shape.disabled=busy||voiceBusy||!!retryId;search.disabled=busy||!!retryId;changeFile.disabled=busy||voiceBusy||!!retryId;connectApp.disabled=busy||voiceBusy||!!retryId;retryApps.disabled=busy||!!retryId;more.disabled=busy||voiceBusy;
    voice?.setDisabled(busy||!!retryId||mode!=='voice');
    for(const control of pendingList.querySelectorAll<HTMLButtonElement>('button'))control.disabled=busy||connectingBusy;
    if(mode!=='connect'){submit.hidden=false;submit.disabled=busy||voiceBusy||mode==='voice'||(!retryId&&mode==='widget'&&!app)||(!retryId&&mode==='file'&&!selectedFile);submit.textContent=busy?'Создаём…':retryId?'Повторить':'На полотно';}fresh.disabled=busy;
    choice.replaceChildren();if(app){choice.append(el('strong','',app.name));search.placeholder='Выбрать другое приложение';}
  }
  function setMode(next:typeof mode){if(busy||voiceBusy||retryId||connectingBusy)return;mode=next;message.hidden=true;status.textContent='';paint();if(next==='widget'){search.focus();void findApps();}else if(next==='note')body.focus();}
  function leaveConnect(){if(!current()||connectingBusy)return;connectionController?.abort();connectFields.replaceChildren();retryId=null;setMode('widget');}
  async function openConnect(){
    if(!current()||busy||voiceBusy||!options.onConnectDevice)return;connectionController?.abort();connectionController=new AbortController();const own=connectionController;retryId=null;mode='connect';queryGeneration++;clearTimeout(searchTimer);message.hidden=true;status.textContent='';paint();
    const connectionCurrent=()=>current()&&mode==='connect'&&connectionController===own&&!own.signal.aborted;
    await mountAppConnectDialog({dialog,api:options.api,accountId:options.accountId,current:connectionCurrent,accountCurrent:current,resultKind:'canvas',signal:own.signal,
      ...(options.connectSelectedDevice?{selectedDevice:options.connectSelectedDevice}:{}),connectDevice:()=>{if(connectionCurrent())options.onConnectDevice?.();},
      embedded:{host:connectFields,form,submit,onBusy:value=>{if(connectionCurrent()){connectingBusy=value;paint();}}},
      canvas:{projectId:options.projectId,prepare:requestId=>flow.prepareConnectedWidget(requestId,{mask:{shape:shape.value} as CanvasMask}),complete:value=>flow.completeConnectedWidget({requestId:value.requestId,appId:value.app.id}),settle:requestId=>flow.settleConnectedWidget(requestId)},
      ...(options.onConnectApp?{onLegacyResume:()=>{if(connectionCurrent()){leaveConnect();options.onConnectApp?.();}}}:{}),
      onComplete:result=>{if(!connectionCurrent()||result.resultKind!=='canvas')return;try{options.onDone?.(result.creation);}catch{/* The linked placement is already durable. */}if(!connectionCurrent())return;
        if(title.value||body.value||selectedFile){connectingBusy=false;own.abort();connectFields.replaceChildren();mode=selectedFile?'file':'note';status.textContent=selectedFile?'Окно на полотне. Выбранный файл остался в форме.':'Окно на полотне. Текст записки остался в форме.';paint();if(mode==='note')body.focus();else submit.focus();}
        else dialog.close();},
    });
  }
  function fileSelected(file:File){if(busy||voiceBusy||retryId)return;try{const kind=canvasCreateFileKind(file);if(previewUrl){URL.revokeObjectURL(previewUrl);previewUrl='';}selectedFile=file;const size=file.size<1024?`${file.size} Б`:file.size<1024*1024?`${Math.ceil(file.size/1024)} КБ`:`${(file.size/1024/1024).toLocaleString('ru-RU',{maximumFractionDigits:1})} МБ`;fileName.textContent=`${file.name} · ${size}`;
      image.hidden=kind!=='image';if(kind==='image'){previewUrl=URL.createObjectURL(file);image.src=previewUrl;}else image.removeAttribute('src');setMode('file');}catch(error){showError(error);}}
  noteMode.addEventListener('click',()=>setMode('note'),{signal});widgetMode.addEventListener('click',()=>setMode('widget'),{signal});
  voiceMode.addEventListener('click',()=>setMode('voice'),{signal});
  connectApp.addEventListener('click',()=>{if(current()&&!busy&&!retryId){if(options.onConnectDevice)void openConnect().catch(showError);else options.onConnectApp?.();}},{signal});connectBack.addEventListener('click',leaveConnect,{signal});more.addEventListener('click',()=>{if(current()&&!busy)options.onMore?.();},{signal});
  fileMode.addEventListener('click',()=>{if(!busy&&!retryId)picker.click();},{signal});changeFile.addEventListener('click',()=>{if(!busy&&!retryId)picker.click();},{signal});
  picker.addEventListener('change',()=>{const file=picker.files?.[0];if(file)fileSelected(file);picker.value='';},{signal});
  form.addEventListener('dragover',event=>{if(event.dataTransfer?.types.includes('Files')){event.preventDefault();if(!busy&&!retryId)form.classList.add('is-dropping');}},{signal});
  form.addEventListener('dragleave',()=>form.classList.remove('is-dropping'),{signal});
  form.addEventListener('drop',event=>{const files=event.dataTransfer?.files;if(!files?.length)return;event.preventDefault();form.classList.remove('is-dropping');if(files.length!==1){showError({code:'canvas_create_one_file',message:'Выберите один файл'});return;}fileSelected(files[0]!);},{signal});
  form.addEventListener('paste',event=>{const files=event.clipboardData?.files;if(!files?.length)return;event.preventDefault();if(files.length===1)fileSelected(files[0]!);else showError({code:'canvas_create_one_file'});},{signal});
  fresh.addEventListener('click',()=>{if(busy)return;retryId=null;fresh.hidden=true;message.hidden=true;status.textContent='';paint();},{signal});
  async function findApps(){
    if(!current()||mode!=='widget')return;const generation=++queryGeneration;appStatus.textContent='Ищем…';retryApps.hidden=true;
    try{const result=await options.api.request<{apps:AppDirectoryItem[]}>('apps.directory.search',{expectedAccountId:options.accountId,query:search.value.trim().slice(0,160),scope:'all',limit:20});
      if(!current()||generation!==queryGeneration||mode!=='widget')return;appResults.replaceChildren();
      for(const value of result.apps){const select=button(value.name,'grid','cc-app-result',()=>{if(busy||retryId)return;app={appId:value.id,name:value.name};paint();});select.setAttribute('role','listitem');appResults.append(select);}
      appStatus.textContent=result.apps.length?'':'Доступных приложений пока нет';
    }catch(error){if(current()&&generation===queryGeneration){appStatus.textContent=/access|authentication|revoked/u.test(String((error as {code?:string})?.code))?canvasCreateErrorText(error):'Не удалось загрузить приложения. Повторите поиск.';retryApps.hidden=false;}}
  }
  async function selectConnectedApp(appId:string):Promise<boolean>{
    if(!current()||mode!=='widget'||busy||voiceBusy||retryId)return false;
    if(!/^app-[a-f0-9]{32}$/u.test(appId))throw Object.assign(new Error('canvas_create_app_unavailable'),{code:'canvas_create_app_unavailable'});
    const generation=++queryGeneration;clearTimeout(searchTimer);
    const result=await options.api.request<{items:{ref:{kind:string;id:string};available:boolean;app?:{id:string;name:string;entry?:{appId:string;domainId:string;path:string}}}[]}>('apps.directory.resolve',{expectedAccountId:options.accountId,appIds:[appId]});
    if(!current()||generation!==queryGeneration||mode!=='widget'||busy||voiceBusy||retryId)return false;
    const value=result.items?.find(item=>item.ref?.kind==='app'&&item.ref.id===appId),entry=value?.app?.entry;
    if(!value?.available||value.app?.id!==appId||typeof value.app.name!=='string'||!entry||entry.appId!==appId||typeof entry.domainId!=='string'||!entry.domainId||typeof entry.path!=='string'||!entry.path.startsWith('/'))throw Object.assign(new Error('canvas_create_app_unavailable'),{code:'canvas_create_app_unavailable'});
    app={appId,name:value.app.name,entry:{domainId:entry.domainId,path:entry.path}};
    appResults.replaceChildren();appStatus.textContent='Приложение выбрано';retryApps.hidden=true;message.hidden=true;paint();return true;
  }
  retryApps.addEventListener('click',()=>{if(!busy&&!retryId)void findApps();},{signal});
  search.addEventListener('input',()=>{queryGeneration++;clearTimeout(searchTimer);searchTimer=setTimeout(()=>{void findApps();},220);},{signal});
  async function loadPending(){try{const values=await flow.listPending();if(!current())return;pendingList.replaceChildren();recovery.hidden=values.length===0;recoveryTitle.textContent=`Продолжить создание · ${values.length}`;
      for(const value of values){const row=el('div','cc-pending-row'),label=el('span','',value.label),resume=button('Продолжить',undefined,'',()=>{if(!busy){if(value.connectionRequestId||value.stage==='connect')void openConnect().catch(showError);else void perform(()=>flow.resume(value.operationId));}}),remove=button('Удалить черновик',undefined,'cc-remove',()=>{if(busy)return;void flow.removeLocal(value.operationId).then(()=>{if(!current())return;if(retryId===value.operationId){retryId=null;fresh.hidden=true;paint();}void loadPending();}).catch(()=>{if(current()){message.textContent='Не удалось удалить черновик. Он остался в списке.';message.hidden=false;}});});remove.hidden=!!value.connectionRequestId&&value.stage!=='connect';row.append(label,resume,remove);pendingList.append(row);}paint();
    }catch(error){if(current()){recovery.hidden=false;recoveryTitle.textContent='Черновики недоступны';if(!message.textContent){message.textContent=canvasCreateErrorText(error);message.hidden=false;}}}}
  async function perform(action:()=>Promise<CanvasCreateResult>){if(busy||!current())return;busy=true;message.hidden=true;paint();try{const result=await action();if(!current())return;try{options.onDone?.(result);}catch{/* Placement is already durable. */}dialog.close();}catch(error){showError(error);}finally{busy=false;if(current()){paint();void loadPending();}}}
  form.addEventListener('submit',event=>{event.preventDefault();if(busy||!current())return;const mask={shape:shape.value} as CanvasMask;
    if(retryId){void perform(()=>flow.resume(retryId!));return;}
    if(mode==='note')void perform(()=>flow.startNote({title:title.value,body:body.value,mask,fontSize:16}));
    else if(mode==='file'&&selectedFile)void perform(()=>flow.startFile(selectedFile!,{mask}));
    else if(mode==='widget'&&app){const value=app;void perform(()=>flow.startWidget({appId:value.appId,...(value.entry?{entry:value.entry}:{}),mask}));}
  },{signal});
  paint();void loadPending();if(mode==='widget'){submit.focus();void findApps();}else body.focus();
  return{...dialog,flow,selectConnectedApp,hasUnsavedChanges:()=>!disposed&&(busy||voiceBusy||mode==='connect'||!!title.value||!!body.value||!!selectedFile),focus(){if(mode==='note')body.focus();else if(mode==='widget')search.focus();else submit.focus();},dispose(){dialog.close({restoreFocus:false});}};
}
