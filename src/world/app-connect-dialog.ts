import {button,el,labeledField,textInput,emptyState} from './dom';
import {pinDialogSubmit,errorText,type WorldDialog} from './dialogs';
import {createAppFieldPlacementController} from './app-field-placement-browser';
import {createAppConnectFlow,createAppConnectJournal,type AppConnectResult,type AppConnectCanvasBridge} from './app-connect-flow.mjs';
import {createAppConnectLocks} from './app-connect-locks.mjs';
import type {WorldApi} from './types';
import './app-connect-dialog.css';

interface Device {hostDeviceId:string;connectorId:string;name:string;online:boolean;claimed:boolean;sourceEndpoints?:{state:'ready'|'pending'|'offline'|'upgrade-required';addresses?:{family:'IPv4';address:string}[]}}
const deviceKey=(value:Pick<Device,'hostDeviceId'|'connectorId'>)=>`${value.hostDeviceId}|${value.connectorId}`;
const explanations:Record<string,string>={
  app_connect_source_host_unsupported:'Укажите localhost или IPv4 выбранного устройства. Адрес чужого устройства, домен и IPv6 пока не поддерживаются.',
  apps_endpoint_connector_upgrade_required:'Для подключения по IP обновите коннектор выбранного устройства и сервер Сот. Подключение сохранено; затем можно продолжить.',
  apps_endpoint_inventory_pending:'Устройство ещё не подтвердило свои адреса. Проверьте соединение и продолжите подключение.',
  apps_endpoint_not_on_selected_device:'Этот IP не принадлежит выбранному устройству. Выберите нужное устройство или исправьте адрес.',
  apps_endpoint_interface_unavailable:'Адрес устройства изменился или временно недоступен. Проверьте сеть и продолжите; второй проект создан не будет.',
  app_source_unreachable:'Приложение не ответило на указанном адресе выбранного устройства. Запустите его и повторите подключение.',
  apps_endpoint_request_conflict:'Сохранённый запрос уже относится к другому источнику. Проверьте прежнее подключение; оно не было перезаписано.',
  app_connect_source_protocol_unsupported:'Локальный коннектор принимает HTTP. Укажите http://localhost:порт; HTTPS-источник пока не поддерживается.',
  app_connect_invalid_address:'Проверьте адрес: например, localhost:3000. Фрагменты# и служебные пути пока не поддерживаются.',
  app_connect_invalid_port:'Нужен порт от1024 до65535, кроме49424. Например, localhost:3000.',
  app_connect_credentials_in_address:'Уберите пароль, токен или код входа из адреса. Используйте обычный адрес приложения.',
  app_connect_field_pending:'Приложение подключено. Соту ещё не удалось подтвердить на сервере. Продолжите — регистрация не повторится.',
  app_connect_placement_unconfirmed:'Приложение подключено, но сота на поле пока не подтверждена. Продолжите сохранение.',
  app_connect_storage_unavailable:'Не удалось сохранить ход подключения на устройстве. Сетевые действия не начинаем без сохранённого запроса.',
  app_connect_journal_corrupt:'Сохранённое подключение не удалось прочитать. Его данные не удалены.',
  app_connect_settings_busy:'У приложения уже есть незавершённое изменение в настройках. Сначала проверьте его там.',
  app_connect_publication_conflict:'Доступ к приложению изменился в другом окне. Проверьте настройки приложения; текущий выбор не перезаписан.',
  app_connect_source_changed:'Источник приложения изменился. Проверьте его настройки перед продолжением.',
  apps_named_zone_disabled:'Приложение подключено. Сервер пока не выдаёт адрес для доступа по ссылке и для всех; сота не выдана за опубликованную.',
  apps_audience_migration_required:'Приложение сохранено, но сервер ещё не поддерживает выбранную видимость. После его обновления продолжите подключение.',
  app_connect_app_unavailable:'Приложение больше не доступно для подключения. Его регистрация сохранена; проверьте настройки перед продолжением.',
  canvas_create_app_unavailable:'Приложение подключено, но доступ к нему пока не подтверждён. Повторите — откроем его в том же выборе окна.',
  app_connect_selection_context_changed:'Подключение сохранено. Продолжите его из «Добавить → Окно», чтобы выбрать приложение в текущем полотне.',
};

interface AppConnectDialogOptions {dialog:WorldDialog;api:WorldApi;accountId:string;current():boolean;accountCurrent():boolean;selectedDevice?:string;preferredContextId?:string;communityId?:string;initialAppIds?:string[];resultKind?:'shortcut'|'selection'|'canvas';canvas?:AppConnectCanvasBridge;embedded?:{host:HTMLElement;form:HTMLFormElement;submit:HTMLButtonElement;onBusy?(busy:boolean):void};signal?:AbortSignal;onLegacyResume?:()=>void;connectDevice():void;onComplete(result:AppConnectResult):Promise<void>|void}
export async function mountAppConnectDialog(options:AppConnectDialogOptions):Promise<void>{
  let {dialog,api,accountId,current,accountCurrent,selectedDevice,preferredContextId,communityId,initialAppIds=[],resultKind,connectDevice,onComplete,embedded,canvas,signal}=options;
  const target=embedded?.host??dialog.body;
  if(embedded){embedded.submit.disabled=true;embedded.submit.hidden=false;}
  let devices:Device[];
  try{const response=await api.request<{devices:Device[]}>('apps.devices',{expectedAccountId:accountId});if(!current())return;devices=response.devices.filter(value=>value.claimed);}
  catch(error){if(current())target.replaceChildren(el('p','sw-error',errorText(error)));return;}
  if(!devices.length){
    let setupOpened=false,checking=false;
    const resume=()=>{if(!setupOpened||checking||!current())return;checking=true;dialog.element.removeEventListener('focusin',resume);void mountAppConnectDialog(options).catch(reason=>{if(current())target.replaceChildren(el('p','sw-error',errorText(reason)));});};
    const connect=button('Подключить устройство','laptop','sw-button-primary',()=>{if(!current())return;if(setupOpened){resume();return;}setupOpened=true;connect.querySelector('span')!.textContent='Продолжить';dialog.element.addEventListener('focusin',resume);connectDevice();});
    dialog.element.addEventListener('close',()=>dialog.element.removeEventListener('focusin',resume),{once:true,...(signal?{signal}:{})});signal?.addEventListener('abort',()=>dialog.element.removeEventListener('focusin',resume),{once:true});
    if(embedded)embedded.submit.hidden=true;
    target.replaceChildren(emptyState('Подключите устройство','На нём будет работать ваше приложение.',connect,'laptop'));return;
  }
  const form=embedded?.form??el('form','sw-app-connect-form'),address=textInput('','localhost:3000',8192);address.required=true;address.setAttribute('aria-label','Адрес приложения');address.inputMode='url';address.autocomplete='off';
  const name=textInput('','Например, Моя галерея',64);name.setAttribute('aria-label','Название приложения');
  const device=el('select','sw-select');device.setAttribute('aria-label','Устройство приложения');
  const preferred=devices.find(value=>value.hostDeviceId===selectedDevice)||devices.find(value=>value.online)||devices[0];
  for(const item of devices){const option=el('option','',`${item.name}${item.online?'':' · не в сети'}`);option.value=deviceKey(item);option.selected=item===preferred;device.append(option);}
  const audience=el('fieldset','sw-app-connect-audience'),legend=el('legend','',canvas?'Приложение':'Кому доступно');audience.append(legend);
  const radios:HTMLInputElement[]=[];const radioName=`app-connect-audience-${crypto.randomUUID()}`;
  const audienceChoices=[['private','Только я'],['link','По ссылке'],['public','Все'],...(communityId?[['community','Это сообщество']]:[])];
  for(const [value,label]of audienceChoices){const labelNode=el('label'),radio=el('input');radio.type='radio';radio.name=radioName;radio.value=value!;radio.checked=communityId?value==='community':value==='private';labelNode.append(radio,el('span','',label));audience.append(labelNode);radios.push(radio);}
  const hint=el('p','sw-muted sw-app-connect-exposure','По ссылке или для всех откроется приложение целиком.');hint.hidden=true;
  const extra=el('details','sw-app-connect-details');extra.append(el('summary','','Название'),labeledField('Название приложения',name));
  const error=el('p','sw-error');error.setAttribute('role','alert');const status=el('p','sw-muted');status.setAttribute('role','status');
  const submit=embedded?.submit??button('Подключить','plus','sw-button-primary sw-button-wide');submit.type='submit';submit.hidden=false;
  const sourceHint=el('p','sw-muted');
  function updateSourceHint():void{const selected=devices.find(value=>deviceKey(value)===device.value),addresses=selected?.sourceEndpoints?.addresses?.map(value=>value.address)??[];sourceHint.textContent=addresses.length?`На этом устройстве: localhost или ${addresses.join(', ')}.`:selected?.sourceEndpoints?.state==='upgrade-required'?'На этом устройстве доступен localhost. Для подключения по IP обновите коннектор.':'Укажите localhost или IP этого устройства; коннектор подтвердит адрес.';}
  device.addEventListener('change',updateSourceHint,signal?{signal}:undefined);updateSourceHint();
  const deviceField=labeledField('Устройство',device),chosenDevice=el('p','sw-muted',`Устройство: ${preferred!.name}`);deviceField.hidden=devices.length===1;chosenDevice.hidden=devices.length!==1;
  const fields=[labeledField('Адрес приложения',address),sourceHint,deviceField,chosenDevice,audience,hint,...(canvas?[el('p','sw-muted','Окно появится только у вас.')]:[]),extra,error,status];
  const legacy=button('Продолжить прежнее подключение','back','sw-button-quiet',()=>{if(current())options.onLegacyResume?.();});legacy.hidden=true;fields.push(legacy);
  if(embedded)target.replaceChildren(...fields);else{form.append(...fields);target.replaceChildren(form);pinDialogSubmit(dialog,form,submit);}
  let busy=false,locked=false;
  const controls=[address,name,device,...radios];
  const setLabel=(value:string)=>{(embedded?submit:submit.querySelector('span')??submit).textContent=value;};
  let flow:ReturnType<typeof createAppConnectFlow>;
  function reflect():void{
    if(!current())return;let pending;try{pending=flow?.pending();}catch(reason){error.textContent=explanations[(reason as{code?:string})?.code??'']||errorText(reason);submit.disabled=true;return;}locked=Boolean(pending&&!pending.complete&&!pending.rejected);
    if(locked&&pending)resultKind=pending.intent.resultKind??'shortcut';
    if(locked&&pending?.intent.communityId){communityId=pending.intent.communityId;if(!radios.some(value=>value.value==='community')){const row=el('label'),radio=el('input');radio.type='radio';radio.name=radioName;radio.value='community';row.append(radio,el('span','','Это сообщество'));audience.append(row);radios.push(radio);controls.push(radio);}}
    audience.classList.toggle('has-community',radios.length===4);
    if(locked&&pending){address.value=pending.intent.address;name.value=pending.intent.name;const previous=devices.find(value=>value.hostDeviceId===pending.intent.hostDeviceId&&(!pending.intent.connectorId||value.connectorId===pending.intent.connectorId));if(previous)device.value=deviceKey(previous);radios.forEach(radio=>radio.checked=radio.value===pending.intent.audience);updateSourceHint();}
    controls.forEach(control=>control.disabled=busy||locked);submit.disabled=busy;setLabel(busy?'Подключаем и размещаем…':canvas?(locked&&pending?.configured?'Продолжить размещение':'Подключить и разместить'):locked?'Продолжить подключение':'Подключить');hint.hidden=!radios.some(radio=>radio.checked&&['link','public'].includes(radio.value));
    legacy.hidden=true;
    if(embedded&&locked&&resultKind!=='canvas'){submit.disabled=true;error.textContent='Есть прежнее подключение с другим действием. Сначала завершите его.';legacy.hidden=!options.onLegacyResume;}
    if(resultKind==='canvas'&&(!canvas||pending?.intent.canvasProjectId!==undefined&&pending.intent.canvasProjectId!==canvas.projectId)){submit.disabled=true;error.textContent='Продолжите размещение через «Добавить» → «Окно» → «Подключить своё».';}
    if(locked&&pending&&!devices.some(value=>value.hostDeviceId===pending.intent.hostDeviceId&&(!pending.intent.connectorId||value.connectorId===pending.intent.connectorId))){error.textContent='Устройство прежнего подключения больше не связано с аккаунтом. Сначала подключите его снова.';submit.disabled=true;}
  }
  try{
    const storage=localStorage,locks=createAppConnectLocks();const journal=createAppConnectJournal({accountId,storage,locks});
    flow=createAppConnectFlow({api,accountId,journal,storage,locks,isCurrent:accountCurrent,...(preferredContextId?{preferredContextId}:{}),initialAppIds,
      ...(canvas?{canvas}:{}),createPlacement:appId=>createAppFieldPlacementController({api,accountId,appId,isCurrent:accountCurrent,initialAppIds}),onStatus:text=>{if(current())status.textContent=text;}});reflect();
  }catch(reason){error.textContent=explanations[(reason as {code?:string})?.code??'']||errorText(reason);submit.disabled=true;return;}
  for(const radio of radios)radio.addEventListener('change',()=>{hint.hidden=!radios.some(value=>value.checked&&['link','public'].includes(value.value));},signal?{signal}:undefined);
  form.addEventListener('submit',event=>{
    event.preventDefault();if(!current()||busy||submit.disabled||!form.reportValidity())return;
    const chosen=devices.find(value=>deviceKey(value)===device.value);if(!chosen)return;
    busy=true;embedded?.onBusy?.(true);error.textContent='';status.textContent='Сохраняем подключение…';reflect();
    const label=name.value.trim()||`Приложение ${address.value.match(/:(\d+)/u)?.[1]||''}`.trim();
    void flow.run({name:label,address:address.value,hostDeviceId:chosen.hostDeviceId,connectorId:chosen.connectorId,audience:radios.find(radio=>radio.checked)!.value as 'private'|'link'|'public'|'community',...(communityId?{communityId}:{}),...(resultKind==='selection'||resultKind==='canvas'?{resultKind}:{}),...(resultKind==='canvas'?{canvasProjectId:canvas!.projectId}:{})})
      .then(async result=>{
        if(result.resultKind==='selection'){
          if(!current())return;await onComplete(result);if(!current())return;await flow.confirmSelection(result);if(current())dialog.close({restoreFocus:false});
        }else if(result.resultKind==='canvas'){if(current())await onComplete(result);}
        else if(accountCurrent())await onComplete(result);
      })
      .catch(reason=>{if(current()){let pending;try{pending=flow.pending();}catch{/* reflect reports an unreadable journal below. */}status.textContent=canvas&&pending?.configured?(pending.complete?'Окно размещено. Завершаем подтверждение…':'Приложение подключено. Осталось разместить окно.') :'';error.textContent=explanations[reason?.code]||(canvas&&pending?.configured?'Нажмите «Продолжить» — используем сохранённое подключение и окно.':errorText(reason));}})
      .finally(()=>{busy=false;embedded?.onBusy?.(false);if(current())reflect();});
  },signal?{signal}:undefined);
  if(!locked)address.focus();
}
