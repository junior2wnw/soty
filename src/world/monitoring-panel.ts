import uPlot from '../../vendor/uplot';
import '../../vendor/uplot/dist/uPlot.min.css';
import {createMonitoringClient, type MonitoringScope, type MonitoringDescription, type MonitoringSeries, type MonitoringConfigCommand, type MonitoringAction, type MonitoringAdminCommand} from '../../modules/monitoring/sdk.mjs';
import {button, el, labeledField} from './dom';
import type {DirectoryApi} from './field-directory';
import {MONITORING_RANGES, monitoringWindow, monitoringPlotData, monitoringPreviousWindow, monitoringComparisonData, monitoringPeriod, monitoringAxisTime, monitoringPointMeaning, monitoringValue, monitoringTime, monitoringStatus, monitoringHistoricalStatus, monitoringErrorText, monitoringAccessLost} from './monitoring-panel-model.mjs';
import './monitoring-panel.css';

export interface MonitoringPanelOptions {
  api: DirectoryApi;
  accountId: string;
  scope: MonitoringScope;
  isCurrent(): boolean;
  title?: string;
}
export interface MonitoringPanelHandle {
  ready: Promise<void>;
  refresh(): Promise<void>;
  setVisible(visible: boolean): void;
  hasUnsavedChanges(): boolean;
  hasUnconfirmedWork(): boolean;
  flush(options?: {allowDurablePending?: boolean}): Promise<void>;
  dispose(): void;
}

/** One view of the signed module. Metric truth, configuration and retry journal belong to its SDK/service. */
export function mountMonitoringPanel(host: HTMLElement, options: MonitoringPanelOptions): MonitoringPanelHandle {
  const view=host.ownerDocument.defaultView!, lifetime=new AbortController(), signal=lifetime.signal;
  let disposed=false,visible=true;
  const root=el('section','mon-panel');root.dataset.monitoringPanel='';root.dataset.scope=options.scope.kind;root.dataset.pwaIgnore='';
  root.setAttribute('aria-label',options.title??(options.scope.kind==='platform'?'Состояние Сот':'Состояние приложения'));
  const current=()=>!disposed&&options.isCurrent()&&root.isConnected;
  const client=createMonitoringClient({request:(op,args)=>options.api.request(op,args),scope:options.scope,
    currentAccountId:()=>options.isCurrent()?options.accountId:null,isCurrent:current});
  const header=el('div','mon-state-row'), state=el('p','mon-state','Проверяем состояние…'), updated=el('p','mon-updated');
  state.setAttribute('role','status');header.append(state,updated);
  const controls=el('div','mon-controls'), metric=el('select','sw-select mon-metric'), range=el('select','sw-select mon-range');
  metric.setAttribute('aria-label','Показатель');range.setAttribute('aria-label','Период');
  for(const preset of MONITORING_RANGES){const option=el('option','',preset.label);option.value=preset.id;range.append(option);}
  const customOption=el('option','','Свой период');customOption.value='custom';range.append(customOption);range.value='hour';
  const liveButton=button('Автообновление',undefined,'sw-button-quiet mon-live',()=>{
    live=!live;liveButton.setAttribute('aria-pressed',String(live));liveButton.title=live?'График обновляется автоматически':'Обновление графика приостановлено; сбор данных не меняется';
    if(live){zoomed=false;resetZoom.hidden=true;if(range.value==='custom'){range.value='hour';custom.hidden=true;clearPlot();}void refresh();}else{readEpoch++;root.removeAttribute('aria-busy');stopTimer();}
  });liveButton.setAttribute('aria-pressed','true');
  const refreshButton=button('','refresh','sw-button-quiet sw-icon-button mon-refresh',()=>{void refresh();});refreshButton.setAttribute('aria-label','Обновить показатели');refreshButton.title='Обновить показатели';
  const compareButton=button('Сравнить',undefined,'sw-button-quiet mon-compare',()=>{
    if(!current()||!visible)return;compareEnabled=!compareEnabled;compareButton.setAttribute('aria-pressed',String(compareEnabled));readEpoch++;root.removeAttribute('aria-busy');clearPlot();empty.textContent=compareEnabled?'Сравниваем периоды…':'Обновляем показатели…';void refresh();
  });compareButton.setAttribute('aria-pressed','false');compareButton.setAttribute('aria-label','Сравнить с предыдущим равным периодом');compareButton.title='Сравнить с непосредственно предыдущим равным периодом';compareButton.disabled=true;
  const periodField=el('div','sw-field mon-period-field'),periodHeading=el('div','mon-period-heading'),periodLabel=el('label','sw-field-label','Период');
  range.id=`monitoring-range-${view.crypto.randomUUID()}`;periodLabel.htmlFor=range.id;periodHeading.append(periodLabel);periodField.append(periodHeading,range);
  const viewActions=el('div','mon-view-actions');viewActions.append(compareButton,liveButton,refreshButton);
  controls.append(labeledField('Показатель',metric),periodField,viewActions);
  const custom=el('form','mon-custom'), from=el('input','sw-input'), to=el('input','sw-input');
  from.type=to.type='datetime-local';from.setAttribute('aria-label','Начало периода');to.setAttribute('aria-label','Конец периода');
  const applyRange=button('Показать',undefined,'sw-button-quiet');applyRange.type='submit';custom.hidden=true;
  custom.append(labeledField('С',from),labeledField('По',to),applyRange);
  const error=el('p','mon-error');error.setAttribute('role','alert');error.hidden=true;
  const figure=el('figure','mon-figure');figure.dataset.glassRole='panel';figure.dataset.glassSafeArea='off';
  const periodCaption=el('p','mon-period-caption'),readout=el('div','mon-readout'), value=el('output','mon-value','—'), pointMeaning=el('span','mon-point-meaning'),pointTime=el('span','mon-point-time'), status=el('span','mon-series-status'),currentLabel=el('span','mon-current-label','Выбранный');currentLabel.hidden=true;
  value.setAttribute('aria-live','off');readout.append(currentLabel,value,pointMeaning,pointTime,status);
  const previousReadout=el('div','mon-previous-readout'),previousLabel=el('span','mon-previous-label','Предыдущий'),previousValue=el('output','mon-previous-value'),previousMeaning=el('span','mon-point-meaning mon-previous-meaning'),previousTime=el('span','mon-point-time mon-previous-time'),comparisonInfo=el('p','mon-comparison-period');
  previousReadout.hidden=comparisonInfo.hidden=true;previousValue.setAttribute('aria-live','off');previousValue.setAttribute('aria-label','Предыдущий период');previousReadout.append(previousLabel,previousValue,previousMeaning,previousTime);
  const chart=el('div','mon-chart');chart.dataset.glassIgnore='';chart.tabIndex=0;chart.setAttribute('role','img');
  const chartHelp=el('p','mon-chart-help','← → — значения; Home / End — начало и конец. Выделите участок мышью, чтобы приблизить.');
  chartHelp.id=`monitoring-help-${view.crypto.randomUUID()}`;chart.setAttribute('aria-describedby',chartHelp.id);
  const empty=el('p','mon-empty','Пока нет наблюдений'), coverage=el('figcaption','mon-coverage');
  const resetZoom=button('Весь период','expand','sw-button-quiet mon-reset-zoom',()=>restoreZoom());resetZoom.hidden=true;
  figure.append(periodCaption,readout,previousReadout,chart,empty,resetZoom,comparisonInfo,coverage,chartHelp);
  const tableDetails=el('details','mon-data'), tableSummary=el('summary','','Значения таблицей'), table=el('table','mon-table'), tableHead=el('thead'), tableBody=el('tbody');
  const columns=el('tr');for(const label of ['Время','Значение','Наблюдения']){const cell=el('th','',label);cell.scope='col';columns.append(cell);}tableHead.append(columns);table.append(tableHead,tableBody);tableDetails.append(tableSummary,table);
  const management=el('details','mon-management'), managementSummary=el('summary','','Управление'), managementBody=el('div','mon-management-body');
  const collectionLabel=el('label','mon-collection'), collection=el('input'), collectionText=el('span','','Собирать показатели');collection.type='checkbox';collectionLabel.append(collection,collectionText);
  const manageHint=el('p','mon-muted',options.scope.kind==='platform'?'Меняется сбор показателей Сот, не работа приложений.':'Меняется сбор показателей этого приложения, не работа приложения.');
  const mutationStatus=el('p','mon-mutation-status');mutationStatus.setAttribute('role','status');
  const pendingBox=el('div','mon-pending'), pendingText=el('p'), pendingButtons=el('div','mon-pending-actions');pendingBox.hidden=true;
  const checkPending=button('Проверить результат','refresh','sw-button-quiet',()=>{void mutate(true);});
  const takeCurrent=button('Принять актуальную настройку',undefined,'sw-button-quiet',()=>{void acceptCurrent();});takeCurrent.hidden=true;
  pendingButtons.append(checkPending,takeCurrent);pendingBox.append(pendingText,pendingButtons);
  const adminForms=el('div','mon-admin-forms'), adminUnavailable=el('div','mon-admin-unavailable'), adminPending=el('div','mon-pending mon-admin-pending'), adminPendingText=el('p'), adminPendingButtons=el('div','mon-pending-actions');adminPending.hidden=true;
  const checkAction=button('Проверить результат','refresh','sw-button-quiet',()=>{void performAction(null);});
  const executeMissing=button('Отправить исходный запрос',undefined,'sw-button-quiet',()=>{void performAction(null,true);});executeMissing.hidden=true;
  const discardAction=button('Оставить актуальные настройки',undefined,'sw-button-quiet',()=>{void acceptActionCurrent();});discardAction.hidden=true;
  adminPendingButtons.append(checkAction,executeMissing,discardAction);adminPending.append(adminPendingText,adminPendingButtons);
  managementBody.append(collectionLabel,manageHint,mutationStatus,pendingBox,adminForms,adminUnavailable,adminPending);management.append(managementSummary,managementBody);management.hidden=true;
  const auditDetails=el('details','mon-audit'), auditSummary=el('summary','','История управления'), auditList=el('ol','mon-audit-list'), auditStatus=el('p','mon-muted');
  const auditMore=button('Ещё события',undefined,'sw-button-quiet',()=>{void loadAudit(true);});auditMore.hidden=true;auditDetails.hidden=true;
  auditDetails.append(auditSummary,auditStatus,auditList,auditMore);
  const boundary=el('p','mon-boundary','Показатели отражают наблюдения Сот. Ответ сервера не подтверждает успешную работу человека в приложении.');
  root.append(header,controls,custom,error,figure,tableDetails,management,auditDetails,boundary);host.append(root);
  for(const node of [metric,range,from,to]){node.dataset.glassRole='field';node.dataset.glassSafeArea='off';}
  for(const node of [liveButton,refreshButton,compareButton,applyRange,resetZoom,tableSummary,managementSummary,auditSummary,checkPending,takeCurrent,auditMore,checkAction,executeMissing,discardAction]){node.dataset.glassRole='control';node.dataset.glassSafeArea='off';}
  collectionLabel.dataset.glassRole='control';collectionLabel.dataset.glassSafeArea='off';

  let description:MonitoringDescription|null=null, series:MonitoringSeries|null=null, selectedMetric='', selectedAt:number|null=null;
  let compareEnabled=false,previousSeries:MonitoringSeries|null=null,comparison:ReturnType<typeof monitoringComparisonData>|null=null;
  let readEpoch=0, auditEpoch=0, auditCursor:number|null=null, auditLoaded=false, auditBusy=false, busy=false, live=true, zoomed=false, updatingPlot=false;
  let timer:number|null=null, plot:uPlot|null=null, plotMetric='', rejected:MonitoringConfigCommand|null=null, mutation:Promise<void>|null=null;
  interface ActionView {action:MonitoringAction;signature:string;baseRevision:number;node:HTMLElement;inputs:Map<string,HTMLInputElement|HTMLSelectElement>;dirty:boolean;stateValid:boolean;confirmation:HTMLElement;message:HTMLElement;save:HTMLButtonElement;reset:HTMLButtonElement;confirm:HTMLButtonElement;cancel:HTMLButtonElement;}
  const actionViews=new Map<string,ActionView>();let adminOutcome:'unknown'|'not-applied'|null=null;
  const statusLabels:Record<string,string>={'collection.setEnabled':'Изменён сбор показателей','monitoring.config.put':'Изменён сбор показателей','monitoring.metric.put':'Настроен показатель'};
  const stopTimer=()=>{if(timer!==null)view.clearTimeout(timer);timer=null;};
  const reportError=(reason:unknown)=>{error.textContent=monitoringErrorText(reason);error.hidden=false;};
  const pending=():MonitoringConfigCommand|null=>{try{return client.pending();}catch(reason){reportError(reason);return null;}};
  const pendingAdmin=():MonitoringAdminCommand|null=>{try{return client.pendingAction();}catch(reason){reportError(reason);return null;}};
  const selected=()=>description?.metrics.find(item=>item.id===selectedMetric)??null;
  function clearPlot(){plot?.destroy();plot=null;plotMetric='';chart.replaceChildren();chart.hidden=true;empty.hidden=false;tableBody.replaceChildren();series=null;previousSeries=null;comparison=null;selectedAt=null;value.textContent='—';periodCaption.textContent=pointMeaning.textContent=pointTime.textContent=pointTime.title='';status.textContent='';coverage.textContent='';currentLabel.hidden=previousReadout.hidden=comparisonInfo.hidden=true;previousValue.textContent=previousMeaning.textContent=previousTime.textContent=previousTime.title=comparisonInfo.textContent='';}
  function deny(){description=null;clearPlot();empty.textContent='Доступ к показателям закрыт';metric.replaceChildren();controls.hidden=custom.hidden=tableDetails.hidden=management.hidden=auditDetails.hidden=true;auditList.replaceChildren();adminForms.replaceChildren();adminUnavailable.replaceChildren();actionViews.clear();auditLoaded=false;auditEpoch++;state.textContent='Показатели недоступны';updated.textContent='';root.dataset.state='unavailable';root.setAttribute('aria-label','Показатели недоступны');}
  function retired(epoch?:number){if(!current()){if(!disposed)deny();return true;}return epoch!==undefined&&epoch!==readEpoch;}
  function syncManagement(){
    if(!description){management.hidden=auditDetails.hidden=true;return;}
    const intent=pending(),adminIntent=pendingAdmin(),allowed=!!description?.permissions.configure&&description.actions.some(action=>action.id==='collection.setEnabled'&&action.enabled);
    const adminAllowed=description.actions.some(action=>action.id!=='collection.setEnabled'&&description!.permissions[action.right]);
    management.hidden=!allowed&&!intent&&!adminAllowed&&!adminIntent;collectionLabel.hidden=!allowed;
    collection.checked=description?.config.enabled??false;collection.disabled=!allowed||busy||!!intent;
    pendingBox.hidden=!intent;checkPending.disabled=busy||!allowed;takeCurrent.disabled=busy||!allowed;
    takeCurrent.hidden=!intent||!rejected||rejected.requestId!==intent.requestId;
    pendingText.textContent=intent?`Запрос ${intent.enabled?'на включение':'на выключение'} сбора сохранён. Его результат ещё не подтверждён.`:'';
    auditDetails.hidden=!description?.permissions.admin;
    if(auditDetails.hidden){auditList.replaceChildren();auditLoaded=false;auditEpoch++;}
    syncAdminActions();
  }
  function readActionInputs(record:ActionView){
    const result:Record<string,boolean|number|string>={};
    for(const field of record.action.fields??[]){const control=record.inputs.get(field.name);if(!control)throw Error('monitoring_action_form_invalid');
      if(field.type==='boolean')result[field.name]=(control as HTMLInputElement).checked;
      else if(field.type==='integer'){const value=control.value.trim()===''?NaN:Number(control.value);if(!Number.isSafeInteger(value)||field.minimum!==undefined&&value<field.minimum||field.maximum!==undefined&&value>field.maximum)throw Object.assign(Error('monitoring_action_input_invalid'),{code:'monitoring_action_input_invalid'});result[field.name]=value;}
      else{if(!field.options?.includes(control.value))throw Object.assign(Error('monitoring_action_input_invalid'),{code:'monitoring_action_input_invalid'});result[field.name]=control.value;}
    }return result;
  }
  function resetAction(record:ActionView){
    const action=record.action;if(!Number.isSafeInteger(action.revision)||!action.current)return;
    record.stateValid=(action.fields??[]).every(field=>{const value=action.current![field.name];return field.type==='boolean'?typeof value==='boolean':field.type==='integer'?Number.isSafeInteger(value):typeof value==='string'&&field.options?.includes(value);});
    for(const field of action.fields??[]){const control=record.inputs.get(field.name),value=action.current[field.name];if(!control)continue;
      if(field.type==='boolean')(control as HTMLInputElement).checked=value===true;else{const text=value===undefined?'':String(value);if(control.value!==text)control.value=text;}}
    record.baseRevision=action.revision!;record.dirty=false;record.confirmation.hidden=true;record.message.textContent=record.stateValid?'':'Актуальные значения не подтверждены';
  }
  function createAction(action:MonitoringAction):ActionView{
    const node=el('form','mon-admin-action'),title=el('h3','',action.label),fields=el('div','mon-admin-fields'),inputs=new Map<string,HTMLInputElement|HTMLSelectElement>();
    node.dataset.actionId=action.id;
    for(const field of action.fields??[]){const input=field.type==='enum'?el('select','sw-select'):el('input','sw-input');
      input.name=field.name;if(field.type==='enum')for(const value of field.options??[]){const option=el('option','',value);option.value=value;input.append(option);}
      else{const native=input as HTMLInputElement;native.type=field.type==='boolean'?'checkbox':'number';if(field.minimum!==undefined)native.min=String(field.minimum);if(field.maximum!==undefined)native.max=String(field.maximum);native.step='1';}
      const label=labeledField(field.label,input);if(field.type==='boolean'){label.classList.add('mon-admin-boolean');label.dataset.glassRole='control';label.dataset.glassSafeArea='off';}
      else{input.dataset.glassRole='field';input.dataset.glassSafeArea='off';}inputs.set(field.name,input);fields.append(label);
    }
    const actions=el('div','mon-pending-actions'),save=button('Применить',undefined,'sw-button-quiet'),reset=button('Взять актуальные',undefined,'sw-button-quiet');save.type='submit';actions.append(save,reset);
    const message=el('p','mon-action-status');message.setAttribute('role','status');
    const confirmation=el('div','mon-action-confirm'),question=el('p','','Применить изменения?'),confirm=button('Подтвердить',undefined,'sw-button-quiet'),cancel=button('Отмена',undefined,'sw-button-primary');confirmation.hidden=true;
    confirmation.append(question,cancel,confirm);node.append(title,fields,actions,message,confirmation);
    for(const control of [save,reset,confirm,cancel]){control.dataset.glassRole=control===cancel?'accent':'control';control.dataset.glassSafeArea='off';}
    const record:ActionView={action,signature:JSON.stringify(action.fields),baseRevision:action.revision??0,node,inputs,dirty:false,stateValid:false,confirmation,message,save,reset,confirm,cancel};
    for(const input of inputs.values())input.addEventListener('input',()=>{if(!current())return;record.dirty=true;record.confirmation.hidden=true;record.message.textContent='Изменения ещё не применены';syncAdminActions();},{signal});
    node.addEventListener('submit',event=>{event.preventDefault();if(!current()||busy||pendingAdmin()||!record.stateValid||record.signature!==JSON.stringify(record.action.fields)||!record.dirty||!record.action.enabled||!description?.permissions[record.action.right])return;
      try{readActionInputs(record);record.confirmation.hidden=false;record.cancel.focus();}catch{record.message.textContent='Проверьте значения полей';}},{signal});
    reset.addEventListener('click',()=>{if(current()&&!busy&&!pendingAdmin()){resetAction(record);syncAdminActions();}},{signal});
    cancel.addEventListener('click',()=>{record.confirmation.hidden=true;record.save.focus();},{signal});
    confirm.addEventListener('click',()=>{if(!record.confirmation.hidden)void performAction(record);},{signal});resetAction(record);return record;
  }
  function syncAdminActions(){
    if(!description)return;const intent=pendingAdmin(),keep=new Set<string>();adminUnavailable.replaceChildren();
    for(const action of description.actions){if(action.id==='collection.setEnabled'||!description.permissions[action.right])continue;keep.add(action.id);
      if(!action.fields||!Number.isSafeInteger(action.revision)){const existing=actionViews.get(action.id);
        if(existing){existing.action=action;for(const control of existing.inputs.values())control.disabled=true;existing.save.disabled=existing.reset.disabled=existing.confirm.disabled=true;existing.confirmation.hidden=true;existing.message.textContent='Сейчас недоступно; ваши значения пока остаются здесь.';}
        else adminUnavailable.append(el('p','mon-muted',`${action.label} — сейчас недоступно`));continue;}
      let record=actionViews.get(action.id);const signature=JSON.stringify(action.fields);
      if(record&&record.signature!==signature&&!record.dirty&&!intent){record.node.remove();actionViews.delete(action.id);record=undefined;}
      if(!record){record=createAction(action);actionViews.set(action.id,record);adminForms.append(record.node);}else{record.action=action;if(!record.dirty&&!intent)resetAction(record);}
      const allowed=action.enabled&&record.stateValid&&record.signature===signature&&!busy&&!intent;
      for(const control of record.inputs.values())control.disabled=!allowed;
      record.save.disabled=!allowed||!record.dirty;record.reset.disabled=!allowed||!record.dirty;record.confirm.disabled=!allowed;record.cancel.disabled=busy;
      if(!action.enabled)record.message.textContent='Сейчас недоступно';else if(record.signature!==signature)record.message.textContent='Настройки формы изменились. Откройте панель заново; ваши значения пока остаются здесь.';
      else if(record.dirty&&record.baseRevision!==action.revision)record.message.textContent='Настройки изменились в другом окне. Ваши значения сохранены здесь.';
    }
    for(const[id,record]of actionViews)if(!keep.has(id)){record.node.remove();actionViews.delete(id);}
    adminPending.hidden=!intent;const action=intent?description.actions.find(item=>item.id===intent.actionId):null,allowed=!!action?.enabled&&description.permissions[action.right];
    checkAction.disabled=executeMissing.disabled=discardAction.disabled=busy||!allowed;
    executeMissing.hidden=discardAction.hidden=!intent||adminOutcome!=='not-applied';
    adminPendingText.textContent=!intent?'':adminOutcome==='not-applied'?'Изменение не применено. Можно отправить исходный запрос или оставить актуальные настройки.':'Результат изменения пока не подтверждён. Повторная проверка не выполняет действие заново.';
  }
  function updateDescription(next:MonitoringDescription){
    description=next;if(!next.permissions.read){deny();return;}
    root.setAttribute('aria-label',options.title??(options.scope.kind==='platform'?'Состояние Сот':'Состояние приложения'));
    controls.hidden=tableDetails.hidden=false;custom.hidden=range.value!=='custom';
    const collector=next.collector,collectorHint=!collector?'':!collector.running?' · сейчас не выполняется':collector.lastFailureAt!==null&&(collector.lastSuccessAt===null||collector.lastFailureAt>collector.lastSuccessAt)?' · последнее обновление не удалось':collector.lastSuccessAt===null?' · ожидаем первое наблюдение':'';
    state.textContent=!next.storage.installed?'Мониторинг не подключён':next.config.enabled?'Сбор включён'+collectorHint:'Сбор выключен';
    updated.textContent=`Проверено ${monitoringTime(next.generatedAt)}`;root.dataset.state=next.config.enabled?'enabled':'disabled';
    const ids=next.metrics.map(item=>item.id).join('\n');
    if(metric.dataset.ids!==ids){const previous=selectedMetric;metric.replaceChildren(...next.metrics.map(item=>{const option=el('option','',item.label);option.value=item.id;return option;}));metric.dataset.ids=ids;
      selectedMetric=next.metrics.some(item=>item.id===previous)?previous:next.metrics[0]?.id??'';metric.value=selectedMetric;if(previous!==selectedMetric)clearPlot();}
    metric.disabled=compareButton.disabled=!next.metrics.length;syncManagement();
  }
  function pointReadout(at:number|null,keyboard=false){
    if(!series)return;let selectedTime=at;
    if(selectedTime===null){selectedTime=[...series.points].reverse().find(item=>item.value!==null)?.at??null;
      if(selectedTime===null&&previousSeries&&comparison){const latest=[...previousSeries.points].reverse().find(item=>item.value!==null);if(latest)selectedTime=latest.at+comparison.shift;}
      selectedTime??=series.points.at(-1)?.at??null;}
    const point=selectedTime===null?undefined:series.points.find(item=>item.at===selectedTime)??{at:selectedTime,value:null,samples:0};
    value.setAttribute('aria-live',keyboard?'polite':'off');value.textContent=point?monitoringValue(point.value,series.unit):'Нет данных';
    pointMeaning.textContent=point?monitoringPointMeaning(series,point.at):'';pointTime.textContent=point?monitoringAxisTime(point.at,false):'Нет наблюдений';pointTime.title=point?monitoringTime(point.at):'';
    if(previousSeries&&comparison&&point){const priorAt=point.at-comparison.shift,prior=previousSeries.points.find(item=>item.at===priorAt);previousValue.setAttribute('aria-live',keyboard?'polite':'off');previousValue.textContent=monitoringValue(prior?.value??null,previousSeries.unit);previousMeaning.textContent=monitoringPointMeaning(previousSeries,priorAt);previousTime.textContent=monitoringAxisTime(priorAt,false);previousTime.title=monitoringTime(priorAt);}
  }
  function palette(){const style=view.getComputedStyle(root),forced=view.matchMedia('(forced-colors: active)').matches;return{ink:style.color,muted:forced?style.color:style.getPropertyValue('--sw-muted').trim()||style.color,
    accent:forced?style.color:style.getPropertyValue('--sw-focus').trim()||style.color,grid:forced?style.color:style.getPropertyValue('--sw-line').trim()||style.color};}
  function chartSize(){return{width:Math.max(160,Math.round(chart.clientWidth||root.clientWidth||320)),height:Math.max(160,Math.round(chart.clientHeight||250))};}
  function restoreZoom(){if(!plot||!series)return;updatingPlot=true;plot.setScale('x',{min:series.coverage.from/1000,max:series.coverage.to/1000});updatingPlot=false;zoomed=false;resetZoom.hidden=true;}
  function renderSeries(next:MonitoringSeries,before:MonitoringSeries|null=null){
    const aligned=before?monitoringComparisonData(next,before):null,data=aligned?.data??monitoringPlotData(next.points);
    series=next;previousSeries=before;comparison=aligned;const chosen=selected();chart.setAttribute('aria-label',`${chosen?.label??next.metricId}. ${monitoringStatus(next.status)}.${before?' Сравнение с предыдущим равным периодом.':''} Значения доступны таблицей.`);
    value.setAttribute('aria-label',before?'Выбранный период':chosen?.label??next.metricId);
    periodCaption.textContent=monitoringPeriod(next.coverage);currentLabel.hidden=previousReadout.hidden=comparisonInfo.hidden=!before;currentLabel.title=before?monitoringPeriod(next.coverage):'';
    comparisonInfo.textContent=before?`Предыдущий: ${monitoringPeriod(before.coverage)}. ${monitoringHistoricalStatus(before,description?.generatedAt??0)}.`:'';
    status.textContent=monitoringStatus(next.status);status.dataset.state=next.status;
    const unsupported=next.source==='host'&&description?.collector&&!description.collector.supportedMetricIds.includes(next.metricId);
    coverage.textContent=`Данные есть в ${next.coverage.observedPoints} из ${next.coverage.totalPoints} интервалов. Пропуск не означает ноль. ${next.source==='host'?'Измерено Сотами.':'Показатель приложения.'}${unsupported?' Сейчас этот показатель не собирается.':''}`;
    const measured=next.points.some(point=>point.value!==null)||!!before?.points.some(point=>point.value!==null);empty.hidden=measured;chart.hidden=!measured;
    empty.textContent=monitoringStatus(next.status);
    if(!measured){plot?.destroy();plot=null;plotMetric='';chart.replaceChildren();}
    else{
      updatingPlot=true;
      if(plot&&plotMetric===next.metricId){plot.setData(data,!zoomed);if(!zoomed)plot.setScale('x',{min:next.coverage.from/1000,max:next.coverage.to/1000});}
      else{plot?.destroy();chart.replaceChildren();plotMetric=next.metricId;
        plot=new uPlot({...chartSize(),legend:{show:false},cursor:{drag:{x:true,y:false,setScale:true}},scales:{x:{time:true}},
          axes:[{stroke:()=>palette().muted,font:'12px system-ui',grid:{stroke:()=>palette().grid},ticks:{stroke:()=>palette().grid},values:(_self,ticks)=>ticks.map((tick,index)=>monitoringAxisTime(tick*1000,index===0||!!series&&series.coverage.to-series.coverage.from>86400000))},
            {stroke:()=>palette().muted,font:'12px system-ui',size:64,grid:{stroke:()=>palette().grid},ticks:{stroke:()=>palette().grid},values:(_self,ticks)=>ticks.map(tick=>monitoringValue(tick,next.unit))}],
          series:[{}, {label:chosen?.label??next.metricId,stroke:()=>palette().accent,width:2,spanGaps:false,points:{show:next.points.filter(point=>point.value!==null).length===1}},...(before?[{label:'Предыдущий период',stroke:()=>palette().ink,width:2,dash:[6,4],spanGaps:false,points:{show:before.points.filter(point=>point.value!==null).length===1}}]:[])],
          hooks:{setCursor:[chart=>{if(updatingPlot||!series)return;const index=chart.cursor.idx,at=index===null||index===undefined?undefined:chart.data[0][index];selectedAt=typeof at==='number'?Math.round(at*1000):null;pointReadout(selectedAt);}],
            setSelect:[chart=>{if(updatingPlot||chart.select.width<=0)return;readEpoch++;root.removeAttribute('aria-busy');zoomed=true;live=false;liveButton.setAttribute('aria-pressed','false');resetZoom.hidden=false;stopTimer();}]}},data,chart);
        plot.setScale('x',{min:next.coverage.from/1000,max:next.coverage.to/1000});
      }
      updatingPlot=false;
    }
    pointReadout(selectedAt);
    columns.replaceChildren(...(before?['Время','Выбранный','Наблюдения','Время ранее','Предыдущий','Наблюдения ранее']:['Время','Значение','Наблюдения']).map(label=>{const th=el('th','',label);th.scope='col';return th;}));
    tableBody.replaceChildren(...(aligned?.times??next.points.map(point=>point.at)).map(at=>{const point=next.points.find(item=>item.at===at),row=el('tr');row.append(el('td','',monitoringTime(at)),el('td','',monitoringValue(point?.value??null,next.unit)),el('td','',String(point?.samples??0)));
      if(before&&aligned){const priorAt=at-aligned.shift,prior=before.points.find(item=>item.at===priorAt);row.append(el('td','',monitoringTime(priorAt)),el('td','',monitoringValue(prior?.value??null,before.unit)),el('td','',String(prior?.samples??0)));}return row;}));
  }
  function schedule(){stopTimer();if(current()&&visible&&live&&host.ownerDocument.visibilityState!=='hidden')timer=view.setTimeout(()=>{timer=null;void refresh();},5000);}
  async function read(){
    if(retired()||!visible)return;
    const requested={range:range.value,from:from.value,to:to.value,compare:compareEnabled};
    if(requested.range==='custom')try{monitoringWindow('custom',Date.now(),requested.from,requested.to);}catch(reason){reportError(reason);return;}
    const epoch=++readEpoch;stopTimer();error.hidden=true;root.setAttribute('aria-busy','true');
    try{
      const next=await client.describe();if(retired(epoch))return;updateDescription(next);if(!description)return;
      if(!selectedMetric){clearPlot();empty.textContent='Пока нет доступных показателей';return;}
      const bounds=monitoringWindow(requested.range,next.generatedAt,requested.from,requested.to),id=selectedMetric;
      const response=await client.query({metricIds:[id],...bounds,maxPoints:120});if(retired(epoch)||id!==selectedMetric)return;
      const result=response.series.find(item=>item.metricId===id);if(!result){clearPlot();throw Error('monitoring_series_missing');}
      let previous:MonitoringSeries|null=null,lastConfig=response.config;
      if(requested.compare){const previousBounds=monitoringPreviousWindow(result.coverage);const priorResponse=await client.query({metricIds:[id],...previousBounds,maxPoints:120});if(retired(epoch)||id!==selectedMetric)return;
        previous=priorResponse.series.find(item=>item.metricId===id)??null;if(!previous)throw Object.assign(Error('monitoring_comparison_invalid'),{code:'monitoring_comparison_invalid'});lastConfig=priorResponse.config;}
      updateDescription({...description,config:lastConfig});renderSeries(result,previous);
      if(auditDetails.open&&!auditLoaded)void loadAudit();
    }catch(reason){if(retired(epoch))return;if(monitoringAccessLost(reason))deny();else if((reason as {code?:string})?.code==='monitoring_comparison_invalid'){clearPlot();empty.textContent='Сравнение недоступно';}else if(series){status.textContent='Не удалось обновить';status.dataset.state='stale';}else{state.textContent='Не удалось получить показатели';empty.textContent='Источник сейчас недоступен';}reportError(reason);}
    finally{if(current()&&epoch===readEpoch){root.removeAttribute('aria-busy');schedule();}}
  }
  function refresh(){return read();}
  async function mutate(retry=false){
    if(!current()||busy||!description?.permissions.configure)return;
    const action=description.actions.find(item=>item.id==='collection.setEnabled'&&item.enabled);if(!action)return;
    const command=retry?pending():{requestId:view.crypto.randomUUID(),expectedRevision:description.config.revision,enabled:collection.checked};
    if(!command)return;busy=true;error.hidden=true;mutationStatus.textContent='Проверяем изменение…';syncManagement();
    const task=(async()=>{try{
      const result=retry?await client.retryPending():await client.setEnabled(command);if(retired())return;
      rejected=null;if(description)description={...description,config:result.config};mutationStatus.textContent='Настройка подтверждена';auditLoaded=false;syncManagement();await refresh();
    }catch(reason){if(retired())return;if(String((reason as {code?:string})?.code)==='monitoring_revision_conflict')rejected={...command};
      if(monitoringAccessLost(reason))deny();mutationStatus.textContent=monitoringErrorText(reason);reportError(reason);syncManagement();
      if(rejected)await refresh();
    }finally{busy=false;if(current())syncManagement();}})();mutation=task;await task;if(mutation===task)mutation=null;
  }
  async function acceptCurrent(){
    const intent=pending();if(!current()||busy||!intent||!rejected||intent.requestId!==rejected.requestId)return;
    busy=true;syncManagement();try{const next=await client.describe();if(retired())return;
      if(!next.permissions.configure)throw Object.assign(Error('monitoring_forbidden'),{code:'monitoring_forbidden'});
      const fresh=pending();if(!fresh||fresh.requestId!==intent.requestId)return;
      client.discardPending(intent.requestId);rejected=null;updateDescription(next);mutationStatus.textContent='Показана актуальная настройка. Новое изменение не отправлялось.';error.hidden=true;
    }catch(reason){if(current()){if(monitoringAccessLost(reason))deny();reportError(reason);}}
    finally{busy=false;if(current())syncManagement();}
  }
  async function performAction(record:ActionView|null,executeIfMissing=false){
    if(retired()||busy||!description)return;
    if(executeIfMissing&&adminOutcome!=='not-applied')return;
    let command:MonitoringAdminCommand|null;
    try{command=record?{actionId:record.action.id,requestId:view.crypto.randomUUID(),expectedRevision:record.baseRevision,input:readActionInputs(record)}:pendingAdmin();}
    catch{if(record)record.message.textContent='Проверьте значения полей';return;}
    if(!command)return;const action=description.actions.find(item=>item.id===command!.actionId);
    if(!action?.enabled||!description.permissions[action.right]||record&&(!record.stateValid||record.signature!==JSON.stringify(action.fields)||!record.dirty||record.confirmation.hidden||actionViews.get(action.id)!==record))return;
    busy=true;adminOutcome=null;mutationStatus.textContent='Проверяем изменение…';syncManagement();
    const task=(async()=>{try{
      const effect=record?client.invokeAction(command):client.retryAction({executeIfMissing});
      if(record)try{
        const durable=client.pendingAction();
        if(durable?.requestId===command.requestId&&durable.actionId===command.actionId&&durable.expectedRevision===command.expectedRevision&&JSON.stringify(durable.input)===JSON.stringify(command.input))record.dirty=false;
      }catch{/* A failed journal read never transfers volatile form ownership. */}
      const result=await effect;if(retired())return;
      if(result.state==='committed'){
        const edited=actionViews.get(command.actionId);if(edited){edited.dirty=false;edited.confirmation.hidden=true;}adminOutcome=null;mutationStatus.textContent='Изменение подтверждено';auditLoaded=false;await refresh();
      }else{adminOutcome=result.state;mutationStatus.textContent=result.state==='not-applied'?'Изменение не применено':'Результат пока неизвестен';if(record)record.confirmation.hidden=true;}
    }catch(reason){if(retired())return;adminOutcome='unknown';if(record)record.confirmation.hidden=true;if(monitoringAccessLost(reason))deny();reportError(reason);mutationStatus.textContent=monitoringErrorText(reason);}
    finally{busy=false;if(current())syncManagement();}})();mutation=task;await task;if(mutation===task)mutation=null;
  }
  async function acceptActionCurrent(){
    const command=pendingAdmin();if(retired()||busy||!command||adminOutcome!=='not-applied')return;
    busy=true;syncManagement();try{const next=await client.describe();if(retired())return;
      const action=next.actions.find(item=>item.id===command.actionId),fresh=pendingAdmin();
      if(!action||!next.permissions[action.right]||!fresh||fresh.requestId!==command.requestId)return;
      client.discardAction(command.requestId);adminOutcome=null;const record=actionViews.get(command.actionId);if(record)record.dirty=false;
      updateDescription(next);mutationStatus.textContent='Показаны актуальные настройки. Новое действие не отправлялось.';error.hidden=true;
    }catch(reason){if(!retired()){if(monitoringAccessLost(reason))deny();reportError(reason);}}
    finally{busy=false;if(current())syncManagement();}
  }
  async function loadAudit(append=false){
    if(!current()||!description?.permissions.admin||auditBusy||append&&auditCursor===null)return;
    const epoch=++auditEpoch;auditBusy=true;auditMore.disabled=true;auditStatus.textContent='Обновляем историю…';
    try{const result=await client.audit({...(append&&auditCursor!==null?{cursor:auditCursor}:{}),limit:30});if(!current()||epoch!==auditEpoch||!description?.permissions.admin)return;
      if(!append)auditList.replaceChildren();for(const entry of result.entries){const row=el('li');row.append(el('span','',statusLabels[entry.operation]??description.actions.find(action=>action.id===entry.operation)?.label??'Изменение управления'),el('time','',monitoringTime(entry.at)));auditList.append(row);}
      auditLoaded=true;auditCursor=result.nextCursor;auditMore.hidden=auditCursor===null;auditStatus.textContent=auditList.children.length?'':'Событий пока нет';
    }catch(reason){if(current()&&epoch===auditEpoch){auditStatus.textContent=monitoringErrorText(reason);if(monitoringAccessLost(reason)){auditList.replaceChildren();auditMore.hidden=true;}}}
    finally{auditBusy=false;if(current())auditMore.disabled=false;}
  }
  metric.addEventListener('change',()=>{if(!current())return;selectedMetric=metric.value;selectedAt=null;zoomed=false;resetZoom.hidden=true;clearPlot();void refresh();},{signal});
  range.addEventListener('change',()=>{if(!current())return;readEpoch++;root.removeAttribute('aria-busy');zoomed=false;resetZoom.hidden=true;custom.hidden=range.value!=='custom';clearPlot();
    if(range.value==='custom'){live=false;liveButton.setAttribute('aria-pressed','false');stopTimer();empty.textContent='Выберите период и нажмите «Показать»';from.focus();}else void refresh();},{signal});
  custom.addEventListener('submit',event=>{event.preventDefault();if(!current())return;try{monitoringWindow('custom',Date.now(),from.value,to.value);}catch(reason){reportError(reason);return;}
    live=false;liveButton.setAttribute('aria-pressed','false');zoomed=false;resetZoom.hidden=true;clearPlot();void refresh();},{signal});
  collection.addEventListener('change',()=>{void mutate();},{signal});
  auditDetails.addEventListener('toggle',()=>{if(auditDetails.open&&!auditLoaded)void loadAudit();},{signal});
  chart.addEventListener('keydown',event=>{
    if(event.isComposing||!current()||!series)return;
    if(event.key==='Escape'&&zoomed){event.preventDefault();event.stopPropagation();restoreZoom();return;}
    const times=comparison?.times??series.points.map(point=>point.at);if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key)||!times.length)return;
    event.preventDefault();const found=selectedAt===null?times.length-1:Math.max(0,times.indexOf(selectedAt));
    const index=event.key==='Home'?0:event.key==='End'?times.length-1:Math.min(times.length-1,Math.max(0,found+(event.key==='ArrowLeft'?-1:1)));
    selectedAt=times[index]!;pointReadout(selectedAt,true);if(plot){updatingPlot=true;plot.setCursor({left:plot.valToPos(selectedAt/1000,'x'),top:0});updatingPlot=false;}
  },{signal});
  const resize=new ResizeObserver(()=>{if(current()&&plot){updatingPlot=true;plot.setSize(chartSize());updatingPlot=false;}});resize.observe(chart);
  const theme=()=>{if(current()&&plot)plot.redraw(false,true);};
  const media=[view.matchMedia('(prefers-color-scheme: dark)'),view.matchMedia('(forced-colors: active)')];for(const item of media)item.addEventListener('change',theme);
  view.addEventListener('storage',theme,{signal});
  host.ownerDocument.addEventListener('visibilitychange',()=>{if(host.ownerDocument.visibilityState==='hidden')stopTimer();else if(current()&&live)void refresh();},{signal});
  const ready=refresh();
  return{ready,refresh,setVisible(next){if(disposed||visible===next)return;visible=next;readEpoch++;root.removeAttribute('aria-busy');stopTimer();if(visible&&current()){if(plot)plot.setSize(chartSize());if(live)void refresh();}},
    hasUnsavedChanges:()=>busy||[...actionViews.values()].some(record=>record.dirty),
    hasUnconfirmedWork:()=>{try{return!!client.pending()||!!client.pendingAction();}catch(reason){reportError(reason);return true;}},
    async flush({allowDurablePending=false}={}){await mutation;
    // Read the actual SDK journal strictly: a storage/validation failure is not
    // permission to discard unknown work. Only explicit host confirmation may
    // leave a valid durable command; volatile form input always blocks.
    const saved=client.pending(),savedAction=client.pendingAction();
    if(busy||[...actionViews.values()].some(record=>record.dirty)||allowDurablePending!==true&&(saved||savedAction))throw Object.assign(Error('monitoring_pending_unconfirmed'),{code:'monitoring_pending_unconfirmed'});},
    dispose(){if(disposed)return;disposed=true;readEpoch++;auditEpoch++;stopTimer();lifetime.abort();resize.disconnect();for(const item of media)item.removeEventListener('change',theme);plot?.destroy();plot=null;description=null;series=null;root.replaceChildren();root.remove();}};
}
