import {createSourceFeedbackProcessingClient,createSourceFeedbackClient} from './source-sdk.js';

export function mountProcessingUi({isCurrent=()=>true}={}){
  const api=createSourceFeedbackProcessingClient(),feedback=createSourceFeedbackClient(),byId=id=>document.getElementById(id),state=byId('processing-state');
  let disposed=false,generation=0,pending=null,pendingKey=null,actionBusy=false;const active=new Set();
  const live=gen=>!disposed&&isCurrent()===true&&gen===generation;
  const purposeName=purpose=>({ocr:'распознавание изображения',asr:'расшифровку аудио',triage:'подсказку по приоритету'})[purpose];
  const consentName=purpose=>({ocr:'локальное распознавание изображения',asr:'локальную расшифровку аудио',triage:'локальную подсказку по приоритету'})[purpose];
  function node(tag,text){const element=document.createElement(tag);element.textContent=text;return element;}
  const recover=node('button','Проверить результат');recover.type='button';recover.hidden=true;state.after(recover);
  function remember(intent,kind){
    if(!pendingKey)throw Error('pending_storage_unavailable');
    try{sessionStorage.setItem(pendingKey,JSON.stringify({requestId:intent.args.requestId,inputDigest:intent.inputDigest,kind}));}
    catch{throw Error('pending_storage_unavailable');}
  }
  function restore(key){
    let raw;try{raw=sessionStorage.getItem(key);}catch{throw Error('pending_storage_unavailable');}
    if(raw===null)return null;let value;try{value=JSON.parse(raw);}catch{throw Error('pending_storage_invalid');}
    if(!value||Object.keys(value).sort().join(',')!=='inputDigest,kind,requestId'||typeof value.requestId!=='string'
      ||!/^processing-[A-Za-z0-9-]{16,100}$/u.test(value.requestId)||typeof value.inputDigest!=='string'
      ||!/^[a-f0-9]{64}$/u.test(value.inputDigest)||!['consent','grant'].includes(value.kind))throw Error('pending_storage_invalid');
    return{intent:{args:{requestId:value.requestId},inputDigest:value.inputDigest},kind:value.kind,recover:true};
  }
  function unknown(){state.textContent='Результат пока не подтверждён. Разрешение повторно не отправлено.';recover.hidden=false;recover.disabled=false;}
  async function action(button,input,kind){
    if(actionBusy)return;actionBusy=true;const gen=generation,controller=new AbortController();active.add(controller);button.disabled=true;
    try{
      const identity=await feedback.context(controller.signal);
      if(!live(gen)||'processing-pending:'+identity.bindingDigest!==pendingKey)throw Error('processing_context_changed');
      if(!pending){const intent=await api.intent(input);remember(intent,kind);pending={intent,kind,button,recover:false};}
      const current=pending;let result;
      if(current.recover)result=await api.receipt(current.intent,controller.signal);
      else result=await api.apply(current.intent,controller.signal);
      if(!live(gen)||pending!==current)return;
      if(result?.outcome!=='committed'||result.requestId!==current.intent.args.requestId||result.inputDigest!==current.intent.inputDigest)throw Error('receipt_unknown');
      try{sessionStorage.removeItem(pendingKey);}catch{throw Error('pending_storage_unavailable');}
      pending=null;recover.hidden=true;state.textContent=current.kind==='consent'?'Разрешение автора сохранено для выбранных материалов и цели.':
        'Разрешение владельца сохранено. Обработчик пока недоступен; обработка не запускалась.';
      if(button!==recover){button.textContent='Сохранено';button.disabled=true;}
      else{await open();if(!pending&&live(generation))state.textContent='Предыдущее разрешение подтверждено.';}
    }catch(error){if(!live(gen))return;
      if(pending){pending.recover=true;unknown();}
      else{state.textContent=error?.message==='pending_storage_unavailable'?'Не удалось сохранить запрос на устройстве. Разрешение не отправлено.':
        error?.message==='processing_context_changed'?'Профиль изменился. Откройте обработку заново.':'Не удалось подготовить запрос. Повторите попытку.';button.disabled=false;}}
    finally{active.delete(controller);actionBusy=false;}
  }
  recover.addEventListener('click',()=>{if(pending)void action(recover,null,pending.kind);});
  async function open(){
    const gen=++generation;for(const controller of active)controller.abort();active.clear();byId('processing-panel').hidden=false;
    const controller=new AbortController();active.add(controller);byId('processing-open').disabled=true;
    try{
      const context=await api.context(controller.signal),identity=await feedback.context(controller.signal);
      if(!live(gen))return;
      if(context.schema!=='soty.feedback.processing-context.v1'||context.localOnly!==true||context.requiresReporterConsent!==true
        ||identity.schema!=='soty.source-feedback.context.v1'||!/^[a-f0-9]{64}$/u.test(identity.bindingDigest))throw new Error('invalid');
      byId('processing-tickets').replaceChildren();
      const key='processing-pending:'+identity.bindingDigest;
      if(key!==pendingKey){pendingKey=key;pending=null;pending=restore(key);}else if(!pending)pending=restore(key);
      if(pending){unknown();return;}
      recover.hidden=true;const list=await feedback.list({limit:20},controller.signal);if(!live(gen))return;
      for(const ticket of list.tickets){
        const info=await api.ticket(ticket.id,controller.signal);if(!live(gen))return;
        const row=node('li',''),label=node('p',ticket.body.slice(0,160));row.append(label);
        const purpose=context.purposes.find(value=>value==='ocr'&&info.hasImage||value==='asr'&&info.hasAudio||value==='triage');
        if(!purpose){row.append(node('p','У обращения нет материалов для доступного обработчика.'));byId('processing-tickets').append(row);continue;}
        const base={ticketId:info.ticketId,ticketRevision:info.ticketRevision,attachmentDigest:info.attachmentDigest,purpose,policyRef:context.policyRef};
        if(info.canConsent){
          const checkbox=document.createElement('input');checkbox.type='checkbox';const consentLabel=node('label','Разрешаю '+consentName(purpose)+' выбранных материалов этого обращения.');consentLabel.prepend(checkbox);row.append(consentLabel);
          const consent=node('button','Разрешить '+purposeName(purpose));consent.type='button';consent.disabled=true;checkbox.addEventListener('change',()=>{consent.disabled=!checkbox.checked||pending!==null;});
          consent.addEventListener('click',()=>{if(!checkbox.checked||pending&&pending.button!==consent)return;return action(consent,{operation:'feedback.processing.consent',...base,expiresInSeconds:120},'consent');});row.append(consent);
        }
        if(info.canGrant){
          const engine=context.engines.find(value=>value.purposes.includes(purpose));
          row.append(node('p',engine?.synthetic?'Тестовый обработчик; качество распознавания не заявлено.':'Выбран локальный обработчик, одобренный владельцем.'));
          const process=node('button','Обработать обращение');process.type='button';process.disabled=true;row.append(process);
          row.append(node('p','Обработчик пока не подключён. Можно сохранить разрешение владельца; запуск недоступен.'));
          const grant=node('button','Сохранить разрешение владельца');grant.type='button';grant.disabled=!engine;
          grant.addEventListener('click',()=>{if(!engine||pending&&pending.button!==grant)return;return action(grant,{operation:'feedback.job.grant',...base,engineRef:engine.ref,budget:context.maxBudget,expiresInSeconds:120},'grant');});row.append(grant);
        }
        byId('processing-tickets').append(row);
      }
      if(live(gen))state.textContent=list.tickets.length?'Обработка требует отдельного разрешения автора материалов.':'Пока нет обращений.';
    }catch(error){if(live(gen))state.textContent=['pending_storage_invalid','pending_storage_unavailable'].includes(error?.message)?
      'Не удалось проверить предыдущий запрос на устройстве. Новое разрешение не отправлено.':
      [401,403].includes(error.status)?'Доступ изменился. Откройте приложение заново.':'Обработка пока недоступна. Исходные обращения сохранены.';}
    finally{active.delete(controller);if(live(gen))byId('processing-open').disabled=false;}
  }
  byId('processing-open').addEventListener('click',open);
  addEventListener('pagehide',()=>{disposed=true;generation++;for(const controller of active)controller.abort();active.clear();pending=null;},{once:true});
}
