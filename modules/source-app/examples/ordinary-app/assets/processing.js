import {createSourceFeedbackProcessingClient,createSourceFeedbackClient} from './source-sdk.js';

export function mountProcessingUi({isCurrent=()=>true}={}){
  const api=createSourceFeedbackProcessingClient(),feedback=createSourceFeedbackClient(),byId=id=>document.getElementById(id),state=byId('processing-state');
  let disposed=false,generation=0,pending=null;const active=new Set();
  const live=gen=>!disposed&&isCurrent()===true&&gen===generation;
  const purposeName=purpose=>({ocr:'распознавание изображения',asr:'расшифровку аудио',triage:'подсказку по приоритету'})[purpose];
  const consentName=purpose=>({ocr:'локальное распознавание изображения',asr:'локальную расшифровку аудио',triage:'локальную подсказку по приоритету'})[purpose];
  function node(tag,text){const element=document.createElement(tag);element.textContent=text;return element;}
  async function action(button,input,kind){
    const gen=generation,controller=new AbortController();active.add(controller);button.disabled=true;
    try{
      if(!pending)pending={intent:await api.intent(input),kind,button};
      const current=pending;let result;
      if(current.recover){result=await api.receipt(current.intent,controller.signal);
        if(result.outcome!=='committed'){if(live(gen)){state.textContent='Результат пока не подтверждён. Разрешение или обработка повторно не запускаются.';button.disabled=false;button.textContent='Проверить результат';}return;}}
      else result=await api.apply(current.intent,controller.signal);
      if(!live(gen)||pending!==current)return;
      pending=null;state.textContent=kind==='consent'?'Разрешение автора сохранено для выбранных материалов и цели.':
        'Разрешение владельца сохранено. Обработчик пока недоступен; обработка не запускалась.';
      button.textContent='Сохранено';button.disabled=true;
    }catch(error){if(!live(gen))return;
      if(error.code==='ordinary_reporter_consent_required'){pending=null;state.textContent='Сначала автор материалов должен разрешить выбранную обработку.';button.disabled=false;}
      else if(error.status===409){pending=null;state.textContent='Обращение или материалы изменились. Откройте список заново и проверьте новую цель обработки.';}
      else if([401,403].includes(error.status)){pending=null;state.textContent='Доступ изменился. Обращение сохранено; войдите заново для проверки.';}
      else{if(pending)pending.recover=true;state.textContent='Результат пока не подтверждён.';button.textContent='Проверить результат';button.disabled=false;}}
    finally{active.delete(controller);}
  }
  async function open(){
    const gen=++generation;for(const controller of active)controller.abort();active.clear();byId('processing-panel').hidden=false;
    if(pending){state.textContent='Сначала проверьте результат предыдущего разрешения.';return;}
    const controller=new AbortController();active.add(controller);byId('processing-open').disabled=true;
    try{
      const context=await api.context(controller.signal),list=await feedback.list({limit:20},controller.signal);
      if(!live(gen))return;
      if(context.schema!=='soty.feedback.processing-context.v1'||context.localOnly!==true||context.requiresReporterConsent!==true)throw new Error('invalid');
      byId('processing-tickets').replaceChildren();
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
    }catch(error){if(live(gen))state.textContent=[401,403].includes(error.status)?'Доступ изменился. Откройте приложение заново.':'Обработка пока недоступна. Исходные обращения сохранены.';}
    finally{active.delete(controller);if(live(gen))byId('processing-open').disabled=false;}
  }
  byId('processing-open').addEventListener('click',open);
  addEventListener('pagehide',()=>{disposed=true;generation++;for(const controller of active)controller.abort();active.clear();pending=null;},{once:true});
}
