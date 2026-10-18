/** Local presentation only. Existing stage buttons retain all launch and
 * authority checks. Neither the iframe nor its sandbox/navigation is changed. */
export function createWidgetRuntimeControls(options) {
  const {view,screen,message,runtime,runtimeActions,current,capture,matchesCapture,canAct,
    createDialog,iconButton,button,el,isFocusTarget,retry,currentEntry,external,dismiss,onModalChange=()=>{}}=options;
  const main=options.presentation==='main';
  let disposed=false,owner=main?options.returnTarget??null:null,header=null,active=null;
  const fallback=el('div',main?'sa-stage-runtime-notice':'sa-widget-runtime-launcher');
  const control=iconButton(main?'Подробности и действия приложения':'Действия окна приложения',main?'info':'more',open);
  control.classList.add(main?'sa-stage-runtime-action':'sa-widget-runtime-action');control.dataset.stageControl=main?'runtime-details':'widget-runtime';
  const mark=el('b','sa-widget-runtime-notice-mark');mark.setAttribute('aria-hidden','true');if(!main)control.append(mark);
  if(!message.id)message.id=`${main?'stage':'widget'}-runtime-message-${view.crypto.randomUUID()}`;
  message.after(fallback);if(main)fallback.append(message,control);else fallback.append(control);
  screen.classList.add(main?'has-stage-runtime-controls':'has-widget-runtime-controls');
  if(main){control.setAttribute('aria-haspopup','dialog');control.setAttribute('aria-expanded','false');}
  const live=()=>!disposed&&current()&&(!owner||owner.isCurrent());
  const returnTarget=()=>{
    const target=main&&isFocusTarget(control)?control:owner?.resolve()??control;
    return isFocusTarget(target)?target:isFocusTarget(control)?control:null;
  };
  const valid=record=>live()&&active===record&&matchesCapture(record.capture);
  function close(restoreFocus=false){
    const record=active;if(!record)return;
    if(!live())control.disabled=true;
    // createDialog owns synchronous cleanup, native close, Escape and focus.
    record.dialog.close({restoreFocus});
  }
  function sync(){
    if(disposed)return;
    const notice=!message.hidden&&!!message.textContent;
    if(main){fallback.hidden=!notice;message.title=message.textContent;}
    control.dataset.notice=notice?'visible':'none';
    mark.hidden=!notice;mark.textContent=runtime.getAttribute('aria-busy')==='true'?'…':'!';
    control.title=main?'Подробности и действия приложения':notice?'Сообщение и действия окна приложения':'Действия окна приложения';
    if(notice)control.setAttribute('aria-describedby',message.id);else control.removeAttribute('aria-describedby');
    const record=active;if(!record)return;
    if(!valid(record)){close(false);return;}
    record.text.textContent=message.textContent||'Если окно пустое, обновите его или откройте в новой вкладке.';
    record.retry.hidden=retry.hidden;record.retry.disabled=!canAct()||retry.disabled;
    record.currentEntry.hidden=currentEntry.hidden;record.currentEntry.disabled=!canAct()||currentEntry.disabled;
    record.external.disabled=!canAct()||external.disabled;
    record.dismiss.hidden=dismiss.hidden;record.dismiss.disabled=dismiss.disabled;
  }
  function open(){
    if(!live()||!control.isConnected)return;
    if(active){if(valid(active))active.dialog.element.focus();else close(false);return;}
    const record={capture:capture(),allowReturn:false,dialog:null,timer:null,text:null,retry:null,currentEntry:null,external:null,dismiss:null};
    active=record;
    // createDialog calls onClose before evaluating its return target. The
    // capture remains the witness after active is cleared by that cleanup.
    const returnCurrent=()=>live()&&(matchesCapture(record.capture)||record.allowReturn);
    let dialog;
    try{dialog=createDialog(main?'Состояние приложения':'Действия окна приложения',()=>{
        if(record.timer!==null)view.clearInterval(record.timer);
        if(active===record)active=null;
        if(main)control.setAttribute('aria-expanded','false');
        onModalChange(dialog.element,false);
      },{isCurrent:returnCurrent,resolve:returnTarget});
    }catch{
      if(active===record)active=null;
      control.title='Не удалось открыть действия окна. Попробуйте ещё раз.';
      return;
    }
    record.dialog=dialog;dialog.element.classList.add(main?'sa-stage-runtime-dialog':'sa-widget-runtime-dialog');
    if(main)control.setAttribute('aria-expanded','true');
    record.text=el('p',main?'sa-stage-runtime-copy':'sa-widget-runtime-copy');
    const actions=el('div',main?'sa-stage-runtime-dialog-actions':'sa-widget-runtime-dialog-actions');
    function act(original,requireVisible){
      if(!valid(record)||!canAct()||original.disabled||requireVisible&&original.hidden){if(active===record&&!valid(record))close(false);return;}
      // Synchronous forwarding preserves the existing user-gesture popup path.
      record.allowReturn=true;
      try{original.click();}finally{if(active===record){
        // Native close may restore its opener before MutationObserver runs.
        // A main notice dismissed by this action must already be unfocusable.
        if(main)fallback.hidden=message.hidden||!message.textContent;
        close(live());
      }}
    }
    record.retry=button(main?'Открыть приложение заново':'Обновить окно','refresh','sw-button-quiet',()=>act(retry,true));
    record.currentEntry=button('Открыть актуальный вход','arrow','sw-button-primary',()=>act(currentEntry,true));
    record.external=button('Открыть в новой вкладке','external','sw-button-quiet',()=>act(external,false));
    record.dismiss=button('Скрыть сообщение','close','sw-button-quiet',()=>act(dismiss,true));
    actions.append(record.retry,record.currentEntry,record.external,record.dismiss);dialog.body.append(record.text,actions);
    record.timer=view.setInterval(()=>{if(active===record&&!valid(record))close(false);},250);
    sync();if(active===record)onModalChange(dialog.element,true);
  }
  // Observe only existing local UI state. No context read, grant or renewal.
  const observer=new view.MutationObserver(sync);
  observer.observe(message,{childList:true,subtree:true,attributes:true,attributeFilter:['hidden']});
  observer.observe(runtimeActions,{subtree:true,attributes:true,attributeFilter:['hidden','disabled']});
  observer.observe(runtime,{childList:true,attributes:true,attributeFilter:['aria-busy']});
  sync();
  return Object.freeze({
    attach(target,returnOwner){
      if(main||!live()||header||!target?.isConnected||target.ownerDocument!==screen.ownerDocument
        ||typeof returnOwner?.isCurrent!=='function'||typeof returnOwner?.resolve!=='function'||!returnOwner.isCurrent())return false;
      owner=returnOwner;header=target;target.append(control);fallback.hidden=true;return true;
    },
    dispose(){
      if(disposed)return;disposed=true;observer.disconnect();close(false);control.remove();if(main&&message.parentElement===fallback)fallback.after(message);fallback.remove();
      screen.classList.remove(main?'has-stage-runtime-controls':'has-widget-runtime-controls');owner=header=null;
    },
  });
}
