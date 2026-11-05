import {button,el,labeledField} from './dom';
import {canvasVaultErrorText} from './canvas-vault-renderer';
import type {CanvasVaultLocalRecovery} from './canvas-vault-source-migration.mjs';

export interface CanvasObjectSecurityStatus {
  locked:boolean;protection?:{kind:'none'|'password'};state?:'sealing';
  capabilities?:{nativeConversion:boolean;placementProtection:boolean};
  device?:'pending'|'complete';protectionComplete?:boolean;operationId?:string;
  reason?:string;
}
export interface CanvasObjectSecurityResult {
  server:'unknown'|'unprotected'|'pending'|'protected';device:'pending'|'complete';protectionComplete:boolean;reason?:string;
}
export interface CanvasObjectSecurityActions {
  status():Promise<CanvasObjectSecurityStatus>;
  protect(password:string,operationId:string):Promise<CanvasObjectSecurityResult>;
  relock(scope:'device'|'all',requestId:string):Promise<void>;
  rotate(oldPassword:string,newPassword:string):Promise<void>;
  localRecoveryState?():Promise<CanvasVaultLocalRecovery>;
  recoverLocalPassword?(passwords:{currentSourcePassword:string;priorLocalPassword:string}):Promise<CanvasVaultLocalRecovery>;
}
export interface CanvasObjectSecurityOptions {
  actions:CanvasObjectSecurityActions;isCurrent():boolean;
  beforeChange?():Promise<void>;onBusyChange?(busy:boolean):void;onPendingChange?(pending:boolean):void;
  onDone?():Promise<void>;
}

/** Password inputs stay in this disposable form; a retry retains only its ID. */
export function mountCanvasObjectSecurity(host:HTMLElement,options:CanvasObjectSecurityOptions){
  const controller=new AbortController(),signal=controller.signal;
  let dead=false,busy=false,loading=false,statusFailed=false,refreshQueued=false,loadGeneration=0,value:CanvasObjectSecurityStatus|null=null,operationId:string|null=null;
  let mode:'protect'|'rotate'='protect';
  let localRecovery:CanvasVaultLocalRecovery|null=null,localRecoveryFailed=false,localRecoveryRequested=false,localNotice='';
  const current=()=>!dead&&options.isCurrent();
  const details=el('details','co-security'),summary=el('summary','','Пароль'),content=el('div','co-security-content');
  const status=el('p','co-status','Проверяем защиту…'),error=el('p','co-error');status.setAttribute('role','status');error.setAttribute('role','alert');error.hidden=true;
  const retry=button('Повторить проверку','refresh','sw-button-quiet');retry.hidden=true;
  const quick=el('div','co-quick'),lock=button('Закрыть здесь','lock'),lockAll=button('Закрыть на всех устройствах','lock');quick.append(lock,lockAll);
  const form=el('form','co-security-form'),old=el('input'),password=el('input'),confirm=el('input');
  for(const input of [old,password,confirm]){input.type='password';input.maxLength=1024;input.minLength=8;}
  old.autocomplete='current-password';password.autocomplete=confirm.autocomplete='new-password';
  old.setAttribute('aria-label','Текущий пароль объекта');password.setAttribute('aria-label','Новый пароль объекта');confirm.setAttribute('aria-label','Повторите новый пароль');
  const oldRow=labeledField('Текущий пароль',old),newRow=labeledField('Новый пароль',password),confirmRow=labeledField('Повторите пароль',confirm);
  const hint=el('p','co-status','Пароль не сохраняется. Сохраните его у себя.'),sourceHint=el('p','co-status');sourceHint.hidden=true;
  const submit=button('Защитить','lock','sw-button-quiet');submit.type='submit';
  form.append(oldRow,newRow,confirmRow,hint,submit);content.append(status,error,retry,quick,sourceHint,form);details.append(summary,content);host.append(details);
  const recovery=el('details','co-recovery'),recoveryTitle=el('summary','','Прежние копии'),recoveryStatus=el('p','co-status'),recoveryForm=el('form','co-security-form'),currentPassword=el('input'),priorPassword=el('input'),recover=button('Открыть прежние копии','lock','sw-button-quiet'),retryRecovery=button('Проверить копии','refresh','sw-button-quiet');
  recovery.hidden=true;recoveryStatus.setAttribute('role','status');recover.type='submit';
  for(const input of[currentPassword,priorPassword]){input.type='password';input.autocomplete='current-password';input.minLength=8;input.maxLength=1024;}
  currentPassword.setAttribute('aria-label','Текущий пароль для копий');priorPassword.setAttribute('aria-label','Прежний пароль на этом устройстве');
  const openRecovery=button('Пароль менялся на другом устройстве?','refresh','sw-button-quiet');openRecovery.hidden=true;
  recoveryForm.append(labeledField('Текущий пароль',currentPassword),labeledField('Прежний пароль',priorPassword),recover);recovery.append(recoveryTitle,recoveryStatus,recoveryForm,retryRecovery);content.append(openRecovery,recovery);
  function clear(){old.value=password.value=confirm.value=currentPassword.value=priorPassword.value='';}
  function paint(){
    const protectedObject=value?.protection?.kind==='password',pending=value?.state==='sealing'||value?.device==='pending',blocked=value?.reason==='native_coordinator_unavailable';
    const unavailable=busy||loading||statusFailed||!value;
    mode=protectedObject&&!pending?'rotate':'protect';summary.textContent=blocked?'Защита не проверена':pending?'Защита не завершена':protectedObject?'Под паролем':'Защитить паролем';
    oldRow.hidden=mode!=='rotate';confirmRow.hidden=!!pending;quick.hidden=!protectedObject;lock.disabled=lockAll.disabled=unavailable;
    password.setAttribute('aria-label',pending?'Пароль объекта':'Новый пароль объекта');
    if(newRow.firstElementChild)newRow.firstElementChild.textContent=pending?'Пароль объекта':'Новый пароль';
    form.hidden=!value||blocked||!protectedObject&&!value.capabilities?.nativeConversion&&!value.capabilities?.placementProtection&&!pending;
    old.disabled=password.disabled=confirm.disabled=submit.disabled=unavailable||blocked;
    retry.hidden=!statusFailed&&!blocked;retry.disabled=busy||loading;
    submit.textContent=busy?'Подождите…':mode==='rotate'?'Сменить пароль':operationId?'Продолжить защиту':'Защитить';
    if(loading)status.textContent='Проверяем защиту…';
    else if(statusFailed)status.textContent='Не удалось проверить защиту. Повторите проверку.';
    else if(value&&!busy)status.textContent=blocked?'Не удалось проверить локальную защиту на этом устройстве. Обновите Соты и повторите проверку.':pending?
      /quota/u.test(value.reason??'')?'Для зашифрованной копии не хватает места. Освободите место на устройстве и повторите. Прежние данные сохранены.':
      'Защита ещё не завершена. Повторите действие, чтобы проверить сохранение и локальные копии.':protectedObject?(value.locked?'Объект закрыт.':'Открыт на этом устройстве.'):'Сначала сохраните изменения. Затем содержимое будет закрыто.';
    if(value&&!protectedObject&&!pending&&form.hidden&&!loading&&!statusFailed)status.textContent='Этот источник пока нельзя перенести под пароль. Новую запись можно сразу создать защищённой.';
    sourceHint.hidden=!value?.capabilities?.placementProtection||protectedObject;
    sourceHint.textContent='Пароль закрывает доступ через Соты. Самостоятельный адрес внешнего приложения сохраняет собственные правила доступа.';
    recovery.hidden=!protectedObject||!localRecoveryRequested&&!localRecoveryFailed&&(!localRecovery||localRecovery.complete);
    openRecovery.hidden=!protectedObject||!options.actions.recoverLocalPassword||pending||blocked||!recovery.hidden;openRecovery.disabled=unavailable;
    recoveryForm.hidden=!options.actions.recoverLocalPassword||localRecoveryFailed;
    currentPassword.disabled=priorPassword.disabled=recover.disabled=retryRecovery.disabled=unavailable;
    retryRecovery.hidden=!localRecoveryFailed;
    recoveryStatus.textContent=localRecoveryFailed?'Не удалось проверить прежние копии. Текущая запись остаётся под паролем.':localRecoveryRequested&&localRecovery?.complete?'Укажите текущий и прежний пароль. Проверим копии на этом устройстве.':localRecovery?.knownSource==='pending'?'На этом устройстве остались копии с прежним паролем. Откройте их, если нужен старый черновик.':'Некоторые прежние копии ещё не распознаны. Их можно проверить прежним паролем.';
    if(localNotice&&!busy&&!loading)status.textContent=localNotice;
  }
  function fail(reason:unknown){if(current()){error.textContent=canvasVaultErrorText(reason);error.hidden=false;}}
  async function load(){
    if(!current())return;const generation=++loadGeneration;loading=true;statusFailed=false;error.hidden=true;paint();
    try{const result=await options.actions.status();if(!current()||generation!==loadGeneration)return;value=result;const pending=result.state==='sealing'||result.device==='pending';if(pending&&result.operationId)operationId=result.operationId;options.onPendingChange?.(pending);
      localRecovery=null;localRecoveryFailed=false;
      if(result.protection?.kind==='password'&&options.actions.localRecoveryState){try{const local=await options.actions.localRecoveryState();if(current()&&generation===loadGeneration)localRecovery=local;}catch{if(current()&&generation===loadGeneration)localRecoveryFailed=true;}}
    }
    catch(reason){if(current()&&generation===loadGeneration){value=null;statusFailed=true;clear();fail(reason);}}
    finally{if(current()&&generation===loadGeneration){loading=false;paint();}}
  }
  retry.addEventListener('click',()=>{if(!current()||busy||loading)return;clear();void load();},{signal});
  retryRecovery.addEventListener('click',()=>{if(!current()||busy||loading)return;clear();void load();},{signal});
  openRecovery.addEventListener('click',()=>{if(!current()||busy||loading)return;clear();localRecoveryRequested=true;recovery.open=true;paint();currentPassword.focus();},{signal});
  async function perform(action:()=>Promise<void>){
    if(!current()||busy||loading||statusFailed||!value)return;busy=true;localNotice='';error.hidden=true;options.onBusyChange?.(true);paint();
    try{await options.beforeChange?.();if(!current())return;await action();}
    catch(reason){fail(reason);}
    finally{clear();busy=false;if(current()){options.onBusyChange?.(false);paint();if(refreshQueued){refreshQueued=false;void load();}}}
  }
  const lockObject=(scope:'device'|'all')=>{
    const requestId=crypto.randomUUID();void perform(async()=>{await options.actions.relock(scope,requestId);if(!current())return;await load();if(current())await options.onDone?.();});
  };
  lock.addEventListener('click',()=>lockObject('device'),{signal});lockAll.addEventListener('click',()=>lockObject('all'),{signal});
  recoveryForm.addEventListener('submit',event=>{event.preventDefault();if(!current()||busy||loading||!options.actions.recoverLocalPassword)return;
    if(currentPassword.value.length<8||priorPassword.value.length<8){fail({code:'vault_password_invalid'});return;}
    if([currentPassword.value,priorPassword.value].some(text=>new TextEncoder().encode(text).byteLength>1024)){error.textContent='Пароль слишком длинный.';error.hidden=false;return;}
    const passwords={currentSourcePassword:currentPassword.value,priorLocalPassword:priorPassword.value};
    void perform(async()=>{const result=await options.actions.recoverLocalPassword!(passwords);if(!current())return;localRecovery=result;localRecoveryFailed=false;localRecoveryRequested=!result.complete;clear();
      localNotice=result.complete?'Прежние копии готовы на этом устройстве.':'Часть копий ещё закрыта. Для них может понадобиться другой прежний пароль.';
      recovery.open=!result.complete;
    });
  },{signal});
  form.addEventListener('submit',event=>{event.preventDefault();if(!current()||busy||loading||statusFailed||!value||value.reason==='native_coordinator_unavailable')return;
    if(!confirmRow.hidden&&password.value!==confirm.value){fail({code:'vault_password_mismatch'});error.textContent='Пароли не совпали.';return;}
    if(password.value.length<8||mode==='rotate'&&old.value.length<8){fail({code:'vault_password_invalid'});return;}
    if([password.value,old.value].some(text=>new TextEncoder().encode(text).byteLength>1024)){error.textContent='Пароль слишком длинный.';error.hidden=false;return;}
    const next=password.value,previous=old.value,kind=mode;
    void perform(async()=>{
      if(kind==='rotate'){await options.actions.rotate(previous,next);if(!current())return;operationId=null;await load();if(current()&&(localRecoveryFailed||localRecovery&&!localRecovery.complete)){recovery.open=true;localNotice=localRecoveryFailed?'Пароль изменён. Проверку прежних копий можно повторить.':'Пароль изменён. Прежние копии можно открыть отдельно.';}else if(current())await options.onDone?.();return;}
      operationId??=crypto.randomUUID();const requestId=operationId;
      const result=await options.actions.protect(next,requestId);if(!current())return;
      options.onPendingChange?.(!result.protectionComplete);
      if(!result.protectionComplete){value={...value!,state:'sealing',device:'pending',protection:{kind:result.server==='protected'?'password':'none'},...(result.reason?{reason:result.reason}:{})};details.open=true;paint();return;}
      operationId=null;await load();if(current())await options.onDone?.();
    });
  },{signal});
  void load();
  return{element:details,isLoadingStatus:()=>loading,refresh:()=>{if(!current())return Promise.resolve();if(busy){refreshQueued=true;return Promise.resolve();}clear();return load();},hasUnsavedChanges:()=>!dead&&(busy||!!old.value||!!password.value||!!confirm.value||!!currentPassword.value||!!priorPassword.value),dispose(){if(dead)return;dead=true;loadGeneration++;controller.abort();clear();details.remove();}};
}
