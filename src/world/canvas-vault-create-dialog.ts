import {createDialog,pinDialogSubmit,type WorldDialog} from './dialogs';
import {el,button} from './dom';
import {createCanvasVaultCreateFlow,type CanvasVaultCreateFlow,type CanvasVaultCreateOptions} from './canvas-vault-create-flow.mjs';
import {canvasCreateFileKind,type CanvasCreateResult} from './canvas-create-flow.mjs';
import {canvasVaultErrorText} from './canvas-vault-renderer';
import type {CanvasMask} from '../../modules/canvas-objects/contract.mjs';
import './canvas-vault-create-dialog.css';
export interface CanvasVaultCreateDialogOptions extends CanvasVaultCreateOptions{parent?:HTMLElement;onDone?:(result:CanvasCreateResult)=>void;onClose?:()=>void}
export interface CanvasVaultCreateDialog extends WorldDialog{flow:CanvasVaultCreateFlow;dispose():void;hasUnsavedChanges():boolean}

/** A new encrypted Source, never a password flag on an existing plaintext note. */
export function openCanvasVaultCreateDialog(options:CanvasVaultCreateDialogOptions):CanvasVaultCreateDialog{
  let dead=false,busy=false,retryId:string|null=null,recovering=false,file:File|null=null,previewUrl='',mode:'note'|'file'='note',flow:CanvasVaultCreateFlow;
  const controller=new AbortController(),signal=controller.signal,current=()=>!dead&&options.isCurrent();
  const dialog=createDialog('Под паролем',()=>{if(dead)return;dead=true;controller.abort();flow?.dispose();title.value=body.value=password.value=confirm.value=picker.value='';file=null;if(previewUrl)URL.revokeObjectURL(previewUrl);previewUrl='';image.removeAttribute('src');options.onClose?.();},undefined,options.parent);
  dialog.element.classList.add('cv-create');
  const form=el('form','cvc-form'),modes=el('div','cvc-modes'),noteMode=button('Записка','edit'),fileMode=button('Файл','attach');modes.setAttribute('role','group');modes.setAttribute('aria-label','Что защитить');modes.append(noteMode,fileMode);
  const title=el('input');title.placeholder='Название — необязательно';title.maxLength=160;title.setAttribute('aria-label','Название защищённой записки');
  const body=el('textarea','cvc-body');body.placeholder='Запишите мысль';body.maxLength=100000;body.setAttribute('aria-label','Текст защищённой записки');
  const noteFields=el('div','cvc-note');noteFields.append(title,body);
  const picker=el('input');picker.type='file';picker.hidden=true;picker.accept='image/png,image/jpeg,image/webp,audio/webm,audio/ogg,audio/wav,video/webm';picker.setAttribute('aria-label','Выбрать защищённый файл до 1 МБ');
  const fileFields=el('div','cvc-file'),image=el('img','cvc-preview'),name=el('span'),change=button('Выбрать файл','attach');image.alt='Предпросмотр выбранного файла';fileFields.hidden=true;fileFields.append(image,name,change);
  const password=el('input');password.type='password';password.autocomplete='new-password';password.minLength=8;password.maxLength=1024;password.placeholder='Пароль — от 8 символов';password.setAttribute('aria-label','Пароль нового объекта');
  const confirm=el('input');confirm.type='password';confirm.autocomplete='new-password';confirm.minLength=8;confirm.maxLength=1024;confirm.placeholder='Повторите пароль';confirm.setAttribute('aria-label','Повторите пароль');
  const settings=el('details'),summary=el('summary','','Форма'),shape=el('select');shape.setAttribute('aria-label','Форма защищённого объекта');for(const[value,text]of[['rect','Карточка'],['ellipse','Круг'],['hex','Сота']]){const option=el('option','',text);option.value=value!;shape.append(option);}shape.value='rect';settings.append(summary,shape);
  const hint=el('p','cvc-hint','Новый объект будет виден только вам. Файлы — до 1 МБ. Пароль не сохраняется.'),status=el('p','cvc-status'),error=el('p','cvc-error');status.setAttribute('role','status');status.setAttribute('aria-live','polite');error.setAttribute('role','alert');error.hidden=true;
  const fresh=button('Создать другое');fresh.hidden=true;const pending=el('details','cvc-pending'),pendingTitle=el('summary','','Зашифрованные черновики'),pendingRows=el('div');pending.hidden=true;pending.append(pendingTitle,pendingRows);
  form.append(modes,noteFields,picker,fileFields,password,confirm,settings,hint,status,error,fresh,pending);dialog.body.append(form);
  const submit=button('Зашифровать и разместить','lock','sw-button-primary');pinDialogSubmit(dialog,form,submit);
  flow=createCanvasVaultCreateFlow({...options,onProgress:value=>{if(!current())return;options.onProgress?.(value);status.textContent=value.phase==='source'?'Шифруем содержимое…':value.phase==='object'?'Подготавливаем карточку…':value.phase==='placement'?'Размещаем…':value.phase==='complete'?'Готово':value.localDurable?'Зашифрованный черновик сохранён':'Черновик не сохранён';}});
  function paint(){
    noteFields.hidden=recovering||mode!=='note';fileFields.hidden=recovering||mode!=='file';confirm.hidden=!!retryId;settings.hidden=!!retryId;
    for(const[control,selected]of[[noteMode,mode==='note'],[fileMode,mode==='file']] as const){control.setAttribute('aria-pressed',String(selected));control.disabled=busy||!!retryId;}
    title.readOnly=body.readOnly=busy||!!retryId;password.disabled=confirm.disabled=busy;shape.disabled=change.disabled=busy||!!retryId;fresh.hidden=!retryId;fresh.disabled=busy;
    for(const row of pendingRows.querySelectorAll<HTMLButtonElement>('button'))row.disabled=busy;
    submit.disabled=busy||!retryId&&mode==='file'&&!file;submit.textContent=busy?'Сохраняем…':retryId?'Продолжить':'Зашифровать и разместить';
  }
  function show(errorValue:unknown){if(!current())return;const code=String((errorValue as {code?:string})?.code??'');error.textContent=code==='vault_password_mismatch'?'Пароли не совпали.':code==='vault_password_too_long'?'Пароль слишком длинный.':code==='canvas_create_media_type_unknown'||code==='canvas_create_media_type_unsupported'?'Подойдут PNG, JPEG, WebP, WebM, Ogg или WAV.':canvasVaultErrorText(errorValue);error.hidden=false;
    const value=errorValue as {operationId?:string;localDurable?:boolean};if(value.operationId){retryId=value.operationId;status.textContent=value.localDurable?'Зашифрованный черновик сохранён на устройстве':'Черновик пока только на этом экране';}paint();void loadPending();}
  function setFile(next:File){if(!current()||busy||retryId)return;try{if(next.size===0||next.size>1024*1024)throw{code:'vault_wire_size'};const kind=canvasCreateFileKind(next);file=next;mode='file';if(previewUrl)URL.revokeObjectURL(previewUrl);previewUrl='';name.textContent=next.name;image.hidden=kind!=='image';if(kind==='image'){previewUrl=URL.createObjectURL(next);image.src=previewUrl;}else image.removeAttribute('src');error.hidden=true;paint();}catch(value){show(value);}}
  noteMode.addEventListener('click',()=>{if(!current()||busy||retryId)return;mode='note';paint();body.focus();},{signal});fileMode.addEventListener('click',()=>{if(current()&&!busy&&!retryId)picker.click();},{signal});change.addEventListener('click',()=>{if(current()&&!busy&&!retryId)picker.click();},{signal});
  picker.addEventListener('change',()=>{const value=picker.files?.[0];if(value)setFile(value);picker.value='';},{signal});
  form.addEventListener('dragover',event=>{if(event.dataTransfer?.types.includes('Files'))event.preventDefault();},{signal});
  form.addEventListener('drop',event=>{const values=event.dataTransfer?.files;if(!values?.length)return;event.preventDefault();if(values.length!==1){show({code:'canvas_create_media_type_unsupported'});return;}setFile(values[0]!);},{signal});
  form.addEventListener('paste',event=>{const values=event.clipboardData?.files;if(!values?.length)return;event.preventDefault();if(values.length===1)setFile(values[0]!);else show({code:'canvas_create_media_type_unsupported'});},{signal});
  fresh.addEventListener('click',()=>{if(!current()||busy)return;retryId=null;recovering=false;password.value=confirm.value='';error.hidden=true;status.textContent='';paint();},{signal});
  async function loadPending(){try{const values=await flow.listPending();if(!current())return;pendingRows.replaceChildren();pending.hidden=!values.length;for(const value of values){const row=el('div'),resume=button('Продолжить','lock'),remove=button('Удалить черновик','close');row.append(el('span','',value.label),resume,remove);pendingRows.append(row);
    resume.addEventListener('click',()=>{if(!current()||busy)return;retryId=value.operationId;recovering=true;password.value=confirm.value='';status.textContent='Введите пароль этого черновика';error.hidden=true;paint();password.focus();},{signal});
    remove.addEventListener('click',()=>{if(!current()||busy)return;busy=true;paint();void flow.removeLocal(value.operationId).then(loadPending).catch(show).finally(()=>{busy=false;paint();});},{signal});}paint();}catch{/* A failed local store is reported by the attempted save; no fake recovery row. */}}
  form.addEventListener('submit',event=>{event.preventDefault();if(!current()||busy)return;
    if(!retryId&&password.value!==confirm.value){show({code:'vault_password_mismatch'});return;}if(password.value.length<8){show({code:'vault_password_invalid'});return;}if(new TextEncoder().encode(password.value).length>1024){show({code:'vault_password_too_long'});return;}
    busy=true;error.hidden=true;paint();const secret=password.value;
    void(async()=>{try{const result=retryId?await flow.resume(retryId,secret):mode==='note'?await flow.startNote({title:title.value,body:body.value,mask:{shape:shape.value} as CanvasMask},secret):await flow.startFile(file!,secret,{mask:{shape:shape.value} as CanvasMask});
      if(!current())return;options.onDone?.(result);dialog.close();
    }catch(value){show(value);}finally{password.value=confirm.value='';busy=false;if(current())paint();}})();
  },{signal});
  paint();void loadPending();body.focus();return Object.assign(dialog,{flow,hasUnsavedChanges:()=>!dead&&(busy||!!title.value||!!body.value||!!password.value||!!confirm.value||!!file),dispose(){dialog.close();}});
}
