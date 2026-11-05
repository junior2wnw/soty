import {createDialog,type DialogReturnTarget,type WorldDialog} from './dialogs';
import {button,el} from './dom';
import {mountMonitoringPanel,type MonitoringPanelHandle,type MonitoringPanelOptions} from './monitoring-panel';

export interface MonitoringDialogHandle extends MonitoringPanelHandle {
  focus():void;
  suspend():void;
  resume():Promise<boolean>;
  forceClose():void;
}
export function openMonitoringDialog(options:MonitoringPanelOptions&{returnTarget?:DialogReturnTarget;onClosed?():void;onModalChange?(dialog:HTMLDialogElement,open:boolean):void}):MonitoringDialogHandle{
  let closed=false,closing=false,visible=true,modalActive=false,presentationEpoch=0,confirmation:WorldDialog|null=null;
  const current=()=>!closed&&options.isCurrent();
  const announceModal=(open:boolean)=>{if(modalActive===open)return;modalActive=open;options.onModalChange?.(dialog.element,open);};
  const dialog=createDialog(options.title??(options.scope.kind==='platform'?'Состояние Сот':'Состояние приложения'),()=>{
    if(closed)return;closed=true;visible=false;presentationEpoch++;confirmation?.close({restoreFocus:false});confirmation=null;panel.dispose();announceModal(false);options.onClosed?.();
  },options.returnTarget);
  dialog.element.classList.add('mon-dialog');
  const panel=mountMonitoringPanel(dialog.body,{...options,isCurrent:current});
  announceModal(true);
  async function requestClose(){
    if(!current()||!visible||closing)return;if(confirmation){confirmation.element.focus();return;}closing=true;
    try{
      if(!panel.hasUnsavedChanges()&&!panel.hasUnconfirmedWork()){dialog.close();return;}
      let accepted=false;
      const choice=createDialog('Закрыть панель?',()=>{if(confirmation===choice)confirmation=null;options.onModalChange?.(choice.element,false);if(accepted&&current())dialog.close();},
        {isCurrent:current,resolve:()=>dialog.element.querySelector<HTMLButtonElement>('.sw-dialog-header button')});
      confirmation=choice;
      if(panel.hasUnsavedChanges())choice.body.append(el('p','','Изменения в форме ещё не применены. При закрытии несохранённый ввод пропадёт.'));
      if(panel.hasUnconfirmedWork())choice.body.append(el('p','','Результат изменения ещё не подтверждён. Сохранённый запрос можно проверить при следующем открытии.'));
      const stay=button('Остаться','back','sw-button-primary',()=>choice.close());
      choice.body.append(stay,button('Закрыть панель','close','sw-button-quiet',()=>{accepted=true;choice.close({restoreFocus:false});}));
      options.onModalChange?.(choice.element,true);stay.focus();
    }finally{closing=false;}
  }
  // The common native dialog owns focus/Tab/Escape; intercept only user close
  // gestures so account revocation and host destruction can still scrub now.
  const closeButton=dialog.element.querySelector<HTMLButtonElement>('.sw-dialog-header button')!;
  closeButton.addEventListener('click',event=>{event.preventDefault();event.stopImmediatePropagation();void requestClose();},{capture:true});
  dialog.element.addEventListener('cancel',event=>{event.preventDefault();void requestClose();});
  function suspend(){if(closed)return;visible=false;presentationEpoch++;confirmation?.close({restoreFocus:false});confirmation=null;panel.setVisible(false);dialog.suspend();announceModal(false);}
  async function resume(){
    if(!current())return false;if(visible&&dialog.element.open)return true;
    visible=true;const ticket=++presentationEpoch;
    const shown=await dialog.resume(()=>current()&&visible&&ticket===presentationEpoch);
    if(!shown||!current()||!visible||ticket!==presentationEpoch)return false;
    panel.setVisible(true);announceModal(true);return true;
  }
  return{ready:panel.ready,refresh:panel.refresh,setVisible(next){if(next)void resume();else suspend();},hasUnsavedChanges:panel.hasUnsavedChanges,hasUnconfirmedWork:panel.hasUnconfirmedWork,flush:panel.flush,suspend,resume,
    focus(){if(current())void resume().then(shown=>{if(shown&&current()&&!dialog.element.contains(dialog.element.ownerDocument.activeElement))dialog.element.focus();});},dispose(){dialog.close({restoreFocus:false});},forceClose(){dialog.close({restoreFocus:false});}};
}
