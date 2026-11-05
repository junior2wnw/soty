import type {WorldDialog,DialogReturnTarget} from './dialogs';
import type {AppRuntimeCloseRequest} from './app-runtime-close.mjs';
export type {AppRuntimeCloseRequest} from './app-runtime-close.mjs';
export interface WidgetRuntimeOwner extends DialogReturnTarget {
  requestClose?(request:AppRuntimeCloseRequest):Promise<void>;
}
export interface WidgetRuntimeControls {
  attach(target:HTMLElement,owner:WidgetRuntimeOwner):boolean;
  requestClose():Promise<void>;
  dispose():void;
}
export function createWidgetRuntimeControls(options:{
  presentation?:'main';returnTarget?:WidgetRuntimeOwner;mainControlHost?:HTMLElement;
  view:Window;screen:HTMLElement;message:HTMLElement;runtime:HTMLElement;runtimeActions:HTMLElement;
  externalRecovery?:HTMLButtonElement;
  current():boolean;capture():unknown;matchesCapture(value:any):boolean;canAct():boolean;
  createDialog(title:string,onClose:()=>void,target:DialogReturnTarget):WorldDialog;
  iconButton(label:string,icon:string,action:()=>void):HTMLButtonElement;
  button(label:string,icon:string,className:string,action:()=>void):HTMLButtonElement;
  el:typeof import('./dom').el;
  isFocusTarget(target:HTMLElement|null):boolean;
  retry:HTMLButtonElement;currentEntry:HTMLButtonElement;external:HTMLButtonElement;dismiss:HTMLButtonElement;
  onModalChange?(modal:HTMLDialogElement,open:boolean):void;
}):WidgetRuntimeControls;
