import type {AppLaunchIntent} from './app-launch.mjs';
export interface RetainedAppRuntimeOwner {
  host:HTMLElement;closeEpoch:number;
  stage:{matches(intent:AppLaunchIntent):boolean;dispose():void}|null;
}
export interface RetainedAppRuntimes<T extends RetainedAppRuntimeOwner>{
  all():T[];active():T|null;has(owner:T):boolean;find(intent:AppLaunchIntent):T|null;
  add(owner:T):boolean;present(owner:T|null):boolean;retire(owner:T):boolean;clear():void;
}
export function createRetainedAppRuntimes<T extends RetainedAppRuntimeOwner>(onPresent?:(owner:T|null)=>void):RetainedAppRuntimes<T>;
