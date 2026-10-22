export type NotificationTarget = { kind:'room';communityId:string } | { kind:'direct';threadId:string;peerAccountId:string;relationshipId:string };
export interface NotificationsView {phase:'checking'|'unavailable'|'off'|'on'|'denied'|'unknown';configured:boolean;supported:boolean;enabled:boolean;busy:boolean;code:string|null;}
export interface NotificationsController {
  view():NotificationsView;
  subscribe(listener:(state:NotificationsView)=>void):()=>void;
  refresh():Promise<NotificationsView>;
  enable():Promise<void>;
  disable():Promise<void>;
  open(notificationId:string,bindingId?:string):Promise<boolean>;
  dispose(options?:{accountChanged?:boolean}):void;
}
export function createNotificationsController(options:{
  client:{getLocalState():Promise<{accountId?:string|null;deviceId?:string|null;localRevision?:number|null}>;
    extension<T>(op:string,args:Record<string,unknown>,options:{expectedAccountId:string}):Promise<T>};
  accountId:string;isCurrent?():boolean;
  onOpen?(target:NotificationTarget,context:{assertCurrent():Promise<void>;signal:AbortSignal}):void|Promise<void>;
}):NotificationsController;
