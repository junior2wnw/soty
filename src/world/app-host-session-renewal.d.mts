import type {AppHostSessionBinding,AppHostSessionState} from './app-launch.mjs';
export type AppHostSessionRenewReason='reset'|'near-expiry';
export interface AppHostSessionProof {frameId:string;lease:string;revision:number}
export interface AppHostSessionRenewReply extends AppHostSessionState {launchUrl:string;expiresAt:number;hostSessionRequestId:string;hostSessionExpiresAt:number}
export interface AppHostSessionContext extends AppHostSessionState {schema:'soty.app-host-session-context.v1';state:'current'|'renew-required'|'admitting'}
export interface AppHostSessionConfirmation extends AppHostSessionState {schema:'soty.app-host-session-confirmation.v1';requestId:string;confirmed:true;hostSessionExpiresAt:number}
export function createAppHostSessionRenewal<TTimer=ReturnType<typeof setTimeout>>(options:{
  readBinding():AppHostSessionBinding|null;isCurrent():boolean;shellUrl:string;
  context(args:AppHostSessionProof&{currentEntryPath:string}):Promise<unknown>;
  request(args:AppHostSessionProof&{requestId:string;parentOrigin:string;currentEntryPath:string}):Promise<unknown>;
  bootstrap(reply:AppHostSessionRenewReply,options:{signal:AbortSignal}):Promise<unknown>;
  confirm(args:AppHostSessionProof&{requestId:string}):Promise<unknown>;
  commit(previous:AppHostSessionBinding,next:AppHostSessionState):boolean;
  close(args:AppHostSessionProof):Promise<unknown>;
  cleanupForReply?(reply:unknown):(()=>Promise<void>)|null;
  onState?(state:'pending'|'confirmed'|'denied'|'unavailable',error?:unknown):void;
  clock?():number;schedule?:(callback:()=>void,delay:number)=>TTimer;cancel?:(timer:TTimer)=>void;randomId?():string;
}):{start():void;renew(reason?:AppHostSessionRenewReason):Promise<boolean>;dispose():void};
