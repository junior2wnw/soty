export interface ScopedRenewBinding {handle:string;source:{id:string;version:number;digest:string};slot:object}
export interface ScopedRenewReply {url:string;handle:string;requestId:string;source:{id:string;version:number;digest:string};expiresAt:number;cleanup?:()=>Promise<void>;slot:object}
export function isRootSsoWorkspacePin(value:unknown):boolean;
export function attachRootSsoWakeRecovery(options:{view:Window;document?:Document;signal:AbortSignal;current():boolean;
  readBinding():ScopedRenewBinding|null;lastLaunchAt():number;recover():Promise<void>;clock?:()=>number;onFailure?(error:unknown):void}):
  (()=>void)&{recoverExpired(error:unknown,binding:ScopedRenewBinding|null|undefined):void};
export function attachScopedRenewalLoads(options:{runtime:HTMLElement;view:Window;signal:AbortSignal;current():boolean;
  profile():unknown;renewal:{probe():Promise<unknown>;tick():Promise<unknown>};readBinding?():ScopedRenewBinding|null;
  onTickFailure?(error:unknown,binding:ScopedRenewBinding|null|undefined):void}):()=>void;
export function createScopedSlotRenewal(options:{readBinding():ScopedRenewBinding|null;readContext(handle:string):Promise<any>;
  request(args:{handle:string;requestId:string}):Promise<ScopedRenewReply>;bootstrap(reply:ScopedRenewReply):Promise<'ready'|'login_required'|'unknown'>;
  commit(previous:ScopedRenewBinding,next:ScopedRenewReply):boolean;isCurrent():boolean;onState?(state:string):void;clock?:()=>number}):{
  probe():Promise<boolean>;renew():Promise<boolean>;ensure(minRemaining?:number):Promise<boolean>;tick():Promise<boolean>;beginCaptureLease():()=>void;dispose():void};
