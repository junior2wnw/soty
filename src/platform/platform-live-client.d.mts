export interface LiveWatch {kind:'app'|'person'|'community'|'object';id:string}
export interface PlatformLiveStatus {paused:boolean;disposed:boolean;inFlight:boolean;failures:number}
export function createPlatformLiveClient(options:{api:{request<T>(operation:string,args?:Record<string,unknown>):Promise<T>};accountId:string;
  isCurrent?:()=>boolean;watch?:()=>LiveWatch[];onInvalidate:(event:{reset:boolean})=>Promise<void>|void;
  onAccessLost?:(error:unknown)=>Promise<void>|void;onStatus?:(status:'connecting'|'live'|'reconnecting'|'paused'|'unavailable')=>void;
  page?:Pick<Document,'visibilityState'|'addEventListener'|'removeEventListener'>;
}):{reconnect():void;dispose():void;status():PlatformLiveStatus};
export function isLiveIdentityLoss(error:unknown):boolean;
export function isLocalReadConflict(error:unknown):boolean;
