export interface CanvasVaultLocalKeyScope{schema:'soty.vault.local-scope.v1';kind:'source'|'placement';id:string}
export function canvasVaultScope(value:unknown):Readonly<CanvasVaultLocalKeyScope>;
export function sameCanvasVaultScope(a:unknown,b:unknown):boolean;
export function canvasVaultScopeSlot(scope:CanvasVaultLocalKeyScope,projectId:string,crypto?:Crypto):Promise<string>;
declare const scopeBrand:unique symbol;
export interface CanvasVaultRecoveryScope{readonly[scopeBrand]:true}
export function createCanvasVaultRecoveryScope(options:{objectId:string;source:CanvasVaultLocalKeyScope;profile:{slot:string;profileId:string;salt:string};isCurrent():boolean}):CanvasVaultRecoveryScope;
export function canvasVaultRecoveryScope(token:CanvasVaultRecoveryScope|undefined,objectId:string,profile:{slot:string;profileId:string;salt:string}):{slot:string;active():void};
