export interface AppRuntimeCloseRequest {
  isCurrent():boolean;
  /** False for Keep, Escape or a retired intent. Resolve after dialog cleanup. */
  confirmUnknown(isCurrent:()=>boolean):Promise<boolean>;
}
export function closeAppRuntime(options:{
  stage:{hasUnconfirmedWork():boolean;flush():Promise<void>;hasUnsavedChanges():boolean};
  isCurrent():boolean;
  confirmUnknown(isCurrent:()=>boolean):Promise<boolean>;
  onFailure(error?:unknown):void;
  commit():void;
}):Promise<boolean>;
