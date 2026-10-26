export function createAppWorkGuard(options:{view:Window;getFrame():HTMLIFrameElement|null;isCurrent():boolean;onStatus?(state:{supported:boolean;dirty:boolean}):void;timeoutMs?:number}):{
  attach(frame:HTMLIFrameElement,origin:string):void; hasUnsavedChanges():boolean; requiresCheck():boolean; hasUnconfirmedWork():boolean; flush():Promise<void>; dispose():void;
};
