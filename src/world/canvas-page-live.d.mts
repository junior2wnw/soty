import type {CanvasPageView} from './canvas-page-client.mjs';
interface CallOptions {signal?:AbortSignal}
interface CanvasPageLiveClient {
 load(first?:CanvasPageView,options?:CallOptions):Promise<CanvasPageView>;
 wait(after:string,options?:CallOptions):Promise<{changed:boolean;viewRevision:string;view:CanvasPageView|null}>;
}
export interface CanvasPageLiveOptions {
 client:CanvasPageLiveClient;
 apply(view:CanvasPageView):void;
 onUnavailable(error:unknown):void;
 isCurrent():boolean;
 events?:Pick<EventTarget,'addEventListener'|'removeEventListener'>;
 online?:()=>boolean;
 timers?:{setTimeout(callback:()=>void,delay:number):unknown;clearTimeout(id:unknown):void};
 retryDelays?:readonly number[];
}
export function createCanvasPageLive(options:CanvasPageLiveOptions):{
 start():Promise<void>;
 refresh(first?:CanvasPageView):Promise<void>;
 dispose():void;
};
