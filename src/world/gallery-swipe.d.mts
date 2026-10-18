export interface GallerySwipeLifetime {():void;cancel():void;}
export interface GalleryViewportPort {readonly scale:number;addEventListener?(type:'resize',listener:()=>void,options?:{signal:AbortSignal}):void;removeEventListener?(type:'resize',listener:()=>void):void;}
export function bindGallerySwipe(stage:HTMLElement,options:{current():boolean;previous():void;next():void;selectionEpoch?():number;viewport?:GalleryViewportPort|null}):GallerySwipeLifetime;
