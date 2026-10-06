import type {AppArt,AppArtInput} from './app-art.mjs';
export interface FieldAppArt extends AppArt {
 readonly icon?:'cells'|'brush'|'bars'|'activity'|'note'|'chess'|'music'|'app';
 readonly profile:'field'|'card';
 readonly fallback:boolean;
 readonly focalPosition:string;
 readonly compactFocalPosition:string;
}
export interface FieldArtOptions {screenWidth?:number;}
export function resolveFieldAppArt(input:AppArtInput,options?:FieldArtOptions):FieldAppArt;
export function createFieldArtResolver(baseManifest:unknown,fieldProfile:unknown):(input:AppArtInput,options?:FieldArtOptions)=>FieldAppArt;
