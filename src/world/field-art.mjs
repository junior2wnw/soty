import baseManifest from './app-art-manifest.json' with {type:'json'};
import fieldProfile from './app-art-field-profile.json' with {type:'json'};
import {createAppArtResolver} from './app-art.mjs';
import {appArtBindingKey} from './app-art-identity.mjs';
const KEY=/^[a-z][a-z0-9-]{1,63}$/u;
const ICONS=new Set(['cells','brush','bars','activity','note','chess','music','app']);
const own=(object,key)=>object&&Object.hasOwn(object,key)?object[key]:undefined;
const safe=value=>typeof value==='string'&&KEY.test(value)?value:null;
const position=point=>`${Number((point.x*100).toFixed(3))}% ${Number((point.y*100).toFixed(3))}%`;
export function createFieldArtResolver(base,profile){
 const baseResolve=createAppArtResolver(base),fieldResolve=createAppArtResolver(profile);
 return function resolve(input,{screenWidth=208}={}){
  const record=input&&typeof input==='object'?input:null;
  const id=typeof record?.appId==='string'?record.appId:typeof record?.id==='string'?record.id:typeof input==='string'?input:'';
  const requested=safe(typeof input==='string'?input:record?.coverKey);
  const alias=safe(own(profile?.aliases,requested||'')),bound=safe(own(profile?.bindings,appArtBindingKey(id)||''));
  // Exact-ID binding wins over a supplied alias. Names and arbitrary URLs never
  // select artwork; an invalid profile candidate falls back to native card art.
  const key=bound||alias||(requested&&own(profile?.assets,requested)?requested:null);
  const selected=key?fieldResolve(key):null;
  const art=selected?.status==='ready'?selected:baseResolve(input);
  const glyph=selected?.status==='ready'?own(own(profile?.assets,key),'icon'):undefined;
  const width=typeof screenWidth==='number'&&Number.isFinite(screenWidth)?Math.min(512,Math.max(88,screenWidth)):208;
  return Object.freeze({...art,...(ICONS.has(glyph)?{icon:glyph}:{}),profile:selected?.status==='ready'?'field':'card',fallback:art.status!=='ready',sizes:`${Math.ceil(width)}px`,focalPosition:position(art.focalPoint),compactFocalPosition:position(art.compactFocalPoint)});
 };
}
export const resolveFieldAppArt=createFieldArtResolver(baseManifest,fieldProfile);
