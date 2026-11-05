import { OpticalGlass, type OpticalElement, type OpticalScene } from '../../vendor/optical-glass/src/liquid-glass.js';
import { roundedHexPath, SQRT3 } from '../geometry/hex.mjs';
import {validateFieldPresentation,fieldPresentationClip,type FieldPresentation} from './unified-field-presentation.mjs';
import './field-glass.css';

export interface FieldGlassStatus {mode:'webgl'|'fallback';surfaces:number;reason?:string}
export interface FieldGlassOptions {background?:string;disabled?:boolean;onStatus?:(status:FieldGlassStatus)=>void}
export interface FieldGlassHandle {ready:Promise<FieldGlassStatus>;refresh(options?:{background?:string;retry?:boolean}):void;dispose():void}
const mounted=new WeakMap<HTMLElement,FieldGlassHandle>();
const STILL=Object.freeze({enabled:false,mode:'surface',coupling:0,parallax:0,tilt:0,lift:0,scale:0,press:0,layerSeparation:0,contentDepth:0});
/** Optical raster allocation bounds. Field geometry/content remain usable through the DOM fallback. */
export const FIELD_GLASS_BUDGET=Object.freeze({side:2048,facePixels:2*1024*1024,scenePixels:8*1024*1024,dprCap:1.35});

// The exact circular-filleted path/transform used by the field's hit clip, not a rounded rectangle.
export const FIELD_GLASS_HEX_PATH=roundedHexPath(1,.12);
export const FIELD_GLASS_HEX=OpticalGlass.shape(({context,width,height})=>{
  context.save();context.transform(0,height/2,-width/SQRT3,0,width/2,height/2);
  context.fillStyle='white';context.fill(new Path2D(FIELD_GLASS_HEX_PATH));context.restore();
},{bevel:.12,rounding:0,irregularity:0,radiusMax:1000000});

/** Real SDF draw ports, in screen × DPR pixels. World corner radii scale with the field camera. */
export function fieldGlassShape(mask:FieldPresentation,worldWidth:number,worldHeight:number):Record<string,unknown>{
  if(mask.shape==='hex')return FIELD_GLASS_HEX;
  return OpticalGlass.shape(({context,width,height})=>{
    context.save();context.fillStyle='white';context.beginPath();
    if(mask.shape==='rect'){
      const radius=Math.min(16,worldWidth/2,worldHeight/2);
      context.roundRect(0,0,width,height,{x:radius*width/worldWidth,y:radius*height/worldHeight});
    }else if(mask.shape==='ellipse')context.ellipse(width/2,height/2,width/2,height/2,0,0,Math.PI*2);
    else if(mask.shape==='polygon'){mask.points.forEach((point,index)=>{if(index===0)context.moveTo(point.x*width,point.y*height);else context.lineTo(point.x*width,point.y*height);});context.closePath();}
    context.fill();context.restore();
  },{bevel:.10,rounding:0,irregularity:0,radiusMax:1000000});
}
function maskFor(node:HTMLElement):FieldPresentation|null{
  const shape=node.dataset.fieldGlassNode;
  const points=node.dataset.fieldGlassPoints??'null';if(points.length>16384)return null;
  try{return validateFieldPresentation(shape==='polygon'?{shape,points:JSON.parse(points)}:{shape});}catch{return null;}
}

function backgroundFor(root:HTMLElement):string{
  const style=getComputedStyle(root),raw=style.backgroundColor;
  const base=raw&&raw!=='rgba(0, 0, 0, 0)'?raw:style.getPropertyValue('--sw-bg').trim()||'#f5f4f0';
  const color=base.replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;');
  const svg=`<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1000"><defs><radialGradient id="a"><stop stop-color="#9d8ed7" stop-opacity=".10"/><stop offset="1" stop-color="#9d8ed7" stop-opacity="0"/></radialGradient><radialGradient id="b"><stop stop-color="#8bcac5" stop-opacity=".08"/><stop offset="1" stop-color="#8bcac5" stop-opacity="0"/></radialGradient></defs><path fill="${color}" d="M0 0h1600v1000H0z"/><ellipse fill="url(#a)" cx="1230" cy="220" rx="700" ry="650"/><ellipse fill="url(#b)" cx="220" cy="900" rx="700" ry="650"/></svg>`;
  return `data:image/svg+xml,${encodeURIComponent(svg)}`;
}
function running(scene:OpticalScene|null):boolean{return !!(scene?.gl&&scene.running&&scene.initialized&&!scene.gl.isContextLost());}

/** One optical scene below live DOM. It never writes field data, gesture state or target geometry. */
export function mountFieldGlass(root:HTMLElement,options:FieldGlassOptions={}):FieldGlassHandle{
  const existing=mounted.get(root);if(existing)return existing;
  const document=root.ownerDocument,win=document.defaultView??window;
  const canvas=document.createElement('canvas');canvas.className='uf-glass-canvas';canvas.setAttribute('aria-hidden','true');
  const hadClass=root.classList.contains('uf-glass-root');
  const previous=['data-field-glass','data-field-glass-stage'].map(key=>root.getAttribute(key));
  root.classList.add('uf-glass-root');root.append(canvas);
  const forced=win.matchMedia('(forced-colors: active)'),reduced=win.matchMedia('(prefers-reduced-motion: reduce)');
  const app=root.closest<HTMLElement>('.sw-app');
  let disposed=false,failed=false,initializing=false,scene:OpticalScene|null=null,starting:Promise<unknown>|null=null;
  let generation=0,queued=false,frame=0,finishFrame:(()=>void)|null=null,background=options.background??'',backgroundKey='';
  let serial:Promise<unknown>=Promise.resolve(),status:FieldGlassStatus={mode:'fallback',surfaces:0},hasReported=false;
  let resolveReady!:(status:FieldGlassStatus)=>void,readySettled=false;
  const ready=new Promise<FieldGlassStatus>(resolve=>{resolveReady=resolve;});
  type Face={face:HTMLElement;element:OpticalElement;size:string;maskKey:string;priorSurface:string|null;priorClip:string};
  const faces=new Map<HTMLElement,Face>();
  const clearFace=(owner:OpticalScene|null,node:HTMLElement,entry:Face)=>{
    owner?.remove(entry.element);entry.face.remove();
    if(entry.priorSurface===null)node.removeAttribute('data-field-glass-surface');else node.setAttribute('data-field-glass-surface',entry.priorSurface);
    if(entry.priorClip)node.style.setProperty('--uf-optical-content-clip',entry.priorClip);else node.style.removeProperty('--uf-optical-content-clip');
  };
  const settle=()=>{if(!readySettled){readySettled=true;resolveReady(status);}};
  const report=(mode:FieldGlassStatus['mode'],reason?:string)=>{
    if(disposed)return;
    if(hasReported&&status.mode===mode&&status.surfaces===faces.size&&status.reason===reason)return;
    hasReported=true;
    status=reason?{mode,surfaces:faces.size,reason}:{mode,surfaces:faces.size};
    root.setAttribute('data-field-glass',mode);canvas.dataset.mode=mode;
    try{options.onStatus?.(status);}catch{/* A status consumer cannot take ownership of the canvas. */}
  };
  const nextPaint=()=>new Promise<void>(resolve=>{finishFrame=resolve;frame=win.requestAnimationFrame(()=>{frame=0;finishFrame=null;resolve();});});
  const release=()=>{
    const old=scene;scene=null;
    for(const [node,entry]of faces)clearFace(old,node,entry);faces.clear();
    old?.stop();
    // Upstream start() can finish after teardown. Destroy only after its late GL allocation settles.
    if(old){if(starting)void starting.then(()=>old.destroy(),()=>old.destroy());else old.destroy();}
  };
  const fallback=(reason:string,sticky=true)=>{
    if(disposed)return;
    generation++;failed=sticky;initializing=false;release();root.setAttribute('data-field-glass-stage','fallback');report('fallback',reason);settle();
  };
  const blocked=()=>options.disabled||forced.matches||reduced.matches||app?.dataset.motion==='off';
  let wasBlocked=!!blocked();
  const targets=()=>{
    const area=root.getBoundingClientRect(),dpr=Math.min(win.devicePixelRatio||1,FIELD_GLASS_BUDGET.dprCap);let pixels=0;
    return [...root.querySelectorAll<HTMLElement>('[data-field-glass-node]')].filter(node=>{
      if(!maskFor(node)||node.hidden||(node as HTMLButtonElement).disabled||node.getAttribute('aria-hidden')==='true'||node.dataset.muted==='true'||node.dataset.nativeUnavailable==='true')return false;
      const rect=node.getBoundingClientRect();
      if(!(rect.width>=.5&&rect.height>=.5&&rect.right>=area.left-80&&rect.left<=area.right+80&&rect.bottom>=area.top-80&&rect.top<=area.bottom+80))return false;
      const width=Math.ceil(rect.width*dpr),height=Math.ceil(rect.height*dpr),allocation=width*height;
      if(width>FIELD_GLASS_BUDGET.side||height>FIELD_GLASS_BUDGET.side||allocation>FIELD_GLASS_BUDGET.facePixels||pixels+allocation>FIELD_GLASS_BUDGET.scenePixels)return false;
      pixels+=allocation;return true;
    });
  };
  const sync=()=>{
    if(disposed||!scene)return;
    const owner=scene;let dirty=false;
    const visible=new Set<HTMLElement>();
    for(const node of targets()){
      let entry=faces.get(node);
      if(entry&&entry.face.parentElement!==node){clearFace(owner,node,entry);faces.delete(node);entry=undefined;dirty=true;}
      const style=getComputedStyle(node),size=`${style.width}|${style.height}`,worldWidth=parseFloat(style.width),worldHeight=parseFloat(style.height),mask=maskFor(node)!;
      if(!(Number.isFinite(worldWidth)&&Number.isFinite(worldHeight)&&worldWidth>0&&worldHeight>0))continue;
      visible.add(node);
      const maskKey=JSON.stringify(mask);
      const add=(face:HTMLElement)=>owner.add({target:face,preset:'quiet',shape:fieldGlassShape(mask,worldWidth,worldHeight),content:null,interaction:0,motion:STILL,
        optics:{ior:1.45,depth:25,refraction:.30,roughness:.014},style:{tint:[.90,.94,1],tintStrength:.06,matte:.025},surface:{texture:0,scale:1,grain:0,rear:.03}});
      if(!entry){
        const face=document.createElement('span');face.className='uf-glass-face';face.setAttribute('aria-hidden','true');face.setAttribute('data-optical-silhouette',mask.shape==='hex'?'pointy-hex':mask.shape);
        node.prepend(face);
        entry={face,element:add(face),size,maskKey,priorSurface:node.getAttribute('data-field-glass-surface'),priorClip:node.style.getPropertyValue('--uf-optical-content-clip')};faces.set(node,entry);dirty=true;
        node.setAttribute('data-field-glass-surface','true');node.style.setProperty('--uf-optical-content-clip',fieldPresentationClip(mask));
      }else if(entry.size!==size||entry.maskKey!==maskKey){
        owner.remove(entry.element);entry.element=add(entry.face);entry.size=size;entry.maskKey=maskKey;entry.face.setAttribute('data-optical-silhouette',mask.shape==='hex'?'pointy-hex':mask.shape);
        node.style.setProperty('--uf-optical-content-clip',fieldPresentationClip(mask));dirty=true;
      }
    }
    for(const [node,entry]of faces)if(!visible.has(node)){clearFace(owner,node,entry);faces.delete(node);dirty=true;}
    // Pan/zoom/selection never rebuild the SDF atlas; target membership, geometry or a real mask change does.
    if(dirty)owner.refresh();
    const desired=background||backgroundFor(root);
    if(owner.initialized&&desired!==backgroundKey){backgroundKey=desired;void owner.setBackground(desired).catch(()=>{if(scene===owner)fallback('background-unavailable');});}
    if(status.mode==='webgl')report('webgl');
  };
  const queue=()=>{if(queued||disposed)return;queued=true;queueMicrotask(()=>{queued=false;if(!disposed)try{if(!scene&&!initializing&&!failed&&!blocked()&&targets().length)start();else sync();}catch{fallback('surface-unavailable');}});};
  const start=()=>{
    const ticket=++generation;initializing=true;
    serial=serial.catch(()=>undefined).then(async()=>{
      if(disposed||ticket!==generation||blocked()||failed)return;
      root.setAttribute('data-field-glass-stage','waiting-for-paint');
      await nextPaint();if(disposed||ticket!==generation)return;
      await nextPaint();if(disposed||ticket!==generation)return;
      if(!targets().length){initializing=false;root.setAttribute('data-field-glass-stage','fallback');report('fallback','no-visible-surfaces');settle();return;}
      let candidate:OpticalScene|null=null;
      try{
        root.setAttribute('data-field-glass-stage','initializing');
        backgroundKey=background||backgroundFor(root);
        candidate=OpticalGlass.create({root,canvas,background:backgroundKey,dprCap:FIELD_GLASS_BUDGET.dprCap,fpsCap:60});scene=candidate;
        if(!candidate.gl){fallback('webgl2-unavailable');return;}
        sync();
        const startup=candidate.start();starting=startup;
        try{await startup;}finally{if(starting===startup)starting=null;}
        if(disposed||ticket!==generation){candidate.destroy();return;}
        if(!running(candidate)){fallback('webgl2-not-running');return;}
        sync();await nextPaint();
        if(disposed||ticket!==generation){candidate.destroy();return;}
        if(!running(candidate)||candidate.gl!.getError()!==candidate.gl!.NO_ERROR||candidate.getMetrics().opticalPasses<1){fallback('webgl-frame-invalid');return;}
        initializing=false;root.setAttribute('data-field-glass-stage','ready');report('webgl');settle();
      }catch{if(!disposed&&ticket===generation)fallback('initialization-failed');else candidate?.destroy();}
    });
  };
  const onPreference=()=>{wasBlocked=!!blocked();if(wasBlocked)fallback(forced.matches?'forced-colors':'reduced-motion',false);else{failed=false;start();}};
  const onLost=(event:Event)=>{event.preventDefault();fallback('webgl-context-lost');};
  canvas.addEventListener('webglcontextlost',onLost);
  forced.addEventListener('change',onPreference);reduced.addEventListener('change',onPreference);
  const observer=new MutationObserver(queue);observer.observe(root,{childList:true,subtree:true,attributes:true,attributeFilter:['style','data-field-glass-node','data-field-glass-points','data-native-unavailable','hidden','disabled','aria-hidden','data-muted']});
  const themeObserver=new MutationObserver(()=>{if(!!blocked()!==wasBlocked)onPreference();else queue();});
  themeObserver.observe(document.documentElement,{attributes:true,attributeFilter:['data-soty-theme','style']});
  if(app&&app!==document.documentElement)themeObserver.observe(app,{attributes:true,attributeFilter:['data-motion','style']});
  const resize=new ResizeObserver(queue);resize.observe(root);
  const handle:FieldGlassHandle={ready,
    refresh(next={}){if(disposed)return;if(next.background!==undefined)background=next.background;if(next.retry){failed=false;release();start();}else queue();},
    dispose(){
      if(disposed)return;disposed=true;generation++;observer.disconnect();themeObserver.disconnect();resize.disconnect();
      forced.removeEventListener('change',onPreference);reduced.removeEventListener('change',onPreference);canvas.removeEventListener('webglcontextlost',onLost);
      if(frame)win.cancelAnimationFrame(frame);finishFrame?.();finishFrame=null;frame=0;
      release();canvas.remove();if(!hadClass)root.classList.remove('uf-glass-root');
      ['data-field-glass','data-field-glass-stage'].forEach((key,index)=>{const prior=previous[index];if(prior===null||prior===undefined)root.removeAttribute(key);else root.setAttribute(key,prior);});
      mounted.delete(root);status={mode:'fallback',surfaces:0,reason:'disposed'};settle();
    },
  };
  mounted.set(root,handle);report('fallback');
  if(blocked()){report('fallback',options.disabled?'disabled':forced.matches?'forced-colors':'reduced-motion');settle();}else start();
  return handle;
}
