import {createGlassUI,GLASS_UI_WORKING_THEME,type GlassUIHandle,type GlassRenderPolicy,type GlassRenderStatus} from '../../vendor/optical-glass/src/ui-kit.js';
import {sotyGlassRole,SOTY_GLASS_SELECTOR} from './glass-roles.mjs';
import '../../vendor/optical-glass/src/ui-kit.css';
import './shell-glass.css';

export interface ShellGlassHandle {readonly renderMode:'static'|'optical';readonly renderStatus:GlassRenderStatus;ready:Promise<void>;dispose():void}
const owners=new WeakMap<HTMLElement,ShellGlassHandle>();
const selector=SOTY_GLASS_SELECTOR;
function background(dark:boolean,groundOverride=''):string{
 const ground=/^#[\da-f]{3,8}$/iu.test(groundOverride)?groundOverride:dark?'#101e26':'#eef3f2',first=dark?'#164b61':'#a1d5db',second=dark?'#3e365e':'#c3b8de';
 return 'data:image/svg+xml;charset=utf-8,'+encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="1400" height="1000"><defs><radialGradient id="a"><stop stop-color="${first}"/><stop offset="1" stop-color="${ground}"/></radialGradient><radialGradient id="b"><stop stop-color="${second}"/><stop offset="1" stop-color="${ground}" stop-opacity="0"/></radialGradient></defs><rect width="1400" height="1000" fill="${ground}"/><ellipse cx="1200" cy="180" rx="760" ry="680" fill="url(#a)"/><ellipse cx="80" cy="900" rx="700" ry="560" fill="url(#b)"/></svg>`);
}

/** Progressive material only. Native nodes/events/data/focus retain their owners. */
export function mountMaterialGlass(root:HTMLElement,{renderPolicy='static'}:{renderPolicy?:GlassRenderPolicy}={}):ShellGlassHandle{
 const existing=owners.get(root);if(existing)return existing;
 let disposed=false,ui:GlassUIHandle|null=null;
 let status:GlassRenderStatus=Object.freeze({policy:renderPolicy,mode:'static',reason:'initializing',gpuReady:false});
 const doc=root.ownerDocument,win=doc.defaultView!,themeRoot=doc.documentElement;
 const oldClass=root.classList.contains('glass-ui'),oldTone=root.getAttribute('data-glass-tone'),oldShell=root.getAttribute('data-shell-glass'),oldMaterial=root.getAttribute('data-glass-material');
 const tokens=new Map<string,{value:string;priority:string}>();
 const dark=()=>themeRoot.dataset.sotyTheme==='dark';
 const canvasBackground=()=>background(dark(),win.getComputedStyle(themeRoot).getPropertyValue('--sw-bg').trim());
 const tone=()=>{
  root.dataset.glassTone=dark()?'dark':'light';
  const palette=win.getComputedStyle(themeRoot);
  for(const[target,source,alpha]of[
   ['--glass-ui-ink','--sw-ink'],['--glass-ui-muted','--sw-muted'],
   ['--glass-ui-surface','--sw-surface','88%'],['--glass-ui-surface-strong','--sw-surface-raised','96%'],
  ]){
   const value=palette.getPropertyValue(source!).trim();
   if(value&&CSS.supports('color',value)){
    if(!tokens.has(target!))tokens.set(target!,{value:root.style.getPropertyValue(target!),priority:root.style.getPropertyPriority(target!)});
    // Preserve the existing brightness preference while the working material
    // owns its cool hue, accent and edge instead of inheriting legacy amber.
    const surface=alpha&&CSS.supports('color',`oklch(from ${value} l 0.012 260)`)?`oklch(from ${value} l 0.012 260)`:value;
    root.style.setProperty(target!,alpha?`color-mix(in srgb, ${surface} ${alpha}, transparent)`:value);
   }else if(tokens.has(target!)){const old=tokens.get(target!)!;old.value?root.style.setProperty(target!,old.value,old.priority):root.style.removeProperty(target!);}
  }
 };
 root.classList.add('glass-ui');root.dataset.glassMaterial='working';root.dataset.shellGlass='initializing';tone();
 const themes=new MutationObserver(()=>{tone();if(ui)void Promise.resolve(ui.setBackground(canvasBackground())).catch(()=>{});});themes.observe(themeRoot,{attributes:true,attributeFilter:['data-soty-theme','style']});
 const ready=createGlassUI({root,background:canvasBackground(),renderPolicy,theme:GLASS_UI_WORKING_THEME,dprCap:1.5,fpsCap:30,autoDestroy:false,
  roles:{navigation:{preset:'ui-navigation',hitArea:'container',options:{motion:{enabled:false},interaction:0}},overlay:{preset:'ui-panel',hitArea:'container',options:{motion:{enabled:false},interaction:0}}},
  onRenderStatus:next=>{status=next;if(!disposed){root.dataset.shellGlass=next.gpuReady?'optical':'static';}}
 }).then(value=>{if(disposed){value.destroy();return;}ui=value;root.dataset.shellGlass=value.renderMode;}).catch(()=>{if(!disposed)root.dataset.shellGlass='unavailable';});
 const handle:ShellGlassHandle={ready,get renderMode(){return status.mode;},get renderStatus(){return status;},dispose(){if(disposed)return;disposed=true;themes.disconnect();ui?.destroy();
  for(const[name,old]of tokens)old.value?root.style.setProperty(name,old.value,old.priority):root.style.removeProperty(name);
  if(!oldClass)root.classList.remove('glass-ui');oldTone===null?root.removeAttribute('data-glass-tone'):root.setAttribute('data-glass-tone',oldTone);oldShell===null?root.removeAttribute('data-shell-glass'):root.setAttribute('data-shell-glass',oldShell);oldMaterial===null?root.removeAttribute('data-glass-material'):root.setAttribute('data-glass-material',oldMaterial);owners.delete(root);
 }};owners.set(root,handle);return handle;
}

/** Compatibility for existing roots whose producers have not declared roles yet. */
export function mountShellGlass(root:HTMLElement,options:{renderPolicy?:GlassRenderPolicy}={}):ShellGlassHandle{
 const existing=owners.get(root);if(existing)return existing;
 const roles=new Map<Element,string>(),ignores=new Map<Element,string|null>(),safeAreas=new Map<Element,string|null>();
 const win=root.ownerDocument.defaultView!;let disposed=false,frame=0;
 const releaseRole=(node:Element,assigned:string)=>{if(node.getAttribute('data-glass-role')===assigned)node.removeAttribute('data-glass-role');};
 const releaseSafeArea=(node:Element,previous:string|null)=>{if(node.getAttribute('data-glass-safe-area')==='off')previous===null?node.removeAttribute('data-glass-safe-area'):node.setAttribute('data-glass-safe-area',previous);};
 const assign=(node:Element)=>{
  if(node.closest('[data-glass-ignore],.uf-plane,.sa-runtime,.sa-frame'))return;
  const current=node.getAttribute('data-glass-role'),assigned=roles.get(node);
  // A producer's explicit role wins, including a later handoff from inference.
  if(assigned!==undefined&&current!==assigned){roles.delete(node);return;}
  if(assigned===undefined&&current!==null)return;
  const role=sotyGlassRole(node);
  if(!role){
   if(assigned!==undefined){releaseRole(node,assigned);roles.delete(node);}
   if(safeAreas.has(node)){releaseSafeArea(node,safeAreas.get(node)!);safeAreas.delete(node);}
   return;
  }
  roles.set(node,role);
  if(!node.hasAttribute('data-glass-safe-area')){safeAreas.set(node,null);node.setAttribute('data-glass-safe-area','off');}
  if(current!==role)node.setAttribute('data-glass-role',role);
 };
 const scan=()=>{
  if(disposed)return;
  for(const node of root.querySelectorAll('.uf-plane,.sa-runtime,.sa-frame')){if(!ignores.has(node))ignores.set(node,node.getAttribute('data-glass-ignore'));node.setAttribute('data-glass-ignore','');}
  assign(root);for(const node of root.querySelectorAll(selector))assign(node);
  for(const[node,assigned]of roles)if(!root.contains(node)&&node!==root){releaseRole(node,assigned);roles.delete(node);}
  for(const[node,previous]of safeAreas)if(!root.contains(node)&&node!==root){releaseSafeArea(node,previous);safeAreas.delete(node);}
  for(const[node,previous]of ignores)if(!root.contains(node)){previous===null?node.removeAttribute('data-glass-ignore'):node.setAttribute('data-glass-ignore',previous);ignores.delete(node);}
 };
 const queue=()=>{if(!disposed&&!frame)frame=win.requestAnimationFrame(()=>{frame=0;scan();});};
 scan();
 const observer=new MutationObserver(queue);observer.observe(root,{subtree:true,childList:true,attributes:true,attributeFilter:['class','type','role']});
 const handle=mountMaterialGlass(root,options),disposeMaterial=handle.dispose;
 handle.dispose=()=>{if(disposed)return;disposed=true;observer.disconnect();if(frame)win.cancelAnimationFrame(frame);disposeMaterial();
  for(const[node,assigned]of roles)releaseRole(node,assigned);
  for(const[node,previous]of safeAreas)releaseSafeArea(node,previous);
  for(const[node,previous]of ignores)previous===null?node.removeAttribute('data-glass-ignore'):node.setAttribute('data-glass-ignore',previous);
 };
 return handle;
}

/** Real browser material enhancement; VM semantic tests have no native canvas. */
export function mountDialogGlass(dialog:HTMLDialogElement,renderPolicy:GlassRenderPolicy='auto'):ShellGlassHandle|null{
 if(!dialog.ownerDocument?.defaultView?.HTMLCanvasElement)return null;
 return mountShellGlass(dialog,{renderPolicy});
}
