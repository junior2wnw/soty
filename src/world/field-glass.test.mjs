import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import ts from 'typescript';
import {roundedHexPath,SQRT3} from '../geometry/hex.mjs';
import {validateFieldPresentation,fieldPresentationClip,fieldPointInPolygon} from './unified-field-presentation.mjs';

const source=readFileSync(new URL('./field-glass.ts',import.meta.url),'utf8');
const css=readFileSync(new URL('./field-glass.css',import.meta.url),'utf8');
const flush=async()=>{for(let i=0;i<12;i++)await Promise.resolve();};
const deferred=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return{promise,resolve,reject};};
function compile(code=source){
  const result=ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext},reportDiagnostics:true});
  assert.equal(result.diagnostics.length,0);
  return result.outputText.replace(/^import[^\n]*\n/gm,'').replace(/^export /gm,'')+'\nexports.mount=mountFieldGlass;exports.hex=FIELD_GLASS_HEX;exports.shape=fieldGlassShape;';
}
function harness({code=source,noGL=false,invalidFrame=false,notRunning=false,noOpticalFrame=false,deferStart=false,forced=false,reduced=false,initialOwnership=false}={}){
  const scenes=[],frames=new Map(),mutations=[],resizes=[],statuses=[],startGate=deferred();let nextFrame=0;
  class Node{
    constructor(tag='div'){this.tagName=tag;this.children=[];this.parentElement=null;this.attributes=new Map();this.listeners=new Map();this.className='';this.style={setProperty(key,value){this[key]=String(value);},getPropertyValue(key){return this[key]??'';},removeProperty(key){const prior=this[key]??'';delete this[key];return prior;}};this.world={width:208,height:240};this.rect={left:100,top:100,width:208,height:240};
      this.dataset=new Proxy({},{get:(_,key)=>this.getAttribute('data-'+String(key).replace(/[A-Z]/g,x=>'-'+x.toLowerCase()))??undefined,set:(_,key,value)=>{this.setAttribute('data-'+String(key).replace(/[A-Z]/g,x=>'-'+x.toLowerCase()),value);return true;}});
      this.classList={contains:name=>this.className.split(/\s+/).includes(name),add:name=>{if(!this.classList.contains(name))this.className+=' '+name;},remove:name=>{this.className=this.className.split(/\s+/).filter(x=>x!==name).join(' ');}};
    }
    setAttribute(key,value){this.attributes.set(key,String(value));}getAttribute(key){return this.attributes.get(key)??null;}removeAttribute(key){this.attributes.delete(key);}
    append(...nodes){for(const node of nodes){node.remove();node.parentElement=this;this.children.push(node);}}prepend(node){node.remove();node.parentElement=this;this.children.unshift(node);}
    remove(){if(this.parentElement)this.parentElement.children=this.parentElement.children.filter(n=>n!==this);this.parentElement=null;}
    replaceChildren(...nodes){for(const node of [...this.children])node.remove();this.append(...nodes);}
    contains(node){return node===this||this.children.some(child=>child.contains(node));}
    closest(selector){if(selector==='.sw-app'){for(let node=this;node;node=node.parentElement)if(node.classList.contains('sw-app'))return node;}return null;}
    querySelectorAll(){return this.children.flatMap(child=>[...(child.dataset.fieldGlassNode!==undefined?[child]:[]),...child.querySelectorAll()]);}
    getBoundingClientRect(){const r=this.className==='uf-glass-face'?this.parentElement.rect:this.rect;return{...r,right:r.left+r.width,bottom:r.top+r.height};}
    addEventListener(type,handler){if(!this.listeners.has(type))this.listeners.set(type,new Set());this.listeners.get(type).add(handler);}removeEventListener(type,handler){this.listeners.get(type)?.delete(handler);}
    emit(type,event={}){for(const handler of this.listeners.get(type)||[])handler(event);}
  }
  class Media extends Node{constructor(matches){super();this.matches=matches;}change(matches){this.matches=matches;this.emit('change');}}
  const contrast=new Media(forced),motion=new Media(reduced),media=query=>query.includes('forced-colors')?contrast:motion;
  class Observer{constructor(callback){this.callback=callback;this.targets=[];this.active=true;mutations.push(this);}observe(target){this.targets.push(target);}disconnect(){this.active=false;this.targets=[];}}
  class Resize{constructor(callback){this.callback=callback;this.active=true;resizes.push(this);}observe(){}disconnect(){this.active=false;}}
  const win={matchMedia:media,requestAnimationFrame:callback=>{frames.set(++nextFrame,callback);return nextFrame;},cancelAnimationFrame:id=>frames.delete(id)};
  const document={defaultView:win,documentElement:new Node('html'),createElement(tag){const node=new Node(tag);node.ownerDocument=document;return node;}};
  const app=new Node();app.className='sw-app';app.ownerDocument=document;
  const root=new Node();root.ownerDocument=document;root.rect={left:0,top:0,width:1000,height:700};root.bg='rgb(245, 244, 240)';app.append(root);
  if(initialOwnership){root.className='uf-glass-root';root.setAttribute('data-field-glass','previous');root.setAttribute('data-field-glass-stage','previous-stage');}
  const target=new Node('button');target.ownerDocument=document;target.dataset.fieldGlassNode='hex';target.style.left='untouched';root.append(target);
  const OpticalGlass={shape:(draw,options)=>({...options,mask:{draw}}),create(options){
    const scene={gl:noGL?null:{NO_ERROR:0,getError:()=>invalidFrame?1:0,isContextLost:()=>false},running:false,initialized:false,destroyed:false,elements:[],specifications:[],refreshes:0,backgrounds:[],allocations:0,released:0,destroys:0,options,
      add(spec){const element={target:spec.target};this.specifications.push(spec);this.elements.push(element);return element;},
      remove(element){this.elements=this.elements.filter(x=>x!==element);return true;},refresh(){this.refreshes++;},
      getMetrics(){return{opticalPasses:noOpticalFrame?0:1,shadedPixels:noOpticalFrame?0:100};},
      async start(){if(deferStart)await startGate.promise;this.allocations++;this.running=!notRunning;this.initialized=!notRunning;return this;},
      stop(){this.running=false;return this;},async setBackground(value){this.backgrounds.push(value);return this;},
      destroy(){if(this.destroyed)return this;this.destroyed=true;this.running=false;this.released+=this.allocations;this.destroys++;return this;}};
    scenes.push(scene);return scene;
  }};
  // A VM is a different JS realm; transfer DTOs at this import port, preserving the strict production validator.
  const context=vm.createContext({exports:{},OpticalGlass,roundedHexPath,SQRT3,validateFieldPresentation:value=>validateFieldPresentation(structuredClone(value)),fieldPresentationClip,MutationObserver:Observer,ResizeObserver:Resize,queueMicrotask,window:win,
    getComputedStyle:node=>({backgroundColor:node.bg??'rgba(0, 0, 0, 0)',width:`${node.world.width}px`,height:`${node.world.height}px`,getPropertyValue:key=>key==='--sw-bg'?root.bg:''}),
    Path2D:class{constructor(path){this.path=path;}},encodeURIComponent});
  vm.runInContext(compile(code),context);
  const handle=context.exports.mount(root,{onStatus:status=>statuses.push({...status})});
  return{root,target,app,document,handle,scenes,frames,mutations,resizes,statuses,contrast,motion,api:context.exports,
    async frame(){await flush();const jobs=[...frames.values()];frames.clear();for(const callback of jobs)callback();await flush();},
    async finish(){for(let i=0;i<4;i++)await this.frame();return handle.ready;},
    async mutate(){mutations[0].callback([]);await flush();},async theme(){mutations[1].callback([]);await flush();},
    async resize(){resizes[0].callback([]);await flush();},resolveStart:()=>startGate.resolve(),
  };
}

test('pinned upstream provenance and explicit local SDF patch match verified SHA256; no fabricated license or publication',()=>{
  const base=new URL('../../vendor/optical-glass/',import.meta.url),provenance=JSON.parse(readFileSync(new URL('PROVENANCE.json',base),'utf8'));
  assert.equal(provenance.commit,'a8aa5d1389f5c86e5d1129912fa92daa8d0229de');assert.equal(provenance.version,'1.4.0');
  assert.equal(provenance.localRevision,'soty-core-3');assert.equal(provenance.files[0].upstreamSha256,'57760bc88cd0ad6c3cf5abfcbfeb2bc72e847cd8154d0b1c22951423da545b4e');
  assert.equal(provenance.patches[0].baseSha256,provenance.files[0].upstreamSha256);assert.equal(provenance.patches[1].baseSha256,provenance.patches[0].resultSha256);
  assert.equal(provenance.patches[1].resultSha256,'98631ec01ae4bc8f8cbc7c4037b34ae8472b800b50ba647854e967f13957bdda');
  assert.equal(provenance.patches[2].id,'glass-native-visibility-1');assert.equal(provenance.patches[2].baseSha256,provenance.patches[1].resultSha256);
  assert.equal(provenance.patches[2].resultSha256,provenance.files[0].sha256);assert.equal(provenance.files[0].sha256,'8304ee1472a1a056f3c8df319c3a5334bf66da8e91ab4128399da6c99e09ab7c');
  for(const file of provenance.files)assert.equal(createHash('sha256').update(readFileSync(new URL(file.path,base))).digest('hex'),file.sha256,file.path);
  const css=provenance.files.find(file=>file.path==='src/ui-kit.css'),doc=provenance.files.find(file=>file.path==='docs/UI_KIT.md');
  assert.equal(provenance.styleRevision,'soty-native-pointer-boundary-1');assert.equal(provenance.cssPatches.length,1);
  const pointer=provenance.cssPatches[0];assert.equal(pointer.id,'glass-native-pointer-boundary-1');assert.equal(pointer.path,css.path);
  assert.equal(pointer.baseSha256,provenance.patches[4].resultSha256);assert.equal(pointer.resultSha256,css.sha256);
  assert.equal(pointer.baseBytes,39355);assert.equal(pointer.resultBytes,css.bytes);assert.equal(pointer.resultBytes-pointer.baseBytes,268);
  assert.equal(css.bytes,39623);assert.equal(css.sha256,'6a58c8f58e2d9cea8f71b60e78b3d8c24355732cfafcec7eef5d046d06144d23');
  assert.equal(readFileSync(new URL(css.path,base)).length,css.bytes);
  assert.equal(pointer.documentation.baseSha256,provenance.uiLayerPatch.files[doc.path]);assert.equal(pointer.documentation.resultSha256,doc.sha256);
  assert.equal(pointer.documentation.baseBytes,39598);assert.equal(pointer.documentation.resultBytes,doc.bytes);assert.equal(readFileSync(new URL(doc.path,base)).length,doc.bytes);
  assert.equal(pointer.evidence.oracleSha256,'5ba3337094146f606d19d8dab68fbb1f7a2e0d600d694d87cd879972f802ae82');
  assert.deepEqual([pointer.evidence.oldBrowser.tests,pointer.evidence.oldBrowser.failed,pointer.evidence.oldBrowser.skipped],[2,2,0]);
  assert.deepEqual([pointer.evidence.actualBrowser.tests,pointer.evidence.actualBrowser.passed,pointer.evidence.actualBrowser.failed,pointer.evidence.actualBrowser.skipped],[8,8,0,0]);
  assert.match(readFileSync(new URL('../../.gitattributes',import.meta.url),'utf8'),/^\/vendor\/optical-glass\/src\/ui-kit\.css -text$/m);
  const descriptor=JSON.parse(readFileSync(new URL('module-descriptor.json',base),'utf8'));assert.equal(descriptor.license,null);assert.equal(descriptor.publicationPerformed,false);assert.equal(descriptor.frameworkDependencies.length,0);
});

test('GPU construction waits for two paints; first verified frame enables one shared scene without moving native content',async()=>{
  const f=harness();assert.equal(f.scenes.length,0);assert.equal(f.root.dataset.fieldGlass,'fallback');
  await f.frame();assert.equal(f.scenes.length,0);await f.frame();assert.equal(f.scenes.length,1);assert.equal(f.root.dataset.fieldGlass,'fallback');
  await f.frame();const state=await f.handle.ready;assert.equal(state.mode,'webgl');assert.equal(state.surfaces,1);
  const scene=f.scenes[0],spec=scene.specifications[0];
  assert.notEqual(spec.target,f.target);assert.equal(spec.target.parentElement,f.target);assert.equal(spec.content,null);assert.equal(spec.motion.enabled,false);assert.equal(spec.interaction,0);
  assert.equal(f.target.style.left,'untouched');assert.equal(typeof spec.shape.mask.draw,'function');assert.equal(spec.shape.rounding,0);
  assert.equal(f.api.mount(f.root),f.handle);assert.equal(f.scenes.length,1);f.handle.dispose();
});

test('the optical mask uses the same circular-filleted path and pointy transform at every screen scale',async()=>{
  const f=harness();await f.finish();const draw=f.scenes[0].specifications[0].shape.mask.draw;
  for(const scale of [.2,.7,1,2]){
    const width=208*scale,height=240*scale,calls=[];
    const context={save(){},restore(){},transform(...matrix){calls.push(matrix);},fill(path){calls.push(path.path);}};
    draw({context,width,height});assert.equal(calls[1],roundedHexPath(1,.12));
    const[a,b,c,d,e,g]=calls[0];assert.equal(e,width/2);assert.equal(g-b,0,'source left vertex is the top tip');assert.equal(g+b,height,'source right vertex is the bottom tip');
    assert.ok(Math.abs(c*(SQRT3/2)+e)<1e-8);assert.equal(a,0);assert.equal(d,0);
  }
  f.handle.dispose();
});

function drawn(shape,width,height){
  const calls=[];const context={save(){},restore(){},transform(...matrix){calls.push(['transform',...matrix]);},beginPath(){calls.push(['begin']);},roundRect(...args){calls.push(['rect',...args]);},ellipse(...args){calls.push(['ellipse',...args]);},moveTo(x,y){calls.push(['move',x,y]);},lineTo(x,y){calls.push(['line',x,y]);},closePath(){calls.push(['close']);},fill(path){calls.push(['fill',path?.path??null]);}};
  shape.mask.draw({context,width,height,dpr:1});return calls;
}
test('rect SDF follows the real 16-world-pixel corner radius through free resize, screen scaling and DPR',async()=>{
  const f=harness();f.target.dataset.fieldGlassNode='rect';f.target.world={width:640,height:360};f.target.rect={left:20,top:30,width:320,height:180};await f.finish();
  const spec=f.scenes[0].specifications[0];assert.equal(spec.shape.rounding,0);assert.equal(f.scenes[0].elements.length,1);
  for(const scale of [.25,.5,1,2]){const width=640*scale,height=360*scale,call=drawn(spec.shape,width,height).find(value=>value[0]==='rect');assert.equal(call[3],width);assert.equal(call[4],height);assert.equal(call[5].x,16*scale);assert.equal(call[5].y,16*scale);}
  const small=f.api.shape({shape:'rect'},100,16),radius=drawn(small,50,8).find(value=>value[0]==='rect')[5];assert.equal(radius.x,4);assert.equal(radius.y,4);
  assert.equal(spec.content,null);assert.equal(spec.target.getAttribute('data-optical-silhouette'),'rect');assert.equal(f.target.dataset.fieldGlassSurface,'true');f.handle.dispose();
});
test('ellipse and concave polygon use their actual masks rather than an underlying rectangular or hex optical face',async()=>{
  const f=harness();f.target.dataset.fieldGlassNode='ellipse';await f.finish();let spec=f.scenes[0].specifications.at(-1);
  const ellipse=drawn(spec.shape,320,160).find(value=>value[0]==='ellipse');assert.deepEqual(ellipse.slice(1,5),[160,80,160,80]);
  const points=[{x:0,y:0},{x:1,y:0},{x:.35,y:.35},{x:0,y:1}];f.target.dataset.fieldGlassNode='polygon';f.target.dataset.fieldGlassPoints=JSON.stringify(points);await f.mutate();spec=f.scenes[0].specifications.at(-1);
  const polygon=drawn(spec.shape,200,100).filter(value=>['move','line'].includes(value[0])).map(value=>({x:value[1],y:value[2]}));assert.deepEqual(polygon,points.map(point=>({x:point.x*200,y:point.y*100})));
  assert.equal(fieldPointInPolygon({x:180,y:90},polygon),false);assert.equal(fieldPointInPolygon({x:10,y:10},polygon),true);assert.equal(f.scenes.length,1);assert.equal(f.scenes[0].elements.length,1);f.handle.dispose();
});
test('only real mask/layout changes refresh the surface; geometry scaling and decorative attributes leave the mounted content untouched',async()=>{
  const f=harness();f.target.dataset.fieldGlassNode='rect';const content=f.document.createElement('article');content.textContent='Нативная записка';f.target.append(content);await f.finish();
  const scene=f.scenes[0],face=scene.elements[0].target,refreshes=scene.refreshes;
  f.target.rect={left:230,top:190,width:416,height:480};await f.mutate();assert.equal(scene.refreshes,refreshes);assert.equal(f.target.children.includes(content),true);assert.equal(scene.elements[0].target,face);
  f.target.world={width:480,height:270};await f.resize();assert.equal(scene.refreshes,refreshes+1);assert.equal(scene.elements[0].target,face);
  f.target.dataset.fieldGlassNode='ellipse';await f.mutate();assert.equal(scene.refreshes,refreshes+2);assert.equal(scene.elements[0].target,face);assert.equal(f.target.children.includes(content),true);f.handle.dispose();assert.deepEqual(f.target.children,[content]);
});
test('malformed/degenerate polygons, unavailable source and invalid world geometry remove optics and keep an honest DOM fallback',async()=>{
  const f=harness();f.target.dataset.fieldGlassNode='rect';await f.finish();const scene=f.scenes[0];
  f.target.dataset.fieldGlassNode='polygon';f.target.dataset.fieldGlassPoints=JSON.stringify([{x:0,y:0},{x:0,y:0},{x:1,y:1}]);await f.mutate();assert.equal(scene.elements.length,0);assert.equal(f.target.dataset.fieldGlassSurface,undefined);
  f.target.dataset.fieldGlassNode='ellipse';await f.mutate();assert.equal(scene.elements.length,1);
  f.target.dataset.nativeUnavailable='true';await f.mutate();assert.equal(scene.elements.length,0);
  f.target.dataset.nativeUnavailable='false';await f.mutate();assert.equal(scene.elements.length,1);
  f.target.world.width=NaN;await f.resize();assert.equal(scene.elements.length,0);assert.equal(f.target.dataset.fieldGlassSurface,undefined);f.handle.dispose();
});
test('surface attributes and safe clip CSS ownership are restored after context loss/dispose',async()=>{
  const f=harness();f.target.dataset.fieldGlassNode='ellipse';f.target.setAttribute('data-field-glass-surface','earlier');f.target.style.setProperty('--uf-optical-content-clip','inset(2px)');await f.finish();
  assert.equal(f.target.dataset.fieldGlassSurface,'true');assert.equal(f.target.style.getPropertyValue('--uf-optical-content-clip'),'ellipse(50% 50% at 50% 50%)');
  f.handle.dispose();assert.equal(f.target.dataset.fieldGlassSurface,'earlier');assert.equal(f.target.style.getPropertyValue('--uf-optical-content-clip'),'inset(2px)');
});
test('negative control catches the original hex-only target query and a false optical shape under an ellipse mask',async()=>{
  const oldQuery=source.replace("root.querySelectorAll<HTMLElement>('[data-field-glass-node]')","root.querySelectorAll<HTMLElement>('[data-field-glass-node=\"hex\"]')");assert.notEqual(oldQuery,source);
  const f=harness({code:oldQuery});f.target.dataset.fieldGlassNode='rect';
  // The DOM port honours the production selector for this control, as an actual browser would.
  const query=f.root.querySelectorAll.bind(f.root);f.root.querySelectorAll=selector=>query(selector).filter(node=>!selector.includes('="hex"')||node.dataset.fieldGlassNode==='hex');
  await f.finish();assert.throws(()=>assert.equal(f.scenes.length,1));f.handle.dispose();
  const wrong=source.replace('shape:fieldGlassShape(mask,worldWidth,worldHeight)','shape:FIELD_GLASS_HEX');assert.notEqual(wrong,source);
  const ellipse=harness({code:wrong});ellipse.target.dataset.fieldGlassNode='ellipse';await ellipse.finish();
  assert.throws(()=>assert.ok(drawn(ellipse.scenes[0].specifications[0].shape,200,100).some(value=>value[0]==='ellipse')));ellipse.handle.dispose();
});
test('valid extreme field geometry never forces unbounded SDF raster allocation; normal resized cards recover optics',async()=>{
  const f=harness();f.target.dataset.fieldGlassNode='rect';f.target.world={width:8192,height:8192};f.target.rect={left:0,top:0,width:16384,height:16384};
  await f.finish();assert.equal(f.scenes.length,0);assert.equal(f.target.dataset.fieldGlassSurface,undefined);assert.equal(f.root.dataset.fieldGlass,'fallback');
  f.target.world={width:640,height:360};f.target.rect={left:20,top:20,width:640,height:360};await f.mutate();await f.finish();assert.equal(f.scenes.length,1);assert.equal(f.target.dataset.fieldGlassSurface,'true');
  f.target.rect={left:0,top:0,width:8192,height:8192};await f.mutate();assert.equal(f.scenes[0].elements.length,0);assert.equal(f.target.dataset.fieldGlassSurface,undefined);f.handle.dispose();
});
test('negative control proves an omitted optical allocation bound would admit the extreme free widget',async()=>{
  const needle='if(width>FIELD_GLASS_BUDGET.side||height>FIELD_GLASS_BUDGET.side||allocation>FIELD_GLASS_BUDGET.facePixels||pixels+allocation>FIELD_GLASS_BUDGET.scenePixels)return false;';
  const broken=source.replace(needle,'if(false)return false;');assert.notEqual(broken,source);
  const f=harness({code:broken});f.target.dataset.fieldGlassNode='rect';f.target.rect={left:0,top:0,width:16384,height:16384};await f.finish();
  assert.throws(()=>assert.equal(f.scenes.length,0));f.handle.dispose();
});
test('verified native optics are not hidden under opaque white cards; fallback and pointer ownership remain unchanged',()=>{
  const check=code=>{
    assert.match(code,/\.uf-glass-root\[data-field-glass=webgl\] \[data-field-glass-surface=true\]>\.uf-object-mask\{background:transparent\}/u);
    assert.match(code,/\.uf-glass-root\[data-field-glass=webgl\] \[data-field-glass-surface=true\] \.nf-object\{[^}]*color:var\(--sw-ink\);background:color-mix\(in srgb,var\(--uf-native-tint\) 16%,transparent\)/u);
    assert.doesNotMatch(code,/backdrop-filter|filter:blur/u);
  };
  check(css);assert.throws(()=>check(css.replace('background:color-mix(in srgb,var(--uf-native-tint) 16%,transparent)','background:#fff')));
  assert.throws(()=>check(css.replaceAll('[data-field-glass=webgl]','[data-field-glass]')));
  const engine=readFileSync(new URL('./unified-field.ts',import.meta.url),'utf8');assert.match(engine,/fieldGlassPoints=JSON\.stringify\(presentation\.points\)/u);
  const parsed=ts.createSourceFile('unified-field.ts',engine,ts.ScriptTarget.Latest,true);let marker;
  const visit=node=>{if(ts.isIfStatement(node)&&node.expression.getText(parsed)==='!blockedObject'&&node.thenStatement.getText(parsed).includes('target.dataset.fieldGlassNode'))marker=node.getText(parsed);ts.forEachChild(node,visit);};visit(parsed);assert.ok(marker);
  const markerOracle=code=>{
    const assign=new Function('target','presentation','focusFrame','blockedObject',ts.transpileModule(code,{compilerOptions:{target:ts.ScriptTarget.ES2022}}).outputText);
    for(const shape of ['hex','rect','ellipse','polygon']){
      const target={dataset:{}},presentation={shape,points:[{x:0,y:0},{x:1,y:0},{x:0,y:1}]};
      assign(target,presentation,false,false);assert.equal(target.dataset.fieldGlassNode,shape,'An ordinary object keeps its actual presentation mask');
      if(shape==='polygon')assert.equal(target.dataset.fieldGlassPoints,JSON.stringify(presentation.points));
      assign(target,presentation,true,false);assert.equal(target.dataset.fieldGlassNode,'rect','Temporary readable focus has the actual rectangular viewport mask');
      assign(target,presentation,false,true);assert.equal(target.dataset.fieldGlassNode,undefined);assert.equal(target.dataset.fieldGlassPoints,undefined);
    }
  };
  markerOracle(marker);
  const broken=marker.replace("focusFrame?'rect':presentation.shape","'rect'");assert.notEqual(broken,marker);
  assert.throws(()=>markerOracle(broken),/ordinary object keeps/u);
});

test('pan, zoom and selection changes do not rebuild the SDF atlas; layout resize and new DOM targets do',async()=>{
  const f=harness();await f.finish();const scene=f.scenes[0],refreshes=scene.refreshes,reports=f.statuses.length;
  f.target.rect={left:130,top:80,width:416,height:480};await f.mutate();assert.equal(scene.refreshes,refreshes);
  assert.equal(f.statuses.length,reports,'camera transforms cannot trigger redundant host state updates');
  f.target.world.width=240;await f.resize();assert.equal(scene.refreshes,refreshes+1);
  f.target.replaceChildren();await f.mutate();assert.equal(scene.elements.length,1);assert.equal(scene.refreshes,refreshes+2);
  f.target.remove();await f.mutate();assert.equal(scene.elements.length,0);f.handle.dispose();
});

test('muted, disabled and hidden objects cannot leave a bright optical surface after their DOM presentation is suppressed',async()=>{
  const f=harness();await f.finish();const scene=f.scenes[0];
  for(const attribute of ['data-muted','aria-hidden','hidden','disabled']){
    if(attribute==='hidden'||attribute==='disabled')f.target[attribute]=true;else f.target.setAttribute(attribute,'true');
    await f.mutate();assert.equal(scene.elements.length,0,attribute);
    if(attribute==='hidden'||attribute==='disabled')f.target[attribute]=false;else f.target.removeAttribute(attribute);
    await f.mutate();assert.equal(scene.elements.length,1,attribute);
  }
  f.handle.dispose();
});

test('light/dark background changes update the existing scene, and runtime motion-off releases optics',async()=>{
  const f=harness();await f.finish();const scene=f.scenes[0];f.root.bg='rgb(16, 17, 18)';await f.theme();
  assert.equal(f.scenes.length,1);assert.equal(scene.backgrounds.length,1);assert.match(decodeURIComponent(scene.backgrounds[0]),/rgb\(16, 17, 18\)/);
  f.app.dataset.motion='off';await f.theme();assert.equal(f.root.dataset.fieldGlass,'fallback');assert.equal(scene.destroyed,true);
  f.app.dataset.motion='on';await f.theme();await f.finish();assert.equal(f.scenes.length,2);assert.equal(f.root.dataset.fieldGlass,'webgl');f.handle.dispose();
});

for(const preference of ['forced','reduced'])test(`${preference} preference uses an honest static fallback without constructing WebGL`,async()=>{
  const f=harness({[preference]:true});assert.equal((await f.handle.ready).mode,'fallback');await f.finish();assert.equal(f.scenes.length,0);f.handle.dispose();
});

test('unsupported, incomplete and failed GL frames never claim optical success',async()=>{
  for(const config of [{noGL:true},{notRunning:true},{invalidFrame:true},{noOpticalFrame:true}]){
    const f=harness(config);const result=await f.finish();assert.equal(result.mode,'fallback');assert.equal(f.root.dataset.fieldGlass,'fallback');assert.equal(f.scenes[0].destroyed,true);f.handle.dispose();
  }
});

test('an empty field avoids constructing WebGL and admits a later visible target without remounting',async()=>{
  const f=harness();f.target.remove();const initial=await f.finish();assert.equal(initial.mode,'fallback');assert.equal(f.scenes.length,0);
  f.root.append(f.target);await f.mutate();await f.finish();assert.equal(f.scenes.length,1);assert.equal(f.root.dataset.fieldGlass,'webgl');f.handle.dispose();
});

test('context loss releases all surfaces, and disposal restores original root ownership with no observers/listeners/frames',async()=>{
  const f=harness();await f.finish();let prevented=false;const canvas=f.root.children.find(n=>n.tagName==='canvas');
  canvas.emit('webglcontextlost',{preventDefault(){prevented=true;}});assert.equal(prevented,true);assert.equal(f.root.dataset.fieldGlass,'fallback');assert.equal(f.target.children.length,0);
  f.handle.dispose();f.handle.dispose();assert.equal(f.root.getAttribute('data-field-glass'),null);assert.equal(f.root.classList.contains('uf-glass-root'),false);
  assert.equal(f.root.children.filter(n=>n.tagName==='canvas').length,0);assert.equal(f.frames.size,0);assert.ok(f.mutations.every(o=>!o.active));assert.ok(f.resizes.every(o=>!o.active));
  assert.equal([...f.contrast.listeners.values()].reduce((n,set)=>n+set.size,0),0);
});

test('pre-existing root attributes and class ownership survive adapter teardown',async()=>{
  const f=harness({initialOwnership:true});await f.finish();f.handle.dispose();
  assert.equal(f.root.dataset.fieldGlass,'previous');assert.equal(f.root.dataset.fieldGlassStage,'previous-stage');assert.equal(f.root.classList.contains('uf-glass-root'),true);
});

async function lateDisposal(code=source){
  const f=harness({code,deferStart:true});await f.frame();await f.frame();assert.equal(f.scenes.length,1);
  f.handle.dispose();assert.equal((await f.handle.ready).reason,'disposed');f.resolveStart();await flush();
  const scene=f.scenes[0];assert.equal(scene.destroyed,true);assert.equal(scene.allocations,scene.released,'late resources must be released after asynchronous start');assert.equal(f.frames.size,0);
}
test('disposal during asynchronous start releases the late allocation rather than resurrecting GPU resources',()=>lateDisposal());

test('negative control: destroying before asynchronous allocation is detected by the cleanup oracle',async()=>{
  const mutant=source.replace('if(old){if(starting)void starting.then(()=>old.destroy(),()=>old.destroy());else old.destroy();}','if(old)old.destroy();');assert.notEqual(mutant,source);
  await assert.rejects(()=>lateDisposal(mutant),/late resources/);
});

test('negative control: a context handle alone cannot prove a running rendered optical scene',async()=>{
  const mutant=source.replace('scene?.gl&&scene.running&&scene.initialized&&!scene.gl.isContextLost()','scene?.gl');assert.notEqual(mutant,source);
  const f=harness({code:mutant,notRunning:true});const result=await f.finish();assert.throws(()=>assert.equal(result.mode,'fallback'));f.handle.dispose();
});

test('negative control: a CSS hex over a rounded optical rectangle fails the actual shape contract',async()=>{
  const mutant=source.replace("if(mask.shape==='hex')return FIELD_GLASS_HEX;","if(mask.shape==='hex')return {radius:.2};");assert.notEqual(mutant,source);
  const f=harness({code:mutant});await f.finish();assert.throws(()=>assert.equal(typeof f.scenes[0].specifications[0].shape.mask?.draw,'function'));f.handle.dispose();
});

test('pointer ownership is excluded from both canvas and decorative faces, including the broken CSS control',()=>{
  const check=code=>{for(const selector of ['uf-glass-canvas','uf-glass-face'])assert.match(code,new RegExp('\\.'+selector+'\\{[^}]*pointer-events:none!important'));};
  check(css);assert.throws(()=>check(css.replaceAll('pointer-events:none!important','pointer-events:auto')));
});
