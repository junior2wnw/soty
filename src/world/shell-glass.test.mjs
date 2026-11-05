import assert from 'node:assert/strict';
import test from 'node:test';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import ts from 'typescript';
import * as classifier from './glass-roles.mjs';

class Element{
  constructor(tag,doc){this.tagName=tag.toUpperCase();this.ownerDocument=doc;this.children=[];this.attrs=new Map();this.dataset=new Proxy({},{get:(_,key)=>this.getAttribute('data-'+key.replace(/[A-Z]/gu,c=>'-'+c.toLowerCase())),set:(_,key,value)=>{this.setAttribute('data-'+key.replace(/[A-Z]/gu,c=>'-'+c.toLowerCase()),value);return true;}});this.style={values:new Map(),getPropertyValue(key){return this.values.get(key)?.value||'';},getPropertyPriority(key){return this.values.get(key)?.priority||'';},setProperty(key,value,priority=''){this.values.set(key,{value,priority});},removeProperty(key){this.values.delete(key);}};this.classList={contains:name=>(this.getAttribute('class')||'').split(' ').includes(name),add:name=>this.setAttribute('class',[...new Set((this.getAttribute('class')||'').split(' ').filter(Boolean).concat(name))].join(' ')),remove:name=>this.setAttribute('class',(this.getAttribute('class')||'').split(' ').filter(value=>value!==name).join(' '))};}
  getAttribute(name){return this.attrs.get(name)??null;}hasAttribute(name){return this.attrs.has(name);}setAttribute(name,value){this.attrs.set(name,String(value));}removeAttribute(name){this.attrs.delete(name);}
  matches(selector){return selector.split(',').some(value=>{value=value.trim();return value.startsWith('.')?this.classList.contains(value.slice(1)):value.startsWith('[')?this.hasAttribute(value.slice(1,-1)):value===this.tagName.toLowerCase();});}
  closest(selector){for(let node=this;node;node=node.parentNode)if(node.matches(selector))return node;return null;}
  append(node){this.children.push(node);node.parentNode=this;}
  contains(node){return node===this||this.children.some(child=>child.contains(node));}
  querySelectorAll(selector){return this.children.flatMap(node=>[...(node.matches(selector)?[node]:[]),...node.querySelectorAll(selector)]);}
}
async function fixture({delay=false}={}){
  const observers=[],frames=new Map(),calls=[],doc={},html=new Element('html',doc),root=new Element('dialog',doc);let frameId=0,release;
  class Observer{constructor(callback){this.callback=callback;observers.push(this);}observe(target,options){this.target=target;this.options=options;}disconnect(){this.disconnected=true;}}
  doc.documentElement=html;html.dataset.sotyTheme='light';html.append(root);
  doc.defaultView={HTMLCanvasElement:class{},getComputedStyle:()=>({getPropertyValue:key=>({'--sw-bg':'#eef3f2','--sw-ink':'#123456','--sw-muted':'#526272','--sw-surface':'#ffffff','--sw-surface-raised':'#fdfdfd'}[key]||'')}),requestAnimationFrame:callback=>{const id=++frameId;frames.set(id,callback);return id;},cancelAnimationFrame:id=>frames.delete(id)};
  const module={exports:{}},source=await readFile(process.env.SHELL_GLASS_SOURCE||new URL('./shell-glass.ts',import.meta.url),'utf8');
  const factory=options=>{const row={options,destroyed:0,backgrounds:[],renderMode:'static',destroy(){this.destroyed++;},setBackground(value){this.backgrounds.push(value);},removeRole(node){node.removeAttribute('data-glass-role');}};calls.push(row);options.onRenderStatus({policy:options.renderPolicy,mode:'static',reason:'policy-static',gpuReady:false});return delay?new Promise(resolve=>{release=()=>resolve(row);}):Promise.resolve(row);};
  const compiled=ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.CommonJS}}).outputText;
  vm.runInNewContext(compiled,{module,exports:module.exports,require:name=>name.endsWith('ui-kit.js')?{createGlassUI:factory,GLASS_UI_WORKING_THEME:{name:'working'}}:name.endsWith('glass-roles.mjs')?classifier:{},MutationObserver:Observer,CSS:{supports:()=>true}});
  return {api:module.exports,root,html,calls,observers,frames,release:()=>release(),button(attrs={}){const button=new Element('button',doc);for(const[key,value]of Object.entries(attrs))button.setAttribute(key,value);root.append(button);return button;},mutate(){for(const observer of observers)if(observer.target===root&&!observer.disconnected)observer.callback([]);for(const callback of frames.values())callback();frames.clear();}};
}

test('direct material root keeps authored roles through class renames and unrelated mutations',async()=>{
  const f=await fixture(),button=f.button({'data-glass-role':'control','data-glass-safe-area':'off'});
  const mount=f.api.mountMaterialGlass||f.api.mountShellGlass,handle=mount(f.root);await handle.ready;
  button.setAttribute('data-glass-role','accent');button.setAttribute('class','renamed-by-producer');f.root.setAttribute('class','unrelated');f.mutate();
  assert.equal(button.getAttribute('data-glass-role'),'accent');assert.equal(f.observers.filter(row=>row.target===f.root).length,0,'explicit root needs no classifier observer');
  assert.equal(mount(f.root),handle);assert.equal(f.api.mountShellGlass(f.root),handle);assert.equal(f.calls.length,1);
  handle.dispose();handle.dispose();assert.equal(f.calls[0].destroyed,1);assert.equal(button.getAttribute('data-glass-role'),'accent');assert.equal(button.getAttribute('data-glass-safe-area'),'off');
});

test('compatibility annotates only absent roles and relinquishes an updated producer role',async()=>{
  const f=await fixture(),inferred=f.button(),authored=f.button({'data-glass-role':'accent'}),handle=f.api.mountShellGlass(f.root);await handle.ready;
  assert.equal(inferred.getAttribute('data-glass-role'),'control');assert.equal(authored.getAttribute('data-glass-role'),'accent');
  inferred.setAttribute('data-glass-role','accent');f.mutate();assert.equal(inferred.getAttribute('data-glass-role'),'accent');
  handle.dispose();assert.equal(inferred.getAttribute('data-glass-role'),'accent');assert.equal(authored.getAttribute('data-glass-role'),'accent');
});

test('compatibility cleanup removes its inferred role but retains explicit fields and iframe boundaries',async()=>{
  const f=await fixture(),inferred=f.button(),plane=new Element('div',f.root.ownerDocument);plane.setAttribute('class','sa-frame');f.root.append(plane);
  const inside=new Element('button',f.root.ownerDocument);plane.append(inside);const handle=f.api.mountShellGlass(f.root);await handle.ready;
  assert.equal(inside.hasAttribute('data-glass-role'),false);assert.equal(plane.hasAttribute('data-glass-ignore'),true);handle.dispose();assert.equal(inferred.hasAttribute('data-glass-role'),false);assert.equal(plane.hasAttribute('data-glass-ignore'),false);
});

test('close before asynchronous material readiness disposes the late instance without reviving its root',async()=>{
  for(const entry of ['mountMaterialGlass','mountShellGlass']){
    const f=await fixture({delay:true}),mount=f.api[entry]||f.api.mountShellGlass,button=f.button({'data-glass-role':'accent','data-glass-safe-area':'off'}),handle=mount(f.root,{renderPolicy:'auto'});
    handle.dispose();f.release();await handle.ready;
    assert.equal(f.calls[0].destroyed,1);assert.equal(f.root.getAttribute('data-shell-glass'),null);assert.equal(f.root.getAttribute('data-glass-material'),null);assert.equal(button.getAttribute('data-glass-role'),'accent');assert.ok(f.observers.every(row=>row.disconnected));assert.equal(f.frames.size,0);
    const next=await fixture(),reopened=(next.api[entry]||next.api.mountShellGlass)(next.root);await reopened.ready;assert.equal(next.calls.length,1);reopened.dispose();
  }
});

test('runtime reuses the current theme bridge and restores the host palette on dispose',async()=>{
  const f=await fixture();f.root.style.setProperty('--glass-ui-ink','old','important');const handle=(f.api.mountMaterialGlass||f.api.mountShellGlass)(f.root);await handle.ready;
  assert.equal(f.root.style.getPropertyValue('--glass-ui-ink'),'#123456');assert.equal(f.root.dataset.glassTone,'light');f.html.dataset.sotyTheme='dark';const themes=f.observers.find(row=>row.target===f.html);themes.callback([]);assert.equal(f.root.dataset.glassTone,'dark');assert.equal(f.calls[0].backgrounds.length,1);handle.dispose();assert.equal(f.root.style.getPropertyValue('--glass-ui-ink'),'old');assert.equal(f.root.style.getPropertyPriority('--glass-ui-ink'),'important');
});
