import {createUnifiedField} from '../unified-field';
import '../unified-field.css';
const initial={schema:'soty.field.v2',contexts:[{contextId:'personal',title:'Synthetic geometry',x:0,y:0}],shortcuts:[{shortcutId:'sample',entity:{kind:'object',id:'object_geometry'},contextId:'personal',placement:{kind:'free',x:0,y:0,width:320,height:240}}]};
const saved=JSON.parse(localStorage.getItem('resize-boundary-document')||'null');
let revision=saved?.revision||0,creates=0,summary:any,mask:any={shape:'hex'};
const entity=()=>({entity:{kind:'object' as const,id:'object_geometry'},title:'Synthetic geometry',presentation:mask});
const engine=createUnifiedField({accountId:'synthetic-geometry',document:saved?.document||initial,revision,entities:[entity()],
  viewState:{mine:{x:0,y:0,scale:1},mineFit:'manual',mineSelection:'sample'},readableObjectFocus:true,
  commit:async document=>{revision++;localStorage.setItem('resize-boundary-document',JSON.stringify({revision,document}));return{status:'saved',revision,document,localDurable:true};},
  objectKey:()=> 'fixture-stable-source',onStateChange:value=>{summary=value;},
  createObject:()=>{creates++;const element=document.createElement('article'),frame=document.createElement('iframe');element.style.cssText='width:100%;height:100%;background:#e6eaf5';frame.src='/frame';frame.style.cssText='width:100%;height:100%;border:0';element.append(frame);return{element,update(){},setVisible(){},setFocused(){},async flush(){},hasUnsavedChanges:()=>false,dispose(){element.remove();}};},
});
document.body.append(engine.element);Object.assign(window,{fixture:{engine,get creates(){return creates;},get mask(){return mask;},get summary(){return summary;},async shape(value:any){mask=value;engine.update({entities:[entity()]});await new Promise(requestAnimationFrame);await new Promise(requestAnimationFrame);}}});
await engine.ready;
