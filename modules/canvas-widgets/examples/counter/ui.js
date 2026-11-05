import {createSotyAppWork} from '/assets/source-sdk.js';
import {bindGlassWidgetContext} from '/assets/widget-context.js';

const root=document.querySelector('main'),output=document.querySelector('output'),button=document.querySelector('button'),status=document.querySelector('[role=status]');
const symbol=button.querySelector('[aria-hidden]');
const context=bindGlassWidgetContext(root,{parentOrigin:document.querySelector('meta[name=soty-parent-origin]').content});
let pending=null,command=null,loaded=false,loading=false;
const work=window.parent===window?null:createSotyAppWork({parentOrigin:document.querySelector('meta[name=soty-parent-origin]').content,
 isDirty:()=>!!pending||!!command,save:async()=>{await pending;if(command)throw Error('counter_action_unconfirmed');}});
function paint(state){
 if(!Number.isSafeInteger(state.value)||state.value<0||!Number.isSafeInteger(state.revision)||state.revision<1)throw Error('counter_response_invalid');
 output.textContent=String(state.value);output.style.setProperty('--counter-digits',String(output.textContent.length));loaded=true;status.textContent='Сохранено';status.dataset.state='ready';
}
async function read(){const response=await fetch('/api/state',{cache:'no-store'});if(!response.ok)throw Error('counter_unavailable');paint(await response.json());}
function action(){
 button.disabled=loading||!!pending;button.setAttribute('aria-label',!loaded?'Повторить подключение':command?'Повторить сохранение':'Прибавить');symbol.textContent=!loaded||command?'↻':'+';
}
async function connect(){
 if(loading||pending)return;loading=true;root.setAttribute('aria-busy','true');status.textContent='Подключаемся…';status.dataset.state='loading';action();
 try{await read();}catch{status.textContent='Нет связи';status.dataset.state='error';}
 finally{loading=false;root.setAttribute('aria-busy','false');action();}
}
button.addEventListener('click',()=>{
 if(pending||loading)return;if(!loaded){void connect();return;}command??=crypto.randomUUID();const id=command;button.disabled=true;root.setAttribute('aria-busy','true');status.textContent='Сохраняем…';status.dataset.state='loading';
 pending=(async()=>{const response=await fetch('/api/increment',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({requestId:id})});if(!response.ok)throw Error('counter_unavailable');
  const state=await response.json();if(state.receipt?.requestId!==id)throw Error('counter_receipt_invalid');paint(state);command=null;
 })().catch(()=>{status.textContent='Нет подтверждения';status.dataset.state='error';}).finally(()=>{pending=null;action();root.setAttribute('aria-busy','false');work?.changed();});
 work?.changed();
});
void connect();
window.addEventListener('pagehide',()=>{work?.dispose();context.dispose();},{once:true});
