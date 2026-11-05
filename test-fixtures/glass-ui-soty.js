import {createGlassUI} from '../vendor/optical-glass/src/ui-kit.js';
import '../vendor/optical-glass/src/ui-kit.css';
import {createDialog,pinDialogSubmit} from '../src/world/dialogs.ts';
import {mountShellGlass} from '../src/world/shell-glass.ts';
import '../src/world/world.css';
import './glass-ui-soty.css';

// Actual shared library and actual Soty native dialog; no account/data API.
const svg='<svg xmlns="http://www.w3.org/2000/svg" width="1400" height="1000"><defs><radialGradient id="a"><stop stop-color="#7ac7d0"/><stop offset="1" stop-color="#f2f5f5"/></radialGradient><radialGradient id="b"><stop stop-color="#aaa4db"/><stop offset="1" stop-color="#f2f5f5" stop-opacity="0"/></radialGradient></defs><rect width="1400" height="1000" fill="#eef3f2"/><ellipse cx="1180" cy="240" rx="650" ry="640" fill="url(#a)"/><ellipse cx="220" cy="810" rx="620" ry="520" fill="url(#b)"/></svg>';
const background='data:image/svg+xml;charset=utf-8,'+encodeURIComponent(svg);
document.body.style.backgroundImage=`url("${background}")`;
const root=document.querySelector('#glass-fixture'),renderPolicy=new URLSearchParams(location.search).get('mode')||'auto';
const statuses=[];
const ui=await createGlassUI({root,background,canvasMode:'viewport',renderPolicy,dprCap:1.5,fpsCap:30,onRenderStatus:status=>statuses.push(status)});
let dialog=null,dialogUI=null,saves=0;
document.querySelector('#save').addEventListener('click',()=>{document.querySelector('#fixture-status').textContent=`Сохранено ${++saves}`;});
document.querySelector('#open-dialog').addEventListener('click',async()=>{
 if(dialog?.element.open)return;
 dialog=createDialog('Добавить на полотно',()=>{dialogUI=null;dialog=null;},undefined,document.body,{renderPolicy});
 dialog.element.classList.add('glass-ui','fixture-dialog');dialog.element.dataset.glassTone='light';dialog.element.dataset.glassRole='overlay';
 const form=document.createElement('form');form.className='fixture-form';
 const label=document.createElement('label');label.textContent='Записка';const text=document.createElement('textarea');text.setAttribute('aria-label','Текст записки');text.dataset.glassRole='field';label.append(text);
 const submit=document.createElement('button');submit.textContent='На полотно';submit.dataset.glassRole='accent';form.append(label);dialog.body.append(form);pinDialogSubmit(dialog,form,submit);
 dialog.element.querySelector('.sw-dialog-header button').dataset.glassRole='control';
 form.addEventListener('submit',event=>{event.preventDefault();document.querySelector('#fixture-status').textContent=text.value;dialog.close();});
 dialogUI=mountShellGlass(dialog.element,{renderPolicy});await dialogUI.ready;
});
window.__glassUIFixture={ui,statuses,get dialogUI(){return dialogUI;},get dialog(){return dialog;}};
