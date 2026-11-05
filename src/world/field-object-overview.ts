import {icon} from './icons';
import './field-object-overview.css';

export type FieldObjectPresentation='overview'|'glance'|'interactive';
/** Only current, permitted identity metadata. visual means the provider already
 * owns decoded pixels; it never asks this composition to read/clone content. */
export interface FieldObjectIdentity{title:string;symbol?:string;mark?:string;visual?:boolean}

export function createFieldObjectOverview(content:HTMLElement,options:{isCurrent():boolean;readIdentity():FieldObjectIdentity|null}){
  const element=document.createElement('div'),mark=document.createElement('span'),title=document.createElement('span');
  element.className='cf-widget-overview';mark.className='cf-widget-overview-mark';title.className='cf-widget-overview-title';
  element.setAttribute('aria-hidden','true');element.append(mark,title);
  let dead=false,presentation:FieldObjectPresentation='glance',signature='';
  function clear(){title.textContent='';mark.replaceChildren();signature='';element.dataset.overviewVisual=content.dataset.overviewVisual='false';}
  function update(){
    if(dead)return;
    const identity=options.isCurrent()?options.readIdentity():null;
    if(!identity){clear();return;}
    title.textContent=identity.title;
    const next=identity.mark===undefined?`icon:${identity.symbol??'cells'}`:`text:${identity.mark}`;
    if(next!==signature){mark.replaceChildren();if(identity.mark===undefined)mark.append(icon(identity.symbol??'cells'));else mark.textContent=identity.mark;signature=next;}
    const visual=identity.visual===true;
    element.dataset.overviewVisual=content.dataset.overviewVisual=String(visual);
    content.setAttribute('aria-hidden',String(presentation==='overview'&&!visual));
  }
  function setPresentation(mode:FieldObjectPresentation){
    if(dead)return;presentation=mode;element.dataset.presentation=content.dataset.presentation=mode;update();
  }
  setPresentation('glance');
  return{element,update,setPresentation,clear,dispose(){if(dead)return;clear();dead=true;element.remove();}};
}
