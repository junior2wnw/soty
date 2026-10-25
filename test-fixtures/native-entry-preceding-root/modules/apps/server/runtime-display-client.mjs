/** Serialized with the managed Boot page. Finite progress only, never a Source
 * authority/readiness assertion and never a URL, ticket, cookie or error. */
export function createBootDisplayClient(config){
  const phases=['boot_admission_pending','boot_cookie_pending','boot_continuation_pending','boot_navigation','boot_failed','boot_inactive'];
  let active=config.mode==='boot'&&!config.renewal&&window.parent!==window,phase=null,origin='',nonce='',sent=0,timer;
  const stop=()=>{if(!active)return;active=false;clearTimeout(timer);window.removeEventListener('message',receiveDisplay);origin=nonce='';};
  const send=()=>{if(!active||!origin||!nonce||!phase||sent>=24)return;
    sent++;try{window.parent.postMessage({schema:'soty.runtime-display.v1',nonce,phase},origin);}catch{}
    if(sent===24)stop();};
  const receiveDisplay=event=>{
    if(!active||origin||event.source!==window.parent||!config.parentOrigins.includes(event.origin)||event.ports?.length)return;
    try{const v=event.data;if(!v||typeof v!=='object'||Array.isArray(v))return;
      const keys=Reflect.ownKeys(v),d=Object.getOwnPropertyDescriptors(v);
      if(keys.length!==2||!['schema','nonce'].every(k=>d[k]?.enumerable&&Object.hasOwn(d[k],'value'))
        ||d.schema.value!=='soty.runtime-display-watch.v1'||typeof d.nonce.value!=='string'||!/^[a-f0-9]{32}$/u.test(d.nonce.value))return;
      origin=event.origin;nonce=d.nonce.value;timer=setTimeout(stop,120000);send();
    }catch{}
  };
  if(active)window.addEventListener('message',receiveDisplay);
  return{phase(next){if(active&&phases.includes(next)){phase=next;send();}},close(){if(active){phase='boot_inactive';send();stop();}}};
}
