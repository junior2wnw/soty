/** Bounded UI observation. Only the injected fresh Root read may confirm
 * authenticated readiness. A load or frame message never authorizes anything. */
const PHASES=new Set(['boot_admission_pending','boot_cookie_pending','boot_continuation_pending','boot_navigation','boot_failed','boot_inactive']);
function fields(value,keys){
  if(!value||typeof value!=='object'||Array.isArray(value))return null;
  try{const names=Reflect.ownKeys(value),d=Object.getOwnPropertyDescriptors(value);
    if(names.length!==keys.length||!keys.every(k=>d[k]?.enumerable&&Object.hasOwn(d[k],'value')))return null;
    return Object.fromEntries(keys.map(k=>[k,d[k].value]));
  }catch{return null;}
}
export function createAppRuntimeDisplay({view,frame,origin,isCurrent,readiness,onChange=()=>{},timeoutMs=12000}){
  const expected=new URL(origin);if(expected.origin!==origin||!['https:','http:'].includes(expected.protocol))throw Error('invalid_runtime_display_origin');
  if(typeof isCurrent!=='function'||typeof readiness!=='function'||timeoutMs!==12000)throw Error('invalid_runtime_display_options');
  let disposed=false,terminal=false,dismissed=false,loaded=false,lateLoaded=false,flight=false,queued=false,checks=0,phase='opening',timer;
  let nonce='';try{const n=view.crypto.randomUUID().replaceAll('-','');if(/^[a-f0-9]{32}$/u.test(n))nonce=n;}catch{}
  const probes=[],live=()=>!disposed&&isCurrent();
  const snapshot=()=>{
    const pending=['opening','boot','authenticating'].includes(phase),failed=['unconfirmed','denied','error'].includes(phase);
    const messages={opening:'Открываем окно приложения…',boot:'Окно Сот открыто. Проверяем вход…',authenticating:'Подтверждаем вход в приложение…',ready:'',
      loaded:'',unconfirmed:loaded?'Окно загружено, но запуск приложения не подтверждён. Если оно пустое, откройте заново.':'Загрузка окна не подтверждена. Если оно пустое, откройте заново.',
      denied:'Этот вход недоступен. Выберите нужный аккаунт или попросите владельца проверить доступ.',
      error:'Не удалось подтвердить запуск приложения. Попробуйте открыть заново.'};
    return Object.freeze({phase,message:dismissed?'':messages[phase]??'',busy:pending&&!dismissed,
      canRetry:failed&&!dismissed,canExternal:(failed||phase==='loaded')&&!dismissed,canDismiss:failed&&!dismissed});
  };
  const notify=()=>{if(live())onChange(snapshot());};
  const stopProbes=()=>{for(const id of probes)view.clearTimeout(id);probes.length=0;};
  // Source-session ready is not Native DOM paint. A later current-frame
  // transport error must remain visible. A first late document may refine a
  // no-load timeout using the same bounded read budget, never a new admission.
  const lateDocument=()=>terminal&&phase==='unconfirmed'&&lateLoaded&&!dismissed;
  const finish=next=>{if(!live()||(terminal&&!lateDocument()&&!(['ready','loaded'].includes(phase)&&next==='error')))return;terminal=true;phase=next;view.clearTimeout(timer);stopProbes();notify();};
  const check=async()=>{
    if(!live()||terminal&&!lateDocument()||!loaded||checks>=3)return;
    if(flight){queued=true;return;}
    flight=true;checks++;
    try{const answer=await readiness();if(!live()||terminal&&!lateDocument())return;
      if(answer==='ready')finish('ready');else if(answer==='denied')finish('denied');
      // Legacy/native apps have no Source readiness protocol. Their document
      // loaded; this says nothing about authenticated app state or data paint.
      else if(answer==='unsupported')finish('loaded');
      // No additional grant, renewal, retry or navigation is triggered here.
    }catch{ /* Unknown is not denial. The existing finite visible deadline wins. */ }
    finally{flight=false;if(queued){queued=false;void check();}}
  };
  // A pre-load probe may reach about:blank; a fast ready read cancels later timers.
  const load=()=>{if(!live()||terminal&&!(phase==='unconfirmed'&&!loaded&&!dismissed))return;
    if(!loaded){loaded=true;lateLoaded=terminal;if(terminal)notify();else probe();}void check();};
  const error=()=>finish('error');
  const receive=event=>{
    if(!live()||terminal&&!['ready','loaded'].includes(phase)||!nonce||event.source!==frame.contentWindow||event.origin!==origin||event.ports?.length)return;
    const v=fields(event.data,['schema','nonce','phase']);
    if(!v||v.schema!=='soty.runtime-display.v1'||v.nonce!==nonce||!PHASES.has(v.phase))return;
    if(v.phase==='boot_failed'){finish('error');return;}
    if(terminal)return; // Session readiness or neutral load cannot hide a later explicit Boot failure.
    if(v.phase==='boot_inactive')return;
    phase=v.phase==='boot_admission_pending'?'boot':'authenticating';notify();
    if(v.phase==='boot_navigation')void check();
  };
  const probe=()=>{if(!live()||terminal||!nonce)return;try{frame.contentWindow?.postMessage({schema:'soty.runtime-display-watch.v1',nonce},origin);}catch{}};
  const close=()=>{if(disposed)return;disposed=true;view.clearTimeout(timer);stopProbes();
    frame.removeEventListener('load',load);frame.removeEventListener('error',error);view.removeEventListener('message',receive);view.removeEventListener('pagehide',close);};
  frame.addEventListener('load',load);frame.addEventListener('error',error);view.addEventListener('message',receive);view.addEventListener('pagehide',close);
  timer=view.setTimeout(()=>{if(live())finish('unconfirmed');else close();},timeoutMs);
  for(const ms of [0,250,1000,3000])probes.push(view.setTimeout(probe,ms));
  // Registration happens before insertion. The stage owns the initial message;
  // the queued notification runs only once the frame is actually current.
  probes.push(view.setTimeout(notify,0));
  return Object.freeze({snapshot,dismiss(){if(!live()||!terminal||['ready','loaded'].includes(phase))return false;dismissed=true;notify();return true;},dispose:close});
}
