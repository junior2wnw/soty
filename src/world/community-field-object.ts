import type {FieldNode} from './unified-field-layout.mjs';
import type {FieldObjectView} from './native-field-objects';
import type {WorldCommunity,WorldHumanCallState} from './types';
import {createFieldObjectOverview,type FieldObjectPresentation} from './field-object-overview';
import {nativeFieldSafeArea} from './native-field-safe-area.mjs';
import {button,el,initials} from './dom';
import {icon} from './icons';
import './community-field-object.css';

export interface CommunityConversationHandle {
  isCurrent():boolean;
  flush():Promise<void>;
  hasUnsavedChanges():boolean;
  dispose():void;
}
export interface CommunityFieldObjectOptions {
  readCommunity():WorldCommunity|null;
  isCurrent():boolean;
  createListenButton(group:WorldCommunity):HTMLButtonElement;
  mountConversation(host:HTMLElement,group:WorldCommunity):CommunityConversationHandle;
  onJoin(group:WorldCommunity):Promise<WorldCommunity>;
  onDetails(group:WorldCommunity):void;
  onPaneChange?(pane:'voice'|'chat'):void;
  readCallState?():WorldHumanCallState|null;
  subscribeCall?(listener:()=>void):()=>void;
}
export interface CommunityFieldObjectView extends FieldObjectView {
  showChat():void;
  showVoice():void;
}

/** A view of an existing community. Root owns membership, call and draft rights. */
export function createCommunityFieldObject(initial:FieldNode,options:CommunityFieldObjectOptions):CommunityFieldObjectView {
  if(initial.entity.entity.kind!=='community')throw new TypeError('community_field_entity_required');
  const communityId=initial.entity.entity.id,root=el('article','cf-community');
  // The field silhouette or the shared preview owns the glass surface.
  root.dataset.glassSafeArea='off';
  const header=el('header','cf-header'),mark=el('span','cf-mark'),name=el('h2','cf-name');mark.append(icon('people'));mark.setAttribute('aria-hidden','true');
  const tools=el('div','cf-tools'),summary=button('Действия с сообществом','more','cf-more');summary.setAttribute('aria-label','Действия с сообществом');summary.title='Действия с сообществом';summary.setAttribute('aria-haspopup','dialog');summary.dataset.fieldControl='';tools.append(summary);
  const working=el('div','cf-working'),voice=el('section','cf-voice'),status=el('p','cf-status'),roster=el('ul','cf-roster');
  status.setAttribute('role','status');roster.setAttribute('aria-label','Подключения к голосовой комнате');
  const primary=el('div','cf-primary'),join=button('Вступить','people','cf-join'),error=el('p','cf-error');join.dataset.glassRole='accent';join.dataset.fieldControl='';
  error.setAttribute('role','alert');error.hidden=true;primary.append(join);voice.append(status,primary,roster,error);
  const chatHost=el('section','cf-chat'),chatToggle=button('Переписка','chat','cf-chat-toggle');chatHost.setAttribute('aria-label','Переписка сообщества');chatHost.hidden=true;chatToggle.dataset.glassRole='control';chatToggle.dataset.fieldControl='';
  header.append(mark,name,chatToggle,tools);working.append(voice,chatHost);
  const glance=el('div','cf-glance'),glanceMark=el('span','cf-glance-mark'),glanceName=el('strong','cf-glance-name'),glanceLabel=el('span','cf-glance-label');glanceMark.append(icon('people'));glanceMark.setAttribute('aria-hidden','true');glance.append(glanceMark,glanceName,glanceLabel);glance.setAttribute('aria-hidden','true');
  root.append(header,working,glance);
  let dead=false,bindingValid=true,visible=false,chatWanted=false,member=false,joining=false,generation=0,presentation:FieldObjectPresentation='glance';
  let conversation:CommunityConversationHandle|null=null,listen:HTMLButtonElement|null=null,boundGroup:WorldCommunity|null=null;
  const current=()=>!dead&&bindingValid&&options.isCurrent();
  const read=()=>{if(!current())return null;const group=options.readCommunity();return group?.communityId===communityId?group:null;};
  const identity=createFieldObjectOverview(root,{isCurrent:current,readIdentity:()=>{const group=read();return group?{title:group.name,symbol:'people'}:null;}});

  function disposeConversation(){
    const handle=conversation;conversation=null;
    const nodes=[...chatHost.querySelectorAll<HTMLElement>('*')];
    try{handle?.dispose();}finally{for(const node of nodes){if('value'in node)(node as HTMLInputElement).value='';node.removeAttribute('src');node.textContent='';}chatHost.replaceChildren();}
  }
  function dropListen(){if(listen){listen.disabled=true;listen.textContent='';listen.remove();}listen=null;if(boundGroup){boundGroup.name='';boundGroup.description='';boundGroup.previewMembers=[];boundGroup.membership=null;}boundGroup=null;}
  function clearRoster(){for(const node of roster.querySelectorAll('*'))node.textContent='';roster.replaceChildren();}
  function clearPrivate(){generation++;chatWanted=false;try{disposeConversation();}finally{dropListen();clearRoster();chatHost.hidden=true;voice.hidden=false;}}
  function scrub(){clearPrivate();member=false;joining=false;identity.clear();name.textContent=glanceName.textContent=glanceLabel.textContent=status.textContent=error.textContent='';join.disabled=true;join.hidden=chatToggle.hidden=tools.hidden=true;root.removeAttribute('aria-label');root.hidden=true;}
  function withGroup(action:(group:WorldCommunity)=>void){const group=read();if(!group){scrub();return;}action(group);}
  summary.addEventListener('click',()=>withGroup(options.onDetails));
  function ensureConversation(group:WorldCommunity){
    if(!chatWanted||!visible||presentation!=='interactive'||group.membership?.state!=='active')return;
    if(conversation&&!conversation.isCurrent()){chatWanted=false;disposeConversation();chatHost.hidden=true;voice.hidden=false;chatToggle.setAttribute('aria-pressed','false');chatToggle.querySelector('span')!.textContent='Переписка';return;}
    conversation??=options.mountConversation(chatHost,group);
  }
  function renderCall(group:WorldCommunity){
    const state=options.readCallState?.(),active=group.membership?.state==='active'&&state?.mode==='group'&&state.roomId===communityId&&(state.state==='joining'||state.state==='listening');
    status.textContent=!active?'Голосовая комната':state.state==='joining'?'Подключаемся…':state.connection==='connected'?'Вы слушаете':state.connection==='reconnecting'?'Связь восстанавливается':state.connection==='error'||state.connection==='closed'?'Связь прервана':'Ожидаем соединения';
    glanceLabel.textContent=active?'Вы в голосовой комнате':'Голосовое сообщество';
    clearRoster();
    for(const participant of active?state.participants??[]:[]){
      const row=el('li','cf-participant'),face=el('span','cf-participant-mark',initials(participant.label)),label=el('span','cf-participant-name',participant.label||'Участник'),microphone=el('span','cf-participant-microphone',participant.microphone?'Микр. вкл':'Микр. выкл');
      face.setAttribute('aria-hidden','true');if(participant.isSelf)label.append(el('span','cf-self',' · вы'));
      microphone.setAttribute('aria-label',participant.microphone?'Передача микрофона включена':'Микрофон выключен');row.append(face,label,microphone);roster.append(row);
    }
    roster.hidden=!roster.childElementCount;
  }
  function refresh(){
    const group=read();if(!group){scrub();return;}
    const active=group.membership?.state==='active';if(member&&!active)clearPrivate();member=active;root.hidden=false;
    name.textContent=glanceName.textContent=group.name;root.setAttribute('aria-label',`Сообщество: ${group.name}`);identity.update();
    if(active){
      if(!listen){boundGroup={...group};listen=options.createListenButton(boundGroup);const epoch=generation;
        // A detached Root-owned button must never revive a disposed/retired view.
        listen.addEventListener('click',event=>{const allowed=read();if(epoch!==generation||!allowed||allowed.membership?.state!=='active'){event.preventDefault();event.stopImmediatePropagation();}},{capture:true});
        listen.dataset.glassRole='accent';listen.dataset.fieldControl='';primary.prepend(listen);
      }else if(boundGroup)Object.assign(boundGroup,group);
    }else dropListen();
    const state=group.membership?.state;join.hidden=active;join.disabled=joining||state==='requested'||state==='banned'||group.joinPolicy==='invite'&&state!=='invited';
    join.querySelector('span')!.textContent=joining?'Подключаемся…':state==='requested'?'Заявка отправлена':state==='banned'?'Участие ограничено':state==='invited'?'Принять приглашение':group.joinPolicy==='request'?'Подать заявку':group.joinPolicy==='invite'?'По приглашению':'Вступить';
    tools.hidden=false;chatToggle.hidden=!active;chatToggle.setAttribute('aria-pressed',String(chatWanted));chatToggle.setAttribute('aria-label',chatWanted?'К голосу':'Переписка');
    if(chatToggle.dataset.pane!==(chatWanted?'chat':'voice')){chatToggle.replaceChildren(icon(chatWanted?'activity':'chat'),el('span','',chatWanted?'К голосу':'Переписка'));chatToggle.dataset.pane=chatWanted?'chat':'voice';}
    root.dataset.pane=chatWanted&&active?'chat':'voice';chatHost.hidden=!chatWanted||!active;voice.hidden=chatWanted&&active;ensureConversation(group);renderCall(group);
  }
  join.addEventListener('click',()=>withGroup(group=>{if(join.disabled||joining)return;joining=true;const ticket=generation;error.hidden=true;refresh();
    void(async()=>{try{const result=await options.onJoin(group);if(!current()||ticket!==generation){if(!dead&&!current())scrub();return;}if(result.communityId!==communityId)throw Error('community_changed');refresh();}
      catch{if(current()&&ticket===generation){error.textContent='Не удалось вступить. Повторите ещё раз.';error.hidden=false;}else if(!dead&&!current())scrub();}
      finally{if(!dead&&ticket===generation){joining=false;refresh();}}})();
  }));
  const setPane=(wanted:boolean)=>withGroup(group=>{if(wanted&&group.membership?.state!=='active')return;const changed=chatWanted!==wanted;chatWanted=wanted;refresh();if(changed&&current())options.onPaneChange?.(wanted?'chat':'voice');});
  const showChat=()=>setPane(true),showVoice=()=>{if(!dead)setPane(false);};
  chatToggle.addEventListener('click',()=>{chatWanted?showVoice():showChat();});
  const unsubscribe=options.subscribeCall?.(()=>{if(!dead)refresh();});
  function update(node:FieldNode){if(dead)return;if(node.entity.entity.kind!=='community'||node.entity.entity.id!==communityId){bindingValid=false;scrub();return;}
    const inset=nativeFieldSafeArea(node.presentation??node.entity.presentation??{shape:'rect'},node.width,node.height);for(const[side,value]of Object.entries(inset))root.style.setProperty(`--cf-safe-${side}`,`${value}px`);refresh();
  }
  update(initial);
  return{element:root,overview:identity.element,interaction:'focus',glanceSize:()=>({width:320,height:240}),update,
    setVisible(value){visible=value;if(value)refresh();},setPresentation(value){if(dead)return;presentation=value;identity.setPresentation(value);refresh();},
    setFocused(value){if(!dead)root.dataset.readableFocus=String(value);},
    activate(){if(!current())return;refresh();(chatWanted?chatHost.querySelector<HTMLElement>('textarea,input,button'):listen??join)?.focus({preventScroll:true});},
    flush:()=>conversation?.flush()??Promise.resolve(),hasUnsavedChanges:()=>conversation?.hasUnsavedChanges()===true,showChat,showVoice,
    dispose(){if(dead)return;dead=true;generation++;try{unsubscribe?.();}finally{try{clearPrivate();}finally{identity.dispose();name.textContent=glanceName.textContent=glanceLabel.textContent=status.textContent=error.textContent='';root.replaceChildren();root.removeAttribute('aria-label');root.remove();}}}
  };
}
