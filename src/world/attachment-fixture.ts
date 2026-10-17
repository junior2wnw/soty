import { createConnectClient } from '../../modules/connect/browser/index.mjs';
import { mountWorldApp } from './app';
import type { WorldApi, WorldCommunity } from './types';
if (!import.meta.env.DEV) throw Error('OWN fixture is development-only');
const params=new URLSearchParams(location.search),actor=params.get('actor')==='bob'?'bob':'alice';
const client=createConnectClient({projectId:'m02-browser-own',endpoint:location.origin+'/api/connect/rpc',dbName:'soty-m02-browser-'+actor,capturePlatformInvitation:false});
const account=await client.bootstrap(actor==='alice'?'Аня · локально':'Борис · локально');
// OWN fault port delays UI delivery only after the real signed HTTP response.
// No signer/client or response contents are exposed to the browser controls.
let holdNextChatAck=false,releaseChatAck:(()=>void)|null=null;
document.addEventListener('soty-own-hold-chat-ack',()=>{holdNextChatAck=true;});
document.addEventListener('soty-own-release-chat-ack',()=>{releaseChatAck?.();releaseChatAck=null;});
const api:WorldApi={request:async<T>(op:string,args?:Record<string,unknown>)=>{
  const result=await client.extension<T>(op,args,{expectedAccountId:account.accountId});
  if(op==='world.chat.send'&&holdNextChatAck){
    holdNextChatAck=false;document.body.dataset.fixtureAckHeld='true';
    await new Promise<void>(resolve=>{releaseChatAck=resolve;});document.body.dataset.fixtureAckHeld='false';
  }
  return result;
}};
await api.request('world.profile.get');
let communityId=params.get('group');
if(!communityId){const created=await api.request<{community:WorldCommunity}>('world.community.create',{requestId:'m02_browser_community',name:'Файлы для своего круга',description:'Локальная приёмка: реальные файлы и SQLite.',joinPolicy:'open'});communityId=created.community.communityId;}
else await api.request('world.membership.join',{communityId});
if(actor==='alice')await api.request('world.community.create',{requestId:'m02_browser_other',name:'Другой локальный чат',joinPolicy:'open'});
document.body.dataset.fixtureCommunity=communityId;document.body.dataset.fixtureActor=actor;
history.replaceState({},'',location.pathname+`?actor=${actor}&group=${encodeURIComponent(communityId)}#messages/${communityId}`);
mountWorldApp(document.querySelector<HTMLElement>('#app')!,{api,localAccount:async()=>({accountId:account.accountId,label:actor==='alice'?'Аня':'Борис'}),
  openLegacy:()=>{},openAccount:()=>{},connectDevice:()=>{},agentCreate:()=>{},listApps:async()=>[],listDevices:async()=>[]});
