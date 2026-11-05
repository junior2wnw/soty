import {accountClient,observeAccount} from '../core/connect-client';
import {createCanvasPageScreen,type CanvasPageScreen} from '../world/canvas-page-screen';
import {parseCanvasPageRoute} from '../world/canvas-page-client.mjs';
import {formatAppLaunchRoute} from '../world/app-launch.mjs';
import type {DirectoryApi} from '../world/field-directory';
import {readCanvasPageHandoff} from '../world/workspace-return.mjs';
import type {UnifiedFieldViewState} from '../world/unified-field';
import {mountShellGlass,type ShellGlassHandle} from '../world/shell-glass';
import '../world/world.css';

/** Opening another person's page does not require an account creation. A
 * saved identity may read its own grants through the same signed transport. */
export async function startCanvasPageEntry(root:HTMLElement):Promise<void>{
 let screen:CanvasPageScreen|null=null,material:ShellGlassHandle|null=null,sequence=0,closed=false;const lifetime=new AbortController();
 const retire=()=>{screen?.dispose();screen=null;};
 const navigate=(hash:string)=>{window.location.hash=hash;};
 const cleanup=()=>{if(closed)return;closed=true;sequence++;retire();material?.dispose();material=null;lifetime.abort();unobserve();root.classList.remove('cp-entry','sw-app');};
 const route=async()=>{
  const ticket=++sequence;retire();if(closed)return;
  const requested=location.hash,pageId=parseCanvasPageRoute(requested);
  if(!pageId){cleanup();const {startWorld}=await import('./world-adapter');if(root.isConnected)await startWorld(root);return;}
  root.replaceChildren();root.classList.add('sw-app','cp-entry');
  if(!material&&root.ownerDocument?.defaultView?.HTMLCanvasElement)material=mountShellGlass(root);
  const local=await accountClient.getLocalState().catch(()=>null);if(closed||ticket!==sequence||!root.isConnected)return;
  const accountId=local?.accountId&&local.deviceId&&!local.current?.revoked?local.accountId:null,guestPreview=location.hash.endsWith('?view=public');
  const current=()=>!closed&&ticket===sequence&&root.isConnected&&location.hash===requested;
  let viewState:UnifiedFieldViewState|undefined,opening=false;
  const api:DirectoryApi={request<T>(op:string,args:Record<string,unknown>={},requestOptions?:{signal?:AbortSignal}){if(!accountId||!current())return Promise.reject(Object.assign(Error('canvas_page_account_changed'),{code:'canvas_page_account_changed'}));return accountClient.extension<T>(op,args,{expectedAccountId:accountId,...(requestOptions?.signal?{signal:requestOptions.signal}:{})});}};
  screen=createCanvasPageScreen({pageId,...(accountId&&!guestPreview?{accountId,api}:{}),guestPreview,homeLabel:accountId?'Моё полотно':'Открыть свои Соты',isCurrent:current,onHome:()=>navigate('mine'),
    onViewStateChange:value=>{if(current())viewState=value;},
    onOpenApp:entry=>{void(async()=>{
      if(!current()||opening)return;opening=true;
      try{
        const launchRoute='#'+formatAppLaunchRoute(entry),handoff=readCanvasPageHandoff({route:requested,launchRoute,accountId,
          deviceId:accountId?local?.deviceId??null:null,canvasView:viewState});
        // Keep this entry's observer alive through the lazy import. A changed
        // account/route, including ABA, retires its ticket before handoff.
        const {startWorld}=await import('./world-adapter');if(!current()||!handoff)return;
        cleanup();navigate(launchRoute);
        if(root.isConnected)await startWorld(root,{canvasPageReturn:handoff});
      }finally{opening=false;}
    })();}});root.replaceChildren(screen.element);await screen.ready;
 };
 const unobserve=observeAccount(()=>{void route();});
 window.addEventListener('hashchange',()=>{void route();},{signal:lifetime.signal});
 window.addEventListener('popstate',()=>{void route();},{signal:lifetime.signal});
 window.addEventListener('pagehide',event=>{if(!(event as PageTransitionEvent).persisted)cleanup();},{signal:lifetime.signal});
 await route();
}
