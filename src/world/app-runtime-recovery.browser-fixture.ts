import './world.css';
import {createPalette} from './theme/palette.mjs';
import {mountAppStage,type AppStageHandle} from './app-stage';
import {formatAppLaunchRoute,parseAppLaunchRoute} from './app-launch.mjs';
import type {WorldApi} from './types';

if(!import.meta.env.DEV)throw Error('Development fixture only');
const appId=`app-${'51'.repeat(16)}`,domainId=`dom_${'52'.repeat(16)}`,accountId='runtime-recovery-synthetic';
const origin=`http://127.0.0.1:${location.port}`,entry={appId,domainId,origin,path:'/embed'};
const native={...entry,domainId:`dom_${'53'.repeat(16)}`,origin:`http://127.0.0.1:${Number(location.port)+1}`,path:'/'};
const source={id:'soty.standard-workspace',version:2,digest:'23061fc95610e3011ec42b3ecf229308bce44b0a1268408ca9b6ee138c98f7e9'};
const host=document.querySelector<HTMLElement>('#fixture-host')!,status=document.querySelector<HTMLElement>('#fixture-status')!;
const nativeChoice=document.querySelector<HTMLInputElement>('#fixture-native')!;
let stage:AppStageHandle|null=null,generation=0,launches=0,nativeReads=0,readinessReads=0;
for(const [key,value] of Object.entries(createPalette('dark',50)))document.documentElement.style.setProperty(key,value);
const update=()=>{status.dataset.launches=String(launches);status.dataset.nativeReads=String(nativeReads);status.dataset.readinessReads=String(readinessReads);};
const api={async request<T>(operation:string,args:Record<string,unknown>={}):Promise<T>{
 if(args.expectedAccountId!==undefined&&args.expectedAccountId!==accountId)throw Error('synthetic account mismatch');
 let result:unknown;
 if(operation==='apps.entry.get'){
  if(args.surface!=='native'||args.current!==true||args.appId!==appId)throw Error('unexpected fixture Native query');
  nativeReads++;result={entry:nativeChoice.checked?native:null};
 }else if(operation==='apps.scoped.context'){readinessReads++;result={};}
 else if(operation==='apps.saved.get')result={revision:0,entry:null};
 else throw Error('unsupported synthetic operation');
 update();return result as T;
}} as WorldApi;
function mount():void{
 generation++;stage?.dispose();launches=nativeReads=readinessReads=0;delete document.documentElement.dataset.releaseBoot;
 const token=generation,intent=parseAppLaunchRoute(formatAppLaunchRoute({appId,domainId,path:entry.path}))!;history.replaceState(null,'',`#${intent.route.replace(/^#/u,'')}`);
 stage=mountAppStage(host,{api,accountId,intent,app:{appId,name:'Длинное русское название приложения для проверки восстановления окна и понятных действий',status:'online',ownerAccountId:accountId},
  isCurrent:()=>generation===token,request:async()=>{launches++;update();return{url:`${origin}/_soty/boot?path=%2Fembed#${'F'.repeat(43)}`,entry,
   runtimeProfile:'soty.selected-human-embed.v2',scopedCloseHandle:'H'.repeat(43),scopedSource:source};},
  onNavigate:next=>history.replaceState(null,'',`#${next.route.replace(/^#/u,'')}`),onBack:()=>{},onAccount:async()=>{},onSettings:()=>{}});
 update();
}
document.querySelector('#fixture-mount')!.addEventListener('click',mount);
document.querySelector('#fixture-release')!.addEventListener('click',()=>{document.documentElement.dataset.releaseBoot='yes';});
window.addEventListener('pagehide',()=>{generation++;stage?.dispose();});
mount();
