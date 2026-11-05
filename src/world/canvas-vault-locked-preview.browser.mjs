import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,mkdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import ts from 'typescript';

// Anonymous Chrome layout fixture: actual renderer/overview/CSS, synthetic
// locked projection. Real server rights are covered by renderer.test.mjs.
const {chromium}=await import(process.env.SOTY_PLAYWRIGHT_MODULE||'playwright');
const read=name=>readFileSync(new URL(name,import.meta.url),'utf8');
const compile=source=>ts.transpileModule(source,{compilerOptions:{target:ts.ScriptTarget.ES2022,module:ts.ModuleKind.ESNext}}).outputText.replace(/^import[^\n]*\n/gmu,'').replace(/^export /gmu,'');
const current=read('./canvas-vault-renderer.ts');
const old=read('./test-fixtures/canvas-vault-renderer-before-locked-preview.ts.txt');
assert.equal(createHash('sha256').update(old).digest('hex'),'eef1129808367e3a584e78e7d44e30e99b8a765fc4edffceace54a06d8c4a786','literal OLD renderer fixture must retain its reviewed bytes');
const css=['./native-field-objects.css','./canvas-vault-renderer.css','./canvas-field-content.css','./field-object-overview.css','./unified-field.css'].map(read).join('\n');
const artifacts=process.env.SOTY_BROWSER_ARTIFACTS_DIR||join(tmpdir(),'soty-browser-fixtures');
let browser;
before(async()=>{browser=await chromium.launch({channel:'chrome',headless:true});mkdirSync(artifacts,{recursive:true});});
after(async()=>{await browser?.close();});

async function fixture(t,width,code=current){
 const page=await browser.newPage({viewport:{width,height:844}});t.after(()=>page.close());
 await page.setContent('<!doctype html><html lang="ru"><style>'+css+'\nbody{margin:0;background:#eef0f6;--sw-surface:#fff;--sw-ink:#242439;--sw-text:#242439;--sw-bg:#eef0f6;--sw-accent:#6050cc;--sw-control-border:#c6c6d2}#node{left:40px;top:100px;transform:scale(.51);transform-origin:0 0;--uf-label-scale:1.9607843137;--uf-node-screen-width:173.4px;width:340px;height:300px}.sw-icon{width:20px;height:20px}</style><article id="node" class="uf-node uf-native"><div class="uf-object-mask" id="mask"></div></article>');
 await page.evaluate(({overview,renderer})=>{
  window.icon=()=>{const el=document.createElement('span');el.className='sw-icon';el.textContent='🔒';return el;};
  window.nativeFieldSafeArea=()=>({});window.CANVAS_VAULT_NOTE_TYPE='application/soty-note';window.vaultErrorCode=error=>error?.code;
  window.createCanvasVaultMemoryCache=()=>({clear:async()=>{}});window.createNativeFieldObjects=()=>{throw Error('locked renderer must not read content');};
  (0,eval)(overview+'\nwindow.makeOverview=createFieldObjectOverview;');window.createFieldObjectOverview=window.makeOverview;
  (0,eval)(renderer+'\nwindow.makeVault=createCanvasVaultView;');
  window.view=window.makeVault({width:340,height:300,entity:{entity:{id:'own-locked-object'}},presentation:{shape:'rect'}},{objectId:'own-locked-object',accountId:'synthetic',isCurrent:()=>true,projection:()=>({view:{schema:'locked',state:'locked'}}),client:{isUnlocked:()=>false,subscribe:()=>()=>{}}});
  document.querySelector('#mask').append(window.view.element);window.view.overview.classList.add('uf-object-overview');document.querySelector('#node').append(window.view.overview);window.view.setPresentation('glance');
 },{overview:compile(read('./field-object-overview.ts')),renderer:compile(code)});
 return page;
}
const inspect=page=>page.evaluate(()=>{const input=document.querySelector('input'),button=document.querySelector('.cv-unlock');return{inputVisible:input.checkVisibility({opacityProperty:true,visibilityProperty:true}),buttonVisible:button.checkVisibility({opacityProperty:true,visibilityProperty:true}),inert:window.view.element.inert,faceVisible:window.view.overview.checkVisibility({opacityProperty:true,visibilityProperty:true}),interaction:window.view.interaction,inputHeight:input.getBoundingClientRect().height};});
function noMiniForm(value){assert.equal(value.inputVisible,false,'locked preview must not paint a miniature password field');assert.equal(value.buttonVisible,false);assert.equal(value.inert,true);assert.equal(value.faceVisible,true);assert.equal(value.interaction,'focus');}

for(const width of [390,1280])test(`locked ${width} preview hides miniature controls and the same focused form has 44px targets`,async t=>{
 const page=await fixture(t,width);noMiniForm(await inspect(page));await page.screenshot({path:artifacts+`/d11-locked-preview-${width}.png`});
 await page.evaluate(()=>{window.oldForm=document.querySelector('.cv-locked');window.oldInput=document.querySelector('input');window.view.setPresentation('interactive');Object.assign(document.querySelector('#node').style,{transform:'scale(1)',width:'300px',height:'380px'});});
 const focused=await page.evaluate(()=>({sameForm:window.oldForm===document.querySelector('.cv-locked'),sameInput:window.oldInput===document.querySelector('input'),visible:document.querySelector('input').checkVisibility({opacityProperty:true,visibilityProperty:true}),height:document.querySelector('input').getBoundingClientRect().height,button:document.querySelector('.cv-unlock').getBoundingClientRect().height,label:document.querySelector('input').getAttribute('aria-label'),inert:window.view.element.inert}));
 assert.equal(focused.sameForm&&focused.sameInput&&focused.visible,true);assert.equal(focused.inert,false);assert.ok(focused.height>=44&&focused.button>=44);assert.equal(focused.label,'Пароль этой соты');
 await page.screenshot({path:artifacts+`/d11-locked-focus-${width}.png`});
});
test('literal OLD renderer visibly paints the mini-form and fails the unchanged browser oracle',async t=>{
 const page=await fixture(t,390,old),value=await inspect(page);assert.equal(value.inputVisible,true);assert.ok(value.inputHeight<44);assert.throws(()=>noMiniForm(value),assert.AssertionError);await page.screenshot({path:artifacts+'/d11-locked-preview-OLD.png'});
});
