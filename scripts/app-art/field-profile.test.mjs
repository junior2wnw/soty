import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,readFile,writeFile,rm,realpath,copyFile,readdir,stat} from 'node:fs/promises';
import {dirname,basename,join} from 'node:path';
import {tmpdir} from 'node:os';
import sharp from 'sharp';
import {build as viteBuild,createLogger} from 'vite';
import {prepareArtwork,importArtwork,bindArtwork,aliasFieldArtwork,configureFieldIcon,validateArtwork,validatePublicArtwork,readJson,rollbackArtwork,buildPrompt} from './pipeline.mjs';
const id='app-'+'1'.repeat(32);
const spec=(key,profile)=>({key,name:'Generic fixture',purpose:'Safe artwork fixture',subject:'One generic studio sculpture',alt:'Studio sculpture',palette:{base:'#303B34',accent:'#C8D6BF',ink:'#F4F5ED'},focalPoint:{x:.5,y:.4},kind:'example',...(profile?{profile}: {})});
async function fixture(t){const root=await mkdtemp(join(tmpdir(),'soty-field-art-'));t.after(async()=>{const actual=await realpath(root),temp=await realpath(tmpdir());assert.equal(dirname(actual).toLowerCase(),temp.toLowerCase());assert.ok(basename(actual).startsWith('soty-field-art-'));await rm(actual,{recursive:true,force:true});});await mkdir(join(root,'scripts','app-art'),{recursive:true});await mkdir(join(root,'public','app-art'),{recursive:true});await writeFile(join(root,'scripts','app-art','registry.json'),JSON.stringify({schemaVersion:1,styleVersion:'fixture-1',assets:{},bindings:{}}));return root;}
async function image(root,square,color='#C8D6BF'){const source=join(root,'source-'+Math.random().toString(16).slice(2)+'.png');await sharp({create:{width:1024,height:square?1024:683,channels:3,background:color}}).png().toFile(source);return source;}
async function setup(t){const root=await fixture(t);await prepareArtwork(root,{definitions:[spec('hive')]});await importArtwork(root,{key:'hive',source:await image(root,false),visualApproved:true});const before=await readFile(join(root,'public','app-art','manifest.json'));await prepareArtwork(root,{definitions:[spec('field-hive','field')]});return{root,before};}
test('field preparation asks for a square with hexagon crop clearance while card preparation preserves landscape composition',()=>{
 const field=buildPrompt(spec('field-fixture','field')),card=buildPrompt(spec('card-fixture'));
 assert.match(field,/square 1:1/u);assert.match(field,/central 70%/u);assert.match(field,/rounded point-up hexagon/u);assert.doesNotMatch(field,/landscape 3:2|4:3 card/u);
 assert.match(card,/landscape 3:2/u);assert.match(card,/4:3 card/u);assert.doesNotMatch(card,/hexagon/u);
});
test('square field import/profile binding preserves exact existing card manifest and four non-upscaled renditions',async t=>{
 const {root,before}=await setup(t);await aliasFieldArtwork(root,{coverKey:'hive',key:'field-hive'});await bindArtwork(root,{appId:id,key:'field-hive',profile:'field'});
 const pending=await readJson(join(root,'public','app-art','field-profile.json'));assert.deepEqual(pending.aliases,{});assert.deepEqual(pending.bindings,{});assert.ok(!JSON.stringify(pending).includes(id));
 const result=await importArtwork(root,{key:'field-hive',source:await image(root,true),visualApproved:true});assert.equal(result.version,1);assert.deepEqual(result.renditionBytes.map(r=>r.width),[160,320,640,960]);
 assert.deepEqual(await readFile(join(root,'public','app-art','manifest.json')),before);
 const field=await readJson(join(root,'public','app-art','field-profile.json'));assert.equal(field.aliases.hive,'field-hive');assert.equal(field.assets['field-hive'].renditions[0].width,field.assets['field-hive'].renditions[0].height);assert.ok(!JSON.stringify(field).includes(id));assert.ok(Object.keys(field.bindings)[0].startsWith('bind-'));
 assert.deepEqual(await readFile(join(root,'public','app-art','field-profile.json')),await readFile(join(root,'src','world','app-art-field-profile.json')));
 assert.equal((await validateArtwork(root)).valid,true);assert.equal((await validatePublicArtwork(root)).fieldProfile.checked.length,1);
});
test('field profile refuses landscape, global accidental binding and opaque/private aliases before activation',async t=>{
 const {root,before}=await setup(t);await assert.rejects(importArtwork(root,{key:'field-hive',source:await image(root,false),visualApproved:true}),/square/u);await assert.rejects(bindArtwork(root,{appId:id,key:'field-hive'}),/profile/u);
 const privateSpec={...spec('local-sensitive','field'),kind:'app',name:'PRIVATE_NAME',purpose:'PRIVATE_PURPOSE',alt:'PRIVATE_ALT'};
 await prepareArtwork(root,{definitions:[privateSpec]});await assert.rejects(aliasFieldArtwork(root,{coverKey:'private-name',key:privateSpec.key}),/opaque/u);
 await bindArtwork(root,{appId:id,key:privateSpec.key,profile:'field'});await importArtwork(root,{key:privateSpec.key,source:await image(root,true),visualApproved:true});
 const field=await readJson(join(root,'public','app-art','field-profile.json'));assert.match(Object.keys(field.assets)[0],/^art-[a-f0-9]{32}$/u);assert.equal(Object.values(field.assets)[0].alt,'');for(const marker of ['PRIVATE_',privateSpec.key,id])assert.ok(!JSON.stringify(field).includes(marker));assert.deepEqual(await readFile(join(root,'public','app-art','manifest.json')),before);
});
test('field public whitelist and source equality reject private fields and tampering',async t=>{
 const {root}=await setup(t);await importArtwork(root,{key:'field-hive',source:await image(root,true),visualApproved:true});
 const profilePath=join(root,'public','app-art','field-profile.json'),sourcePath=join(root,'src','world','app-art-field-profile.json');
 const saved=await readJson(profilePath);saved.description='PRIVATE_DESCRIPTION';await writeFile(profilePath,JSON.stringify(saved));await writeFile(sourcePath,JSON.stringify(saved));await assert.rejects(validatePublicArtwork(root),/private or unexpected/u);
 delete saved.description;await writeFile(profilePath,JSON.stringify(saved));await writeFile(sourcePath,JSON.stringify({...saved,aliases:{hive:'field-hive'}}));await assert.rejects(validatePublicArtwork(root),/Source and public field profiles differ/u);
});
test('a real field resolver build includes ready WebP and excludes private source identity, purpose, prompts and paths',async t=>{
 const {root}=await setup(t),definition={...spec('PRIVATE_LOCAL_KEY'.toLowerCase().replaceAll('_','-'),'field'),kind:'app',appId:id,name:'PRIVATE_FIELD_NAME',purpose:'PRIVATE_FIELD_PURPOSE',subject:'PRIVATE_FIELD_SUBJECT',alt:'PRIVATE_FIELD_ALT'};
 await prepareArtwork(root,{definitions:[definition]});
 const generated=join(root,'PRIVATE_FIELD_GENERATOR_BASENAME.png');await sharp({create:{width:1024,height:1024,channels:3,background:'#C8D6BF'}}).png().toFile(generated);
 await importArtwork(root,{key:definition.key,source:generated,visualApproved:true,generationId:'PRIVATE_FIELD_GENERATION_ID'});
 for(const name of ['field-art.mjs','app-art.mjs','app-art-identity.mjs'])await copyFile(new URL('../../src/world/'+name,import.meta.url),join(root,'src','world',name));
 await writeFile(join(root,'index.html'),'<html><body><script type="module" src="/entry.mjs"></script></body></html>');
 await writeFile(join(root,'entry.mjs'),"import {resolveFieldAppArt} from './src/world/field-art.mjs'; document.body.textContent=JSON.stringify(resolveFieldAppArt('hive')); ");
 const warnings=[],logger=createLogger('error',{allowClearScreen:false});logger.warn=logger.warnOnce=message=>warnings.push(String(message));
 await viteBuild({root,configFile:false,customLogger:logger,logLevel:'error',build:{outDir:join(root,'dist'),emptyOutDir:true}});
 assert.deepEqual(warnings,[]);
 const files=await readdir(join(root,'dist'),{recursive:true});assert.equal(files.filter(name=>name.endsWith('.webp')).length,8);
 for(const name of files){assert.ok(!/provenance\.json|source\.|app-art-history|registry\.json|jobs[\\/]/u.test(name));if(!/\.(?:json|html|js)$/u.test(name))continue;const bytes=await readFile(join(root,'dist',name),'utf8');for(const marker of ['PRIVATE_',definition.key,id,'promptHash','definitionHash','outputBasename','app-art-history'])assert.ok(!bytes.includes(marker),`Private field marker reached ${name}`);}
 assert.equal((await validatePublicArtwork(root)).valid,true);
});
test('cold checkout verifies field profile and refuses missing private history without losing either public manifest',async t=>{
 const {root:operator,before}=await setup(t),cold=await fixture(t);await aliasFieldArtwork(operator,{coverKey:'hive',key:'field-hive'});await importArtwork(operator,{key:'field-hive',source:await image(operator,true),visualApproved:true});
 for(const name of await readdir(join(operator,'public','app-art'),{recursive:true})){const source=join(operator,'public','app-art',name);if(!(await stat(source)).isFile())continue;const target=join(cold,'public','app-art',name);await mkdir(dirname(target),{recursive:true});await copyFile(source,target);}
 for(const name of ['app-art-manifest.json','app-art-field-profile.json']){await mkdir(join(cold,'src','world'),{recursive:true});await copyFile(join(operator,'src','world',name),join(cold,'src','world',name));}
 await rm(join(cold,'scripts','app-art','registry.json'));const fieldBefore=await readFile(join(cold,'public','app-art','field-profile.json'));assert.equal((await validatePublicArtwork(cold)).valid,true);await assert.rejects(aliasFieldArtwork(cold,{coverKey:'hive',key:'field-hive'}),/registry missing/u);assert.deepEqual(await readFile(join(cold,'public','app-art','field-profile.json')),fieldBefore);assert.deepEqual(await readFile(join(cold,'public','app-art','manifest.json')),before);
});
test('field version rollback keeps profile alias stable and exact card bytes untouched',async t=>{
 const {root,before}=await setup(t);await aliasFieldArtwork(root,{coverKey:'hive',key:'field-hive'});await importArtwork(root,{key:'field-hive',source:await image(root,true),visualApproved:true});const first=await readJson(join(root,'public','app-art','field-profile.json'));
 await importArtwork(root,{key:'field-hive',source:await image(root,true,'#ACB4AA'),visualApproved:true});await rollbackArtwork(root,{key:'field-hive',version:1});assert.deepEqual(await readJson(join(root,'public','app-art','field-profile.json')),first);assert.deepEqual(await readFile(join(root,'public','app-art','manifest.json')),before);assert.equal((await validateArtwork(root)).valid,true);
});
test('explicit field glyph updates have immutable layout proof and preserve generation, version, card and raster bytes',async t=>{
 const {root,before}=await setup(t);await importArtwork(root,{key:'field-hive',source:await image(root,true),visualApproved:true});
 const historyPath=join(root,'output','app-art-history','index.json'),accepted=(await readJson(historyPath)).assets['field-hive'];
 const cardTimes=await Promise.all([join(root,'public','app-art','manifest.json'),join(root,'src','world','app-art-manifest.json')].map(path=>stat(path)));
 const first=await configureFieldIcon(root,{key:'field-hive',icon:'cells'});assert.equal(first.version,1);assert.equal(first.unchanged,false);
 const next=(await readJson(historyPath)).assets['field-hive'];assert.deepEqual(next.original,accepted.original);assert.deepEqual(next.renditions,accepted.renditions);assert.deepEqual(next.provenance,accepted.provenance);assert.equal(next.layout.icon,'cells');
 assert.equal((await configureFieldIcon(root,{key:'field-hive',icon:'cells'})).unchanged,true);assert.equal((await validateArtwork(root)).valid,true);
 const profile=await readJson(join(root,'public','app-art','field-profile.json'));assert.equal(profile.assets['field-hive'].icon,'cells');assert.deepEqual(await readFile(join(root,'public','app-art','manifest.json')),before);
 for(const [index,path]of [join(root,'public','app-art','manifest.json'),join(root,'src','world','app-art-manifest.json')].entries())assert.equal((await stat(path)).mtimeMs,cardTimes[index].mtimeMs,'An identical card manifest must not be rewritten or trigger HMR');
 await assert.rejects(configureFieldIcon(root,{key:'hive',icon:'cells'}),/accepted field/u);await assert.rejects(configureFieldIcon(root,{key:'field-hive',icon:'https://tracker.invalid/icon'}),/approved local/u);
 await importArtwork(root,{key:'field-hive',source:await image(root,true,'#ACB4AB'),visualApproved:true});await rollbackArtwork(root,{key:'field-hive',version:1});assert.equal((await readJson(join(root,'public','app-art','field-profile.json'))).assets['field-hive'].icon,'cells');assert.equal((await validateArtwork(root)).valid,true);
 profile.assets['field-hive'].icon='PRIVATE_ICON';for(const path of [join(root,'public','app-art','field-profile.json'),join(root,'src','world','app-art-field-profile.json')])await writeFile(path,JSON.stringify(profile));await assert.rejects(validatePublicArtwork(root),/display icon/u);
});
