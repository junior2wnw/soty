import {createHash} from 'node:crypto';
import {readFile, lstat, readdir} from 'node:fs/promises';
import {resolve, join, relative, sep} from 'node:path';
import {verifyMaintainedSourceCiSdk} from './build-maintained-source-ci-sdk.mjs';

const directory=resolve('test-fixtures/planner-source-ci');
const need=value=>{if(!value)throw Error('maintained_source_ci_capsule_mismatch');};
const sha=value=>createHash('sha256').update(value).digest('hex');
const raw=await readFile(join(directory,'SOURCE-PINS.json'));
need(raw.length<=32768&&sha(raw)==='9d359c79cfcf71ccd69091e65bac75c6ac144dc3747892aee913120c34c00882');
const manifest=JSON.parse(raw);
need(manifest.schema==='soty.maintained-source-ci-capsule.v2'
  &&manifest.sourceRevision==='6c614feb489767266ce23676b2bc0df6342f8370'
  &&manifest.productionSourceInstalled===false&&Object.keys(manifest.files).length===86);
const actual=[];
async function walk(path){
  for(const name of await readdir(path)){
    if(path===directory&&['node_modules','dist'].includes(name))continue;
    const entry=join(path,name),stat=await lstat(entry);
    need(!stat.isSymbolicLink());
    if(stat.isDirectory())await walk(entry);
    else{need(stat.isFile()&&stat.nlink===1);actual.push(relative(directory,entry).split(sep).join('/'));}
  }
}
await walk(directory);
need(actual.sort().join('\n')===[...Object.keys(manifest.files),'SOURCE-PINS.json'].sort().join('\n'));
for(const [name,pin] of Object.entries(manifest.files)){
  need(!name.startsWith('/')&&!name.includes('..')&&!name.includes('\\'));
  const bytes=await readFile(join(directory,name));
  need(bytes.length===pin.bytes&&sha(bytes)===pin.sha256);
}
const overlay=await verifyMaintainedSourceCiSdk(directory);
process.stdout.write(JSON.stringify({schema:'soty.maintained-source-ci-capsule-check.v1',passed:true,
  sourceRevision:manifest.sourceRevision,canonicalFiles:86,originalGitFiles:85,sdkOverlays:1,inputFiles:overlay.inputFiles,productionSourceInstalled:false})+'\n');
