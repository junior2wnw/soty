import {createHash} from 'node:crypto';
import {readFile, lstat, readdir} from 'node:fs/promises';
import {resolve, join, relative, sep} from 'node:path';

const directory=resolve('test-fixtures/planner-source-ci');
const need=value=>{if(!value)throw Error('maintained_source_ci_capsule_mismatch');};
const sha=value=>createHash('sha256').update(value).digest('hex');
const raw=await readFile(join(directory,'SOURCE-PINS.json'));
need(raw.length<=32768&&sha(raw)==='20f07b5f219a8b1cef60f1ff3c31287fdabae520d270acaa79c584be2d8dc1bd');
const manifest=JSON.parse(raw);
need(manifest.schema==='soty.maintained-source-ci-capsule.v1'
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
process.stdout.write(JSON.stringify({schema:'soty.maintained-source-ci-capsule-check.v1',passed:true,
  sourceRevision:manifest.sourceRevision,canonicalFiles:86,productionSourceInstalled:false})+'\n');
