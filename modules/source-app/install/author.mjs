import {readFile,writeFile,mkdir,lstat} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {createAuthorDraft} from '../../app-contract/sdk.mjs';
import {parseSourceJson} from '../shared/strict-json.mjs';
import {check} from '../server/wire.mjs';

/** Framework detection is convenience metadata. It never installs an auth
 * hook, executes package scripts, or converts a manifest into a Native grant. */
export async function createSourceAuthorDraft({directory,title}){
  check(typeof directory==='string'&&resolve(directory)===directory,'source_author_directory_invalid');
  const stat=await lstat(directory);check(stat.isDirectory()&&!stat.isSymbolicLink(),'source_author_directory_invalid');
  const draft=createAuthorDraft({title});let framework='plain-web';
  try{const file=join(directory,'package.json'),stat=await lstat(file);check(stat.isFile()&&!stat.isSymbolicLink()&&stat.size<=65536);
    const data=parseSourceJson(new TextDecoder('utf-8',{fatal:true}).decode(await readFile(file)),{bytes:65536});
    const deps={...data.dependencies,...data.devDependencies};
    if(Object.hasOwn(deps,'next'))framework='next';else if(Object.hasOwn(deps,'vite'))framework='vite';else if(Object.hasOwn(deps,'react'))framework='react';
    else if(Object.hasOwn(deps,'express')||Object.hasOwn(deps,'fastify'))framework='node-http';
  }catch(error){if(error.code!=='ENOENT')throw Object.assign(new Error('source_author_framework_inspection_failed'),{code:'source_author_framework_inspection_failed'});}
  const destination=join(directory,'.soty');await mkdir(destination,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});
  const destStat=await lstat(destination);check(destStat.isDirectory()&&!destStat.isSymbolicLink(),'source_author_directory_invalid');
  await writeFile(join(destination,'author.json'),JSON.stringify(draft,null,2)+'\n',{flag:'wx',mode:0o600});
  return{schema:'soty.source-author-result.v1',created:true,framework,manifest:'.soty/author.json',metadataOnly:true,connected:false,
    next:'approved-source-and-feature-consent'};
}
