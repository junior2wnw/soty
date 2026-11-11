import {readFile,writeFile,mkdir,lstat} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {check} from '../server/wire.mjs';

/** A new app uses the existing shared login. No credentials or grants are generated. */
export async function createRelayStarter({directory,title,port=30000}={}){
  check(typeof directory==='string'&&resolve(directory)===directory,'relay_starter_directory_invalid');
  check(typeof title==='string'&&title.trim()&&title.length<=80&&!/[\u0000-\u001f\u007f]/u.test(title),'relay_starter_title_invalid');
  check(Number.isSafeInteger(port)&&port>=1024&&port<=65535&&![49424,49425].includes(port),'relay_starter_port_invalid');
  const stat=await lstat(directory);check(stat.isDirectory()&&!stat.isSymbolicLink(),'relay_starter_directory_invalid');
  for(const name of ['app','.soty']){try{await lstat(join(directory,name));check(false,'relay_starter_exists',409);}catch(error){if(error.code!=='ENOENT')throw error;}}
  const bundled=typeof __SOTY_SOURCE_APP_BUNDLE__!=='undefined'&&__SOTY_SOURCE_APP_BUNDLE__===true;
  const input=new URL(bundled?'./templates/relay/':'../examples/relay-starter/',import.meta.url);
  // Read the finite template inventory before creating any project files.
  const templates=await Promise.all(['server.mjs','ui.html','README.md'].map(async name=>[name,await readFile(new URL(name,input))]));
  await mkdir(join(directory,'app'),{mode:0o700});await mkdir(join(directory,'.soty'),{mode:0o700});
  for(const [name,bytes]of templates)await writeFile(join(directory,'app',name),bytes,{flag:'wx',mode:0o644});
  await writeFile(join(directory,'.soty','app.json'),JSON.stringify({schema:'soty.local-app.v1',name:title.trim(),port,entryPath:'/'},null,2)+'\n',{flag:'wx',mode:0o600});
  await writeFile(join(directory,'.soty','app-auth.json'),JSON.stringify({schema:'soty.app-auth-configuration.v1',appId:null,issuer:null},null,2)+'\n',{flag:'wx',mode:0o600});
  return{schema:'soty.relay-starter.v1',created:true,authentication:'soty-window',contract:'soty.relay-window-identity.v1',
    manifest:'.soty/app.json',configuration:'.soty/app-auth.json',entry:'app/server.mjs',connected:false,
    next:'Register with the existing connector; set appId and trusted issuer from apps.authentication.get. Run node app/server.mjs --initialize only for a new app database. Read /agents/shared-auth.md.'};
}
