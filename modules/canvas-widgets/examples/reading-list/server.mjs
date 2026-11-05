import {readFile} from 'node:fs/promises';
import {resolve,isAbsolute} from 'node:path';
import {pathToFileURL} from 'node:url';

const asset=name=>readFile(new URL('./assets/'+name,import.meta.url));
const html=value=>value.replaceAll('&','&amp;').replaceAll('"','&quot;').replaceAll('<','&lt;').replaceAll('>','&gt;');
export async function readingListAssets({sdkDirectory,glassCss,parentOrigin,widgetContextScript}){
 const origin=new URL(parentOrigin);if(origin.origin!==parentOrigin||!['http:','https:'].includes(origin.protocol))throw Error('reading_parent_origin_invalid');
 const [document,script,style,sdk]=await Promise.all([asset('embed.html'),asset('ui.js'),asset('ui.css'),readFile(resolve(sdkDirectory,'dist/browser.mjs'))]);
 const context=widgetContextScript??await readFile(new URL('../../../../vendor/optical-glass/src/widget-context.js',import.meta.url));
 return{document:document.toString().replace('__SOTY_PARENT_ORIGIN__',html(parentOrigin)),script:Buffer.concat([Buffer.from(context),Buffer.from('\n/* Reading List consumer */\n'),script]),
  style:Buffer.concat([Buffer.from(glassCss),Buffer.from('\n/* Reading List consumer */\n'),style]),sdk};
}

/** Author-selected code/keys only. The ordinary SDK owns routes, proof, ACL,
 * encrypted sessions, personal SQLite data and commit/receipt fences. */
export async function createReadingListServer({sdkDirectory,glassCss,widgetContextScript,profile,transportKey,connectorPort,databasePath,cipherKey,keyId,
 initialize=false,personalWorkspaces=false,listen,clock,ui={}}){
 if(!isAbsolute(sdkDirectory))throw Error('reading_sdk_directory_required');
 const sdk=await import(pathToFileURL(resolve(sdkDirectory,'dist/server.mjs')).href);
 const {createDomain}=await import(pathToFileURL(resolve(sdkDirectory,'dist/templates/reading-list/domain.mjs')).href);
 const defaults=await readingListAssets({sdkDirectory,glassCss,widgetContextScript,parentOrigin:profile.parentOrigin});
 const host=sdk.createSqliteRootWorkspaceHost({profile,databasePath,cipherKey,keyId,initialize,personalWorkspaces,title:'Список чтения',domain:createDomain(),...(clock?{clock}:{})});
 try{return sdk.createRootWorkspaceServer({profile,transportKey,connectorPort,host,ui:{...defaults,...ui},...(listen?{listen}:{}),...(clock?{clock}:{})});}
 catch(error){host.close();throw error;}
}
