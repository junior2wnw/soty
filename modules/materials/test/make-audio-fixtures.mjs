// Maintainer helper. Synthetic tones only; not run during application startup/tests.
import {spawnSync} from 'node:child_process';
import {mkdirSync,writeFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
const directory=new URL('./fixtures/',import.meta.url);mkdirSync(directory,{recursive:true});
for(const [extension,codec,format] of [['webm','libopus','webm'],['ogg','libopus','ogg']]) {
  const result=spawnSync(process.argv[2]??'ffmpeg',['-hide_banner','-loglevel','error','-f','lavfi','-i','sine=frequency=440:duration=0.25','-ar','48000','-ac','1','-c:a',codec,'-f',format,'pipe:1'],{maxBuffer:1024*1024,windowsHide:true});
  if(result.status!==0)throw new Error(String(result.stderr));writeFileSync(fileURLToPath(new URL(`tone.${extension}`,directory)),result.stdout);
}
const video=spawnSync(process.argv[2]??'ffmpeg',['-hide_banner','-loglevel','error','-f','lavfi','-i','testsrc2=size=96x64:rate=8:duration=0.5','-f','lavfi','-i','sine=frequency=440:duration=0.5','-ar','48000','-ac','1','-c:v','libvpx','-deadline','realtime','-c:a','libopus','-shortest','-f','webm','pipe:1'],{maxBuffer:1024*1024,windowsHide:true});
if(video.status!==0)throw new Error(String(video.stderr));writeFileSync(fileURLToPath(new URL('video.webm',directory)),video.stdout);
