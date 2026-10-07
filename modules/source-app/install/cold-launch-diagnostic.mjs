// Closed public projection only. Docker Error text, stderr, config and tokens
// must never be returned; classification reads own data properties only.
const own=(value,key)=>value&&Object.getOwnPropertyDescriptor(value,key)?.value;
const exits=new Set(['spawn_failed','timeout','output_limit','aborted','interrupted','nonzero','none']);
const states=new Set(['created','running','exited','dead','restarting','paused','removing']);
function stateErrorClass(input){
  if(input==='')return'none';
  if(typeof input!=='string'||input.length>16384)return'unknown';
  if(/(?:error mounting|failed to mount|mount[^\n]*(?:read-only file system|no such file))/iu.test(input))return'mount_setup_failed';
  if(/(?:exec|executable)[^\n]*(?:not found|permission denied|no such file)/iu.test(input))return'entry_unavailable';
  if(/(?:permission denied|operation not permitted)/iu.test(input))return'permission_denied';
  if(/read-only file system/iu.test(input))return'read_only_filesystem';
  return'other';
}
export function sourceColdLaunchDiagnostic(error,state){
  const cli=own(own(error,'linuxCliDiagnostic'),'exitClass'),status=own(state,'Status'),exit=own(state,'ExitCode');
  return Object.freeze({cliExitClass:exits.has(cli)?cli:'none',stateClass:states.has(status)?status:'unknown',
    stateErrorClass:stateErrorClass(own(state,'Error')),
    containerExitClass:exit===0?'zero':Number.isSafeInteger(exit)&&exit>0&&exit<=255?'nonzero':'unknown'});
}
