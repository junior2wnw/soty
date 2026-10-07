import {isDeepStrictEqual} from 'node:util';
const check=value=>{if(!value)throw Error('source_cold_guard_refused');};
export const SOURCE_COLD_NATIVE_REALM='cold-synthetic';
// Called only after the complete immutable container guard. This projection
// stays INSIDE encrypted metadata; none of its IDs, mounts or env are public.
export function sourceColdStoppedOriginal(actual){
  check(actual?.State?.Status==='exited'&&actual.State.Running===false&&actual.State.ExitCode===0&&actual.State.OOMKilled===false
    &&Number.isSafeInteger(actual.RestartCount)&&actual.RestartCount===0
    &&typeof actual.State.StartedAt==='string'&&typeof actual.State.FinishedAt==='string'
    &&actual.Mounts.some(m=>m.Type==='volume'&&m.Destination==='/data'));
  return {Id:actual.Id,Image:actual.Image,RestartCount:actual.RestartCount,
    State:{Status:actual.State.Status,Running:actual.State.Running,ExitCode:actual.State.ExitCode,OOMKilled:actual.State.OOMKilled,
      StartedAt:actual.State.StartedAt,FinishedAt:actual.State.FinishedAt},
    Mounts:actual.Mounts.map(m=>({...m})),
    Config:{User:actual.Config.User,WorkingDir:actual.Config.WorkingDir,Env:actual.Config.Env.slice(),
      Labels:{...actual.Config.Labels},Entrypoint:actual.Config.Entrypoint.slice(),Cmd:actual.Config.Cmd.slice()}};
}
export function assertSourceColdOriginalUnchanged(before,after){
  check(isDeepStrictEqual(before,after));return true;
}
export function sourceColdNativeCheckpoint(before){
  const reader=before?.data?.reader,identity=before?.data?.nativeIdentityDigest;
  check(reader?.realmId===SOURCE_COLD_NATIVE_REALM&&reader.format===3&&reader.objects===29&&/^[a-f0-9]{64}$/u.test(identity));
  return{schema:'soty.ordinary-native-checkpoint.v1',realmId:reader.realmId,readerFormat:reader.format,readerObjects:reader.objects,nativeIdentitySha256:identity};
}
const restoreCodes=new Set(['restore_authentication_failed','restore_incomplete','restore_archive_invalid','restore_limit_exceeded',
  'restore_timeout','restore_io_failed','restore_platform_unavailable','restore_target_invalid','restore_cleanup_pending']);
export function sourceColdFailureCode(error){
  const code=error&&Object.getOwnPropertyDescriptor(error,'code')?.value;
  return restoreCodes.has(code)||['source_cold_stream_unknown','source_cold_command_failed'].includes(code)?code:'source_cold_guard_refused';
}
