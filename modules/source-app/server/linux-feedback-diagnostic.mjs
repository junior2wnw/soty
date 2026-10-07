// Safe public failure projection: no message/stack/stderr/body/env/URL/identity.
const codes=new Set(['source_feedback_processor_not_ready','source_feedback_processor_unknown','source_feedback_processor_cleanup_unknown',
  'ordinary_feedback_job_outcome_unknown','ordinary_feedback_job_denied','ordinary_feedback_job_not_ready','ordinary_native_access_denied',
  'authentication_required','source_app_root_changed','source_app_commit_unknown','source_app_effect_unknown']);
const exits=new Set(['spawn_failed','timeout','output_limit','aborted','interrupted','nonzero','none']);
const groups=new Set(['identity','state','limits','cpu','hardening','namespaces','command','mounts']);
const fsCodes=new Set(['EACCES','EPERM','ENOENT','ENOTEMPTY','EBUSY','EEXIST','ENOSPC','EMFILE','ENFILE','EIO','ETIMEDOUT']);
const cleanupStages=new Set(['create_recovery','stop_inspect','stop_term','stop_reconcile','stop_after_term','stop_kill','stop_wait','attach_join',
  'final_inspect','container_rm','container_absence','packet_lstat','packet_guard','packet_realpath','packet_rm']);
const own=(value,key)=>value&&Object.getOwnPropertyDescriptor(value,key)?.value;
export function linuxFeedbackFailure(error){
  const code=own(error,'code'),diagnostic=own(error,'linuxCliDiagnostic'),mismatch=own(error,'specMismatchGroups');
  return {code:codes.has(code)?code:'unclassified',cliExitClass:exits.has(own(diagnostic,'exitClass'))?own(diagnostic,'exitClass'):'none',
    specMismatchGroups:Array.isArray(mismatch)?mismatch.filter(value=>groups.has(value)).slice(0,8):[]};
}
export function linuxFeedbackCleanupFailure(stage,error,reconciliation='unknown'){
  return {stage:cleanupStages.has(stage)?stage:'unknown',...linuxFeedbackFailure(error),
    fsClass:fsCodes.has(own(error,'code'))?own(error,'code'):'none',reconciliation:reconciliation==='stopped'?'stopped':'unknown'};
}
