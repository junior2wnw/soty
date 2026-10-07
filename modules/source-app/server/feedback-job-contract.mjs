import { fields, check, digest, jsonCopy, deepFreeze, requestId } from './wire.mjs';
import { validateSourceFeedbackInput } from './feedback-wire.mjs';

export const FEEDBACK_JOB_LIMITS = deepFreeze({ wallMs:60000,cpuMs:10000,cleanupMs:2000,scratchBytes:2097152,
  outputBytes:32768,mediaBytes:1048576,attachments:3,parallel:1,grantSeconds:900 });
const hash = value => typeof value === 'string' && /^[a-f0-9]{64}$/u.test(value);
const id = value => typeof value === 'string' && /^[A-Za-z0-9_.:-]{1,128}$/u.test(value);
const purpose = value => ['asr','ocr','triage'].includes(value);
const integer = (value,min,max) => Number.isSafeInteger(value) && value>=min && value<=max;
function pin(input) { const value=fields(input,['id','version','digest']);check(id(value.id)&&integer(value.version,1,2147483647)&&hash(value.digest),'source_feedback_job_pin_invalid');return Object.freeze(value); }
function strings(values,checkValue) { values=jsonCopy(values);check(Array.isArray(values)&&values.length>0&&values.length<=3&&new Set(values).size===values.length&&values.every(checkValue));return Object.freeze(values); }

/** Policy/contract data, NEVER Native permission. Source SQL supplies the real
 * current owner/support/session/reporter-consent proof at claim/final commit. */
export function feedbackProcessingPolicy(input) {
  const value=fields(input,['schema','ref','purposes','localOnly','requiresReporterConsent','maxBudget']);
  check(value.schema==='soty.feedback.processing-policy.v1'&&value.localOnly===true&&value.requiresReporterConsent===true,'source_feedback_processing_policy_denied');
  value.ref=pin(value.ref);value.purposes=strings(value.purposes,purpose);value.maxBudget=feedbackJobBudget(value.maxBudget);return deepFreeze(value);
}
export function feedbackJobBudget(input) {
  const value=fields(input,['wallMs','cpuMs','cleanupMs','scratchBytes','outputBytes','mediaBytes','attachments','parallel']);
  check(integer(value.wallMs,1,FEEDBACK_JOB_LIMITS.wallMs)&&integer(value.cpuMs,1,Math.min(value.wallMs,FEEDBACK_JOB_LIMITS.cpuMs))
    &&integer(value.cleanupMs,1,FEEDBACK_JOB_LIMITS.cleanupMs)&&integer(value.scratchBytes,1,FEEDBACK_JOB_LIMITS.scratchBytes)
    &&integer(value.outputBytes,1,FEEDBACK_JOB_LIMITS.outputBytes)&&integer(value.mediaBytes,1,FEEDBACK_JOB_LIMITS.mediaBytes)
    &&integer(value.attachments,0,FEEDBACK_JOB_LIMITS.attachments)&&value.parallel===1,'source_feedback_job_budget_invalid');return Object.freeze(value);
}
export function feedbackJobRequest(input) {
  const value=fields(input,['requestId','ticketId','ticketRevision','attachmentDigest','purpose','engineRef','policyRef','budget','expiresInSeconds']);
  check(requestId(value.requestId)&&id(value.ticketId)&&integer(value.ticketRevision,1,2147483647)&&hash(value.attachmentDigest)&&purpose(value.purpose)
    &&integer(value.expiresInSeconds,1,FEEDBACK_JOB_LIMITS.grantSeconds),'source_feedback_job_input_invalid');
  value.engineRef=pin(value.engineRef);value.policyRef=pin(value.policyRef);value.budget=feedbackJobBudget(value.budget);return deepFreeze(value);
}
/** Exact retained Native data. No URL/path/commands/Root actor/key may be added
 * by the job request or processor. Existing real media validation is reused. */
export function feedbackJobInput(input) {
  const value=fields(input,['ticketId','ticketRevision','body','attachments']);
  check(id(value.ticketId)&&integer(value.ticketRevision,1,2147483647));
  const media=validateSourceFeedbackInput('submit',{requestId:'retained-native-input',body:value.body,attachments:value.attachments});
  return deepFreeze({ticketId:value.ticketId,ticketRevision:value.ticketRevision,body:media.body,attachments:media.attachments});
}
export function feedbackAttachmentDigest(attachments) {
  const copied=jsonCopy(attachments,{bytes:1500000});
  check(Array.isArray(copied)&&copied.length<=FEEDBACK_JOB_LIMITS.attachments);
  // The parent input validator validates the actual bytes/container. This
  // closed digest covers ordered exact retained kind/name/type/bytes.
  for(const item of copied)fields(item,['kind','name','mimeType','dataBase64']);return digest(copied);
}
export function feedbackJobIntent(request,retained,policy) {
  request=feedbackJobRequest(request);retained=feedbackJobInput(retained);policy=feedbackProcessingPolicy(policy);
  check(request.ticketId===retained.ticketId&&request.ticketRevision===retained.ticketRevision
    &&request.attachmentDigest===feedbackAttachmentDigest(retained.attachments),'source_feedback_job_context_changed',409);
  check(digest(request.policyRef)===digest(policy.ref)&&policy.purposes.includes(request.purpose),'source_feedback_processing_policy_denied',403);
  for(const key of Object.keys(request.budget))check(request.budget[key]<=policy.maxBudget[key],'source_feedback_job_budget_invalid');
  const bytes=retained.attachments.reduce((sum,item)=>sum+Buffer.from(item.dataBase64,'base64').length,0);
  check(bytes<=request.budget.mediaBytes&&retained.attachments.length<=request.budget.attachments,'source_feedback_job_budget_invalid');
  check(request.purpose!=='asr'||retained.attachments.some(item=>item.kind==='audio'),'source_feedback_job_media_missing');
  check(request.purpose!=='ocr'||retained.attachments.some(item=>item.kind==='image'),'source_feedback_job_media_missing');
  return deepFreeze({request,inputDigest:digest(retained),attachmentDigest:request.attachmentDigest,policyRef:policy.ref});
}
/** Host-side SQL deadline facts are not a grant. Caller JSON cannot supply or
 * extend these deadlines: the Native service reads its CURRENT owned rows. */
export function assertFeedbackJobLifetime(facts,budget) {
  const {now,grantExpiresAt,sessionExpiresAt,keyExpiresAt,sourceProofExpiresAt}=fields(facts,['now','grantExpiresAt','sessionExpiresAt','keyExpiresAt','sourceProofExpiresAt']);
  budget=feedbackJobBudget(budget);check([now,grantExpiresAt,sessionExpiresAt,keyExpiresAt,sourceProofExpiresAt].every(value=>integer(value,0,Number.MAX_SAFE_INTEGER)));
  check(Math.min(grantExpiresAt,sessionExpiresAt,keyExpiresAt,sourceProofExpiresAt)-now>=budget.wallMs+budget.cleanupMs,'source_feedback_job_lifetime_insufficient',403);
}
const engines=new WeakMap();
/** Only approved trusted constructor code can create an engine port. Its
 * process runner must enforce real process/CPU/scratch/network bounds; a
 * localOnly label is not an OS sandbox or a transcription quality guarantee. */
export function createFeedbackProcessorEngine(options) {
  const value=fields(options,['ref','purposes','localOnly','synthetic','process']);
  check(value.localOnly===true&&typeof value.synthetic==='boolean'&&typeof value.process==='function','source_feedback_engine_invalid',503);
  value.ref=pin(value.ref);value.purposes=strings(value.purposes,purpose);const port=Object.freeze({ref:value.ref,purposes:value.purposes,synthetic:value.synthetic});engines.set(port,Object.freeze(value));return port;
}
export function feedbackProcessorEngine(port) { const value=engines.get(port);check(value,'source_feedback_engine_unapproved',503);return value; }
/** Bounded DATA only; no operations/status/commands/prompt-as-authority. Triage
 * suggestions require human review and never close/change the Native ticket. */
export function feedbackProcessorOutput(input,jobPurpose,budget) {
  budget=feedbackJobBudget(budget);check(purpose(jobPurpose));input=jsonCopy(input,{bytes:FEEDBACK_JOB_LIMITS.outputBytes});let value;
  if(jobPurpose==='triage'){value=fields(input,['kind','suggestedPriority','suggestedLabels','summary','explanation']);
    check(value.kind==='triage-suggestion'&&integer(value.suggestedPriority,0,100)&&Array.isArray(value.suggestedLabels)&&value.suggestedLabels.length<=8
      &&value.suggestedLabels.every(label=>typeof label==='string'&&label.isWellFormed()&&label.length<=80)
      &&typeof value.summary==='string'&&value.summary.isWellFormed()&&value.summary.length<=2000
      &&typeof value.explanation==='string'&&value.explanation.isWellFormed()&&value.explanation.length<=4000,'source_feedback_processor_output_invalid');
  }else{value=fields(input,['kind','text']);check(value.kind==='transcript'&&typeof value.text==='string'&&value.text.isWellFormed()&&value.text.length<=8000,'source_feedback_processor_output_invalid');}
  check(Buffer.byteLength(JSON.stringify(value))<=budget.outputBytes,'source_feedback_processor_output_invalid');return deepFreeze(value);
}
