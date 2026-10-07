import { fields,check,digest,syncResult } from './wire.mjs';
import { feedbackJobBudget,feedbackProcessorEngine } from './feedback-job-contract.mjs';

const ports=new WeakMap();
/** Reviewed host code supplies the exact Linux process executor. Branding is
 * constructor custody, not evidence of a sandbox: actual OS acceptance must
 * independently cover this executor/engine/pin/budget before production use. */
export function createFeedbackJobEnforcer(options){
  const value=fields(options,['engine','platform','maxBudget','syntheticTestOnly','execute'],['assertHostBounds']);
  const engine=feedbackProcessorEngine(value.engine);
  check(value.platform==='linux'&&typeof value.syntheticTestOnly==='boolean'&&typeof value.execute==='function'
    &&(!value.syntheticTestOnly||engine.synthetic),'source_feedback_enforcer_invalid',503);
  check(value.assertHostBounds===undefined||typeof value.assertHostBounds==='function');
  value.maxBudget=feedbackJobBudget(value.maxBudget);
  const port=Object.freeze({engineDigest:digest(engine.ref),syntheticTestOnly:value.syntheticTestOnly});ports.set(port,Object.freeze(value));return port;
}
export function feedbackJobEnforcer(port){const value=ports.get(port);check(value,'source_feedback_processor_not_ready',503);return value;}
/** Synchronous current host admission, read only outside SQL. No manifest,
 * localhost label or `process` function proves hard CPU/network enforcement. */
export function assertFeedbackHostBounds(port,budget){
  const value=feedbackJobEnforcer(port);budget=feedbackJobBudget(budget);
  if(value.syntheticTestOnly)return;
  check(process.platform==='linux'&&typeof value.assertHostBounds==='function'
    &&syncResult(value.assertHostBounds(Object.freeze({engineRef:feedbackProcessorEngine(value.engine).ref,budget})))===true,
    'source_feedback_processor_not_ready',503);
}
