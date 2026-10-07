export interface ProcessingPin {readonly id:string;readonly version:number;readonly digest:string}
export interface ProcessingBudget {wallMs:number;cpuMs:number;cleanupMs:number;scratchBytes:number;outputBytes:number;mediaBytes:number;attachments:number;parallel:1}
export interface ProcessingPolicy {schema:'soty.feedback.processing-policy.v1';ref:ProcessingPin;purposes:('asr'|'ocr'|'triage')[];localOnly:true;requiresReporterConsent:true;maxBudget:ProcessingBudget}
export interface ProcessingEngine {readonly ref:ProcessingPin;readonly purposes:readonly ('asr'|'ocr'|'triage')[];readonly synthetic:boolean}
export interface ProcessingEnforcer {readonly engineDigest:string;readonly syntheticTestOnly:boolean}
export interface ProcessingIntent {readonly args:{readonly requestId:string;readonly input:Readonly<Record<string,unknown>>};readonly inputDigest:string}
export function createFeedbackProcessorEngine(options:{ref:ProcessingPin;purposes:('asr'|'ocr'|'triage')[];localOnly:true;synthetic:boolean;process:(input:unknown)=>Promise<unknown>}):ProcessingEngine;
export function createFeedbackJobEnforcer(options:{engine:ProcessingEngine;platform:'linux';maxBudget:ProcessingBudget;syntheticTestOnly:boolean;
  assertHostBounds?:(input:{engineRef:ProcessingPin;budget:ProcessingBudget})=>boolean;
  execute:(options:{engine:ProcessingEngine;input:unknown;purpose:'asr'|'ocr'|'triage';budget:ProcessingBudget;signal:AbortSignal})=>Promise<unknown>}):ProcessingEnforcer;
export function feedbackProcessingPolicy(input:ProcessingPolicy):Readonly<ProcessingPolicy>;
export const FEEDBACK_JOB_LIMITS:Readonly<ProcessingBudget&{grantSeconds:900}>;
export function createSourceFeedbackProcessingClient(options?:{fetch?:typeof fetch}):{
  context(signal?:AbortSignal):Promise<unknown>;ticket(ticketId:string,signal?:AbortSignal):Promise<unknown>;status(jobId:string,signal?:AbortSignal):Promise<unknown>;
  result(jobId:string,signal?:AbortSignal):Promise<unknown>;intent(input:Record<string,unknown>):Promise<ProcessingIntent>;
  apply(intent:ProcessingIntent,signal?:AbortSignal):Promise<unknown>;receipt(intent:ProcessingIntent,signal?:AbortSignal):Promise<unknown>;
};
