export type BootRuntimePhase = 'boot_admission_pending'|'boot_cookie_pending'|'boot_continuation_pending'|'boot_navigation'|'boot_failed'|'boot_inactive';
export type RootRuntimePhase = 'root_launch'|'root_launch_failed'|'root_frame'|'root_attached'|'root_load'|'root_error'|'root_timeout'|'root_boot_unobserved'|'boot_document_ack';
export type RuntimeCspPhase = `page_csp_${'frame'|'script'|'other'}_${'enforce'|'report'}`;
export type RuntimePhase = BootRuntimePhase|RootRuntimePhase|RuntimeCspPhase;
export interface RuntimeFrameSample { connected:boolean;hasArea:boolean;ancestorsVisible:boolean;hit:'all'|'some'|'none'|'unknown'; }
export interface RuntimePhaseSnapshot {
  schema:'soty.runtime-phase-trace.v1';surface:'main';observation:'partial';truncated:boolean;
  events:{t100ms:number;phase:RuntimePhase}[];view:RuntimeFrameSample|null;
}
export function sampleRuntimeFrame(frame:HTMLIFrameElement,view:Window):RuntimeFrameSample;
export function createAppRuntimePhaseTrace(options:{view:Window;isCurrent():boolean;getFrame():HTMLIFrameElement|null;getEpoch():number;canSample?():boolean;onChange?():void}):{
  arm():boolean;reset():void;beginManual():boolean;attach(frame:HTMLIFrameElement,origin:string):void;inserted():void;
  launchFailed():void;sample():void;snapshot():RuntimePhaseSnapshot|null;
  state():'empty'|'armed'|'limited'|'recording';copy():Promise<boolean>;clear():void;dispose():void;
};
