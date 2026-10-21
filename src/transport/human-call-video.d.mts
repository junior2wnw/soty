import type { CallIceReply } from './human-call-ice.mjs';
export interface CameraScope { readonly accountId:string; readonly deviceId:string; readonly callId:string; readonly epoch:number; readonly joinRef:string; readonly generation:number }
export interface CameraAdmission { readonly schema:'soty.human-camera-admission.v1'; readonly callId:string; readonly epoch:number; readonly accountId:string; readonly deviceId:string; readonly grantRef:string; readonly cameraGeneration:number; readonly expiresAt:number }
export interface CameraView { readonly state:'off'|'permission'|'publishing'|'on'; readonly configured:boolean; readonly ready:boolean; readonly code:string|null; readonly cleanupPending:boolean; readonly cleanupFailed:boolean; readonly ownedTracks:number }
export interface CameraLease { isCurrent():boolean; release():void }
export interface CaptureCoordinator { acquire(resource:'camera'|'microphone',owner:object):CameraLease|null; busy(resource:'camera'|'microphone'):boolean }
export interface CameraAck { readonly type:'call.video.published'; readonly callId:string; readonly epoch:number; readonly grantRef:string; readonly cameraGeneration:number }
export interface CameraLimits { readonly width:number; readonly height:number; readonly frameRate:number; readonly bitrate:number; readonly tracks:number }
export interface CameraStage { commit():Promise<CameraAck>; dispose():void|Promise<void> }
export interface CameraPrepare { readonly scope:CameraScope; readonly admission:CameraAdmission; readonly track:MediaStreamTrack; readonly limits:CameraLimits; readonly signal:AbortSignal }
export interface RemoteCamera { readonly deviceId:string; readonly accountId:string; readonly cameraGeneration:number; readonly track:MediaStreamTrack; readonly stream:MediaStream }
export interface HumanCallCameraOptions { readonly enabled:boolean; readonly coordinator:CaptureCoordinator;
  readonly acquire?:(constraints:MediaStreamConstraints,context:{owner:object;signal:AbortSignal})=>Promise<{owner:object;stream:MediaStream}>;
  readonly renderRemote?:(value:RemoteCamera)=>{close():void}; readonly onLocalStream?:(stream:MediaStream|null,owner:object)=>void }
export const HUMAN_VIDEO_LIMITS:Readonly<CameraLimits & {peers:3;candidates:32;messages:128;timeoutMs:10000}>;
export function createHumanCallCameraOwner(options:{ enabled:boolean; coordinator:CaptureCoordinator;
  authorize:(scope:CameraScope,context:{signal:AbortSignal})=>Promise<CameraAdmission>;
  acquire:(constraints:MediaStreamConstraints,context:{owner:object;signal:AbortSignal})=>Promise<{owner:object;stream:MediaStream}>;
  prepare:(value:CameraPrepare)=>CameraStage|Promise<CameraStage>;
  isCurrent:(scope:CameraScope)=>boolean; onState:(view:CameraView)=>void;
  onLocalStream?:(stream:MediaStream|null,owner:object)=>void; clock?:()=>number }):{
  snapshot():CameraView; enable():Promise<{ok:boolean;code?:string}>; setSession(scope:CameraScope|null):{ok:boolean;code?:string};
  setReady(value:boolean):CameraView; disable():CameraView; end():CameraView; dispose():CameraView;
};
export function createHumanCallVideoTransport(options:{scope:CameraScope;mediaInstance:string;
  send:(type:string,values:Record<string,unknown>)=>void;rtcFactory?:(config:RTCConfiguration)=>RTCPeerConnection;
  renderRemote?:(value:RemoteCamera)=>{close():void};onFault?:(code:string)=>void;clock?:()=>number}):{
  snapshot():Readonly<{configured:boolean;activePeers:number;retainedPeers:number;generation:number;cleanupFailed:boolean;unavailable:boolean;qualified:false}>;
  retry():void;setRoster(values:readonly unknown[]):unknown;noteAdmission(value:unknown):CameraAdmission;
  receive(message:unknown):void;prepare(value:CameraPrepare):CameraStage;setIce(value:CallIceReply):boolean;close():void;
};
