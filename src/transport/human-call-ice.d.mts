export interface CallIceScope {accountId:string;deviceId:string;callId:string;epoch:number;joinRef:string}
export interface CallIceServer {readonly urls:readonly string[];readonly username?:string;readonly credential?:string}
export interface CallIceReply extends CallIceScope {readonly schema:'soty.human-call-ice.v1';readonly revision:number;readonly expiresAt:number;readonly iceServers:readonly CallIceServer[]}
export const CALL_ICE_LIMITS:Readonly<{servers:number;urlsPerServer:number;urlLength:number;usernameLength:number;credentialLength:number;ttlMinMs:number;ttlMaxMs:number}>;
export function closedIceRecord(value:unknown,required:readonly string[],optional?:readonly string[]):boolean;
export function canonicalCallIceUrl(value:unknown):string;
export function validateCallIceReply(value:unknown,scope:CallIceScope,options?:{now?:number;allowedUrls?:readonly string[]}):CallIceReply;
