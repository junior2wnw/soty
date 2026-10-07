export interface CaptureSourcePin {readonly id:string;readonly version:number;readonly digest:string}
export function captureSourcePin(value:unknown):CaptureSourcePin|null;
export function matchesScopedCaptureContext(value:unknown,expected:{appId:string;source:CaptureSourcePin},now?:number):boolean;
