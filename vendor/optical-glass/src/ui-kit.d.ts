import type {OpticalScene} from './liquid-glass.js';
export const GLASS_UI_WORKING_THEME:Record<string,unknown>;
export type GlassRenderPolicy='auto'|'static'|'optical';
export interface GlassRenderStatus {readonly policy:GlassRenderPolicy;readonly mode:'static'|'optical';readonly reason:string;readonly gpuReady:boolean}
export interface GlassUIHandle {
 readonly scene:OpticalScene|null;readonly canvas:HTMLCanvasElement|null;readonly renderMode:'static'|'optical';readonly renderPolicy:GlassRenderPolicy;readonly renderStatus:GlassRenderStatus;
 setRenderPolicy(policy:GlassRenderPolicy):Promise<unknown>;
 setRole(reference:Element|string,role:string,options?:Record<string,unknown>):Element[];
 removeRole(reference:Element|string):Element[];
 setTheme(theme:Record<string,unknown>):unknown;
 setBackground(source:string,options?:Record<string,unknown>):unknown;
 refresh():unknown;destroy():void;
}
export function createGlassUI(options:{root:HTMLElement;background?:string;canvasMode?:'root'|'viewport';renderPolicy?:GlassRenderPolicy;dprCap?:number;fpsCap?:number;autoDestroy?:boolean;roles?:Record<string,unknown>;theme?:Record<string,unknown>;onRenderStatus?:(status:GlassRenderStatus)=>void}):Promise<GlassUIHandle>;
