/** Local types for the pinned, unmodified runtime subset. Not an upstream package declaration. */
export interface OpticalElement {target:HTMLElement}
export interface OpticalScene {
  gl:WebGL2RenderingContext|null;running:boolean;initialized:boolean;destroyed:boolean;elements:OpticalElement[];
  add(specification:Record<string,unknown>&{target:HTMLElement}):OpticalElement;
  remove(reference:OpticalElement|HTMLElement):boolean;
  start():Promise<OpticalScene>;stop():OpticalScene;refresh():void;
  getMetrics():{opticalPasses:number;shadedPixels:number};
  setBackground(source:string):Promise<OpticalScene>;destroy():OpticalScene;
}
export interface ShapePaint {context:CanvasRenderingContext2D;width:number;height:number;dpr:number}
export const OpticalGlass:{
  create(options:{root:Element;canvas:HTMLCanvasElement;background:string;dprCap?:number;fpsCap?:number}):OpticalScene;
  shape(draw:(paint:ShapePaint)=>void,options?:Record<string,unknown>):Record<string,unknown>;
};
export const LiquidGlass:typeof OpticalGlass;
export default OpticalGlass;
