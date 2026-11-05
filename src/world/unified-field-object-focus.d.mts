export interface ObjectFocusRect {left:number;top:number;width:number;height:number}
export function fieldObjectFocusFrame(view:{width:number;height:number},visible?:ObjectFocusRect):ObjectFocusRect&{barTop:number};
export function fieldObjectNeedsFocus(scale:number,previous?:boolean):boolean;
