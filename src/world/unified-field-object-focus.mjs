/** Screen-space reading is temporary presentation, never a second placement. */
export function fieldObjectFocusFrame(view, visible = {left:0,top:0,width:view.width,height:view.height}) {
  const left=Math.max(0,visible.left),top=Math.max(0,visible.top);
  const right=Math.max(left,Math.min(view.width,visible.left+visible.width));
  const bottom=Math.max(top,Math.min(view.height,visible.top+visible.height));
  const inset=view.width<720?12:24,width=Math.max(1,Math.min(960,right-left-inset*2));
  const height=Math.max(1,Math.min(720,bottom-top-76));
  return {left:left+(right-left-width)/2,top:top+60,width,height,barTop:top+8};
}

/** Hysteresis keeps a window's controls from flickering around a zoom boundary. */
export function fieldObjectNeedsFocus(scale, previous=false) {
  return scale < (previous ? .86 : .78);
}
