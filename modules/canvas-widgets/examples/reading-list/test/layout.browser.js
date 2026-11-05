async page=>{
 const frame=page.frames().find(value=>value!==page.mainFrame());
 await frame.locator('#content').waitFor({state:'visible'});
 const iframe=page.locator('iframe'),rows=[];
 const read=()=>frame.evaluate(()=>{
  const box=selector=>{const r=document.querySelector(selector).getBoundingClientRect();return{x:r.x,y:r.y,width:r.width,height:r.height,bottom:r.bottom,right:r.right};};
  return{viewport:{width:innerWidth,height:innerHeight},scrollWidth:document.documentElement.scrollWidth,
   main:box('main'),count:box('#count'),content:box('.glass-ui__widget-content'),action:box('#submit'),
   inputPresent:!!document.querySelector('#title'),inputVisible:getComputedStyle(document.querySelector('#title')).display!=='none',
   static:getComputedStyle(document.querySelector('main')).backgroundColor,mode:document.querySelector('main').dataset.glassRenderMode,
   canvasCount:document.querySelectorAll('canvas').length};
 });
 const passes=value=>value.scrollWidth<=value.viewport.width+1&&value.main.width<=value.viewport.width+1
  &&value.action.width>=44&&value.action.height>=44&&value.action.right<=value.viewport.width+1
  &&value.action.bottom<=value.viewport.height+1&&value.count.y>=value.content.y&&value.count.bottom<=value.content.bottom+1
  &&value.inputPresent&&value.inputVisible&&value.mode==='static'&&value.canvasCount===0;
 for(const[width,height]of [[720,720],[280,240],[360,640],[220,160]]){
  await iframe.evaluate((node,size)=>{node.style.width=size[0]+'px';node.style.height=size[1]+'px';},[width,height]);
  await frame.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  const value=await read();rows.push({width,height,pass:passes(value),...value});
  if(!passes(value))throw Error('reading_layout_failed:'+JSON.stringify(rows.at(-1)));
 }
 await iframe.evaluate(node=>{node.style.width='280px';node.style.height='240px';});
 await frame.evaluate(()=>{document.querySelector('main').style.minWidth='1024px';});
 const broken=await read();if(passes(broken))throw Error('width1024_negative_control_not_detected');
 await frame.evaluate(()=>{document.querySelector('main').style.minWidth='';});
 const restored=await read();if(!passes(restored))throw Error('reading_layout_not_restored');
 await page.screenshot({path:'output/playwright/reading-widget-compact.png'});
 await iframe.evaluate(node=>{node.style.width='720px';node.style.height='720px';});
 await page.screenshot({path:'output/playwright/reading-widget-full.png'});
 return{gate:'reading-widget-real-Native-layout',rows,negativeControl:{pass:passes(broken),observed:broken},restored:passes(restored)};
}
