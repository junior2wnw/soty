async page=>{
  if(!page.url().startsWith('http://127.0.0.1:49335/'))throw Error('Own fixture required');
  await page.setViewportSize({width:1280,height:900});await page.evaluate(()=>{localStorage.removeItem('resize-boundary-document');localStorage.removeItem('resize-boundary-source-draft');});await page.reload();await page.waitForFunction(()=>window.fixture?.engine&&document.querySelector('.uf-resize-handle'));await page.evaluate(()=>window.fixture.engine.ready);
  const proof={schema:'soty.resize-boundary.browser.v1',rows:[],failures:[],startedAt:new Date().toISOString()};
  const masks=[{shape:'hex'},{shape:'ellipse'},{shape:'rect'},{shape:'polygon',points:[{x:0,y:0},{x:.8,y:0},{x:.8,y:.2},{x:.2,y:.2},{x:.2,y:.8},{x:0,y:.8}]}];
  for(const mask of masks)for(const scale of [.51,1,2]){
    await page.evaluate(async({mask,scale})=>{await window.fixture.shape(mask);window.fixture.engine.setCamera({x:0,y:0,scale});await new Promise(requestAnimationFrame);await new Promise(requestAnimationFrame);},{mask,scale});
    const row=await page.evaluate(({mask,scale})=>{
      const node=document.querySelector('.uf-node[data-shortcut-id="sample"]'),button=document.querySelector('.uf-resize-handle'),n=node.getBoundingClientRect(),r=button.getBoundingClientRect(),w=parseFloat(node.style.width),h=parseFloat(node.style.height),x=(r.x+r.width/2-n.x)/scale,y=(r.y+r.height/2-n.y)/scale;
      let inside;
      if(mask.shape==='hex'){
        const path=document.querySelector('#uf-hex-clip path'),matrix=path.transform.baseVal.consolidate().matrix.inverse();inside=(x,y)=>path.isPointInFill(new DOMPoint(x/w,y/h).matrixTransform(matrix));
      }else{
        const ctx=document.createElement('canvas').getContext('2d'),path=new Path2D();if(mask.shape==='ellipse')path.ellipse(w/2,h/2,w/2,h/2,0,0,Math.PI*2);else if(mask.shape==='rect')path.roundRect(0,0,w,h,Math.min(16,w/2,h/2));else{mask.points.forEach((p,i)=>i?path.lineTo(p.x*w,p.y*h):path.moveTo(p.x*w,p.y*h));path.closePath();}inside=(x,y)=>ctx.isPointInPath(path,x,y);
      }
      const samples=Array.from({length:16},(_,i)=>inside(x+Math.cos(i*Math.PI/8)/scale,y+Math.sin(i*Math.PI/8)/scale)),onBoundary=samples.some(Boolean)&&samples.some(v=>!v);
      return{shape:mask.shape,scale,center:{x,y},width:r.width,height:r.height,onBoundary,glyph:button.querySelector('svg')?'icon':getComputedStyle(button,'::before').content};
    },{mask,scale});proof.rows.push(row);if(!row.onBoundary||Math.abs(row.width-44)>.1||Math.abs(row.height-44)>.1)proof.failures.push({shape:mask.shape,scale,error:'native contour center / 44px'});
  }
  await page.evaluate(()=>{window.__frame=document.querySelector('iframe');window.__window=window.__frame.contentWindow;});
  const source=page.locator('iframe').contentFrame();await source.locator('textarea').fill('Synthetic retained source draft');const handle=page.locator('.uf-resize-handle');proof.drags=[];
  for(const scale of [.51,1,2]){
    await page.evaluate(async scale=>{await window.fixture.shape({shape:'hex'});await window.fixture.engine.setShortcutPlacement('sample',{kind:'free',x:0,y:0,width:320,height:240});window.fixture.engine.setCamera({x:0,y:0,scale});await new Promise(requestAnimationFrame);await new Promise(requestAnimationFrame);},scale);
    const r=await handle.boundingBox(),start={x:r.x+r.width/2,y:r.y+r.height/2},original=await page.evaluate(()=>window.fixture.engine.snapshot().shortcuts[0].placement);
    await page.mouse.move(start.x,start.y);await page.mouse.down();await page.mouse.move(start.x+40,start.y+30,{steps:4});
    const moved=await handle.boundingBox(),delta={x:moved.x+moved.width/2-start.x,y:moved.y+moved.height/2-start.y};await page.mouse.up();await page.waitForFunction(()=>window.fixture.summary.persistence==='saved');
    proof.pointer=await page.evaluate(()=>window.fixture.engine.snapshot().shortcuts[0].placement);proof.drags.push({scale,delta,placement:proof.pointer});
    if(Math.abs(delta.x-40)>.75||Math.abs(delta.y-30)>.75)proof.failures.push({scale,error:'grip does not follow captured pointer'});
    if(Math.abs(proof.pointer.x-proof.pointer.width/2-(original.x-original.width/2))>.01||Math.abs(proof.pointer.y-proof.pointer.height/2-(original.y-original.height/2))>.01)proof.failures.push({scale,error:'pointer moved fixed top-left'});
  }
  await handle.focus();await page.keyboard.press('ArrowRight');await page.keyboard.press('Shift+ArrowDown');await page.waitForFunction(()=>window.fixture.summary.persistence==='saved');proof.keyboard=await page.evaluate(()=>window.fixture.engine.snapshot().shortcuts[0].placement);
  if(Math.abs(proof.keyboard.width-proof.pointer.width-8)>.01||Math.abs(proof.keyboard.height-proof.pointer.height-1)>.01||proof.keyboard.x!==proof.pointer.x||proof.keyboard.y!==proof.pointer.y)proof.failures.push({error:'keyboard resize semantics changed'});
  proof.source=await page.evaluate(()=>({sameElement:window.__frame===document.querySelector('iframe'),sameWindow:window.__window===document.querySelector('iframe').contentWindow,creates:window.fixture.creates}));proof.source.value=await source.locator('textarea').inputValue();if(!proof.source.sameElement||!proof.source.sameWindow||proof.source.creates!==1||proof.source.value!=='Synthetic retained source draft')proof.failures.push({error:'resize replaced Source frame'});
  await page.screenshot({path:'output/playwright/resize-boundary-field.png'});await page.reload();await page.waitForFunction(()=>window.fixture?.engine);await page.evaluate(()=>window.fixture.engine.ready);proof.reopened=await page.evaluate(()=>window.fixture.engine.snapshot().shortcuts[0].placement);if(JSON.stringify(proof.reopened)!==JSON.stringify(proof.keyboard))proof.failures.push({error:'committed geometry did not survive reload'});
  proof.ok=!proof.failures.length;proof.finishedAt=new Date().toISOString();await page.evaluate(value=>window.__resizeProof=value,proof);return proof;
}
