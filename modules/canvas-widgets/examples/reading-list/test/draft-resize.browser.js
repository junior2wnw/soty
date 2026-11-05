async page=>{
 const frame=page.frames().find(value=>value!==page.mainFrame());
 await frame.getByLabel('Что прочитать',{exact:true}).fill('Непрочитанная книга');
 await frame.evaluate(()=>{window.readingInputWitness=document.querySelector('#title');window.readingOriginWitness=performance.timeOrigin;
  window.readingInputWitness.setSelectionRange(3,8);});
 const results=[];
 for(const[width,height]of [[280,240],[360,360],[720,720]]){
  await page.locator('iframe').evaluate((node,size)=>{node.style.width=size[0]+'px';node.style.height=size[1]+'px';},[width,height]);
  await frame.evaluate(()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve))));
  const actual=await frame.evaluate(()=>({sameInput:window.readingInputWitness===document.querySelector('#title'),
   sameOrigin:window.readingOriginWitness===performance.timeOrigin,timeOrigin:performance.timeOrigin,value:document.querySelector('#title').value,
   start:document.querySelector('#title').selectionStart,end:document.querySelector('#title').selectionEnd,active:document.activeElement.id}));
  if(!actual.sameInput||!actual.sameOrigin||actual.value!=='Непрочитанная книга'||actual.start!==3||actual.end!==8||actual.active!=='title')throw Error('reading_draft_resize_failed:'+JSON.stringify(actual));
  results.push({width,height,...actual});
 }
 const dirty=await page.evaluate(()=>window.workStates.some(state=>state.dirty===true));
 if(!dirty)throw Error('standard_app_work_dirty_not_observed');
 return{gate:'reading-widget-live-input-not-remounted',results,standardAppWorkDirty:dirty};
}
