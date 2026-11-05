async page=>{
 await page.waitForFunction(()=>!!window.widgetHost);
 const frame=page.frames().find(value=>value!==page.mainFrame());
 await frame.waitForFunction(()=>document.querySelector('#count').value!=='—');
 const state=()=>frame.evaluate(()=>({mode:document.querySelector('main').dataset.glassWidgetMode??null,
  editorVisible:document.querySelector('#content').getClientRects().length>0,
  visibleButtons:Array.from(document.querySelectorAll('button')).filter(node=>node.getClientRects().length>0).map(node=>node.textContent),
  timeOrigin:performance.timeOrigin}));
 const before=await state();
 if(before.mode!=='glance'||before.editorVisible||before.visibleButtons.length)throw Error('reading_preview_promises_actions:'+JSON.stringify(before));
 let requests=0;const observe=request=>{if(new URL(request.url()).pathname.startsWith('/api/embed/'))requests++;};page.on('request',observe);
 try{
  await page.evaluate(()=>window.widgetHost.setMode('interactive'));
  await frame.waitForFunction(()=>document.querySelector('main').dataset.glassWidgetMode==='interactive');
  await frame.getByLabel('Что прочитать',{exact:true}).fill('Не потерять при фокусе');
  await frame.evaluate(()=>{window.modeInputWitness=document.querySelector('#title');window.modeInputWitness.setSelectionRange(3,7);});
  await page.evaluate(()=>window.widgetHost.setMode('glance'));
  await frame.waitForFunction(()=>document.querySelector('main').dataset.glassWidgetMode==='glance');
  const glance=await state();if(glance.editorVisible||glance.visibleButtons.length)throw Error('reading_glance_shows_controls:'+JSON.stringify(glance));
  await page.evaluate(()=>window.widgetHost.setMode('interactive'));
  await frame.waitForFunction(()=>document.querySelector('main').dataset.glassWidgetMode==='interactive');
  const after=await state(),input=await frame.evaluate(()=>({same:window.modeInputWitness===document.querySelector('#title'),value:document.querySelector('#title').value,
   start:document.querySelector('#title').selectionStart,end:document.querySelector('#title').selectionEnd}));
  if(!input.same||input.value!=='Не потерять при фокусе'||input.start!==3||input.end!==7||after.timeOrigin!==before.timeOrigin||!after.editorVisible||requests!==0)
   throw Error('reading_context_replaced_input_or_authority:'+JSON.stringify({before,after,input,requests}));
  return{gate:'explicit-UI-only-context-not-viewport',before,glance,after,input,extraSourceApiRequests:requests};
 }finally{page.off('request',observe);}
}
