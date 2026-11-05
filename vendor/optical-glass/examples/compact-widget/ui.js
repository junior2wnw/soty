// Only the example's model uses JS. Glass material and layout are CSS-only.
const form=document.getElementById('add'),input=document.getElementById('idea'),items=document.getElementById('items'),count=document.getElementById('count');
const original=items.innerHTML;
form.addEventListener('submit',event=>{event.preventDefault();const text=input.value.trim();if(!text)return;const item=document.createElement('li');item.textContent=text;items.append(item);count.value=items.children.length;form.reset();input.focus();});
document.getElementById('reset').addEventListener('click',()=>{items.innerHTML=original;count.value=items.children.length;});
