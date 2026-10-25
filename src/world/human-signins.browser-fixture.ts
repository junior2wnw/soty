// Disposable qualification entry: actual components/styles; RPC authority stays
// in the Node fixture's real signed Connect clients, not a browser identity DTO.
import { mountAccountCenter } from './account-center';
import { mountHumanSignins } from './human-signins';
import { createDialog } from './dialogs';
import { createPalette } from './theme/palette.mjs';
import './world.css';
import './product.css';
import './theme/theme.css';
const params = new URL(location.href).searchParams, theme = params.get('theme') === 'dark' ? 'dark' : 'light';
for (const [name, value] of Object.entries(createPalette(theme, 50))) document.documentElement.style.setProperty(name, value);
document.documentElement.dataset.sotyTheme = theme;
document.body.style.cssText = 'margin:0;background:var(--sw-bg);color:var(--sw-ink);font:15px system-ui';
const main = document.createElement('main'); main.className = 'sw-human-fixture'; main.style.cssText = 'padding:20px;max-width:700px;margin:auto';
const note = document.createElement('p'); note.textContent = 'Проверка интерфейса · только тестовые профили'; main.append(note);
const launch = document.createElement('button'); launch.type = 'button'; launch.className = 'sw-button'; launch.textContent = 'Профиль и настройки'; launch.disabled = true; main.append(launch); document.body.append(main);
let generation = 0, identity: {accountId: string; deviceId: string; label: string} | null = null;
let opened: ReturnType<typeof createDialog> | null = null, mounted: {dispose():void} | null = null;
async function select(): Promise<void> {
  const captured = ++generation; launch.disabled = true; identity = null; opened?.close(); mounted?.dispose(); opened = null; mounted = null;
  const selected = await (await fetch('/__fixture/identity', {cache:'no-store'})).json() as typeof identity;
  if (captured === generation) { identity = selected; launch.disabled = false; }
}
launch.addEventListener('click', () => {
  if (!identity) return;
  const captured = generation, active = identity, current = () => generation === captured;
  const menu = createDialog('Профиль и настройки', () => mounted?.dispose()); menu.element.classList.add('sw-account-dialog'); opened = menu;
  mounted = mountAccountCenter({host:menu.body,identity:{displayName:active.label},isCurrent:current,initialSection:'security',onAction:action=>{
    if (action !== 'human-signins' || !current()) return;
    menu.close({restoreFocus:false}); const dialog = createDialog('Входы в приложения', () => mounted?.dispose()); dialog.element.classList.add('sw-human-signins-dialog'); opened = dialog;
    mounted = mountHumanSignins({host:dialog.body,accountId:active.accountId,deviceId:active.deviceId,accountLabel:active.label,isCurrent:()=>current()&&dialog.element.open,
      api:{async request<T>(method:string,args?:Record<string,unknown>):Promise<T>{const response=await fetch('/__fixture/rpc',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({method,args})});
        const value=await response.json() as T&{code?:string};if(!response.ok)throw Object.assign(Error('fixture_rpc_failed'),{code:value.code??'fixture_rpc_failed'});return value;}}});
  }});
});
Object.assign(globalThis,{humanSigninFixture:{async profile(mode:'A'|'B'|'A-device2'){await fetch('/__fixture/profile',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({mode})});await select();},
  async dropNextAck(){await fetch('/__fixture/drop-next-ack',{method:'POST'});},async counts(){return(await fetch('/__fixture/counts')).json();}}});
await select();
