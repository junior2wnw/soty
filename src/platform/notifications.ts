import { button, el } from '../world/dom';
import type { NotificationsController, NotificationsView } from './notifications-client.mjs';

/** The app owns the controller across screens; closing settings leaves notifications enabled. */
export function mountNotifications(host:HTMLElement,controller:NotificationsController):{dispose():void} {
  const root=el('section'),label=el('h2','','Уведомления'),state=el('p','sw-muted'),actions=el('div','sw-row');state.setAttribute('role','status');
  const toggle=button('Включить уведомления','bell','sw-button-quiet',()=>{void(controller.view().enabled?controller.disable():controller.enable());});
  const retrying=()=>['push_notification_unavailable','push_navigation_blocked'].includes(controller.view().code??'');
  const check=button('Проверить','refresh','sw-button-quiet',()=>{void(retrying()?controller.retryOpen():controller.refresh());});
  const disable=button('Отключить на устройстве','close','sw-button-quiet',()=>{void controller.disable();});
  actions.append(toggle,check,disable);root.append(label,state,actions);host.append(root);
  const render=(value:NotificationsView):void=>{
    state.textContent=value.code==='push_subscription_bound'?'Подписка связана с другим профилем этого браузера. Отключите её на устройстве, затем включите заново.'
      :value.code==='push_endpoint_unsupported'?'Push-сервис этого браузера пока не поддерживается. Уведомления остаются выключены.'
      :value.code==='push_navigation_blocked'?'Сохраните или закройте незавершённую работу, затем повторите открытие сообщения.'
      :value.code==='push_notification_unavailable'?'Не удалось открыть сообщение. Проверьте связь и повторите. Доступ мог измениться.'
      :value.phase==='checking'?'Проверяем уведомления…'
      :!value.configured?'Фоновые уведомления пока не подключены.'
      :!value.supported?'В этом окне браузер пока не поддерживает фоновые уведомления.'
      :value.phase==='denied'?'Уведомления запрещены в браузере. Разрешите их в настройках сайта и нажмите «Проверить».'
      :value.phase==='unknown'?'Подтверждение не получено. Нажмите «Проверить»; подписка не повторяется автоматически.'
      :value.enabled?'Включены на этом устройстве. Текст сообщений на экране блокировки скрыт.'
      :'Получайте новые сообщения, даже когда Соты закрыты. Текст сообщений останется скрыт.';
    toggle.querySelector('span')!.textContent=value.enabled?'Выключить уведомления':'Включить уведомления';
    toggle.setAttribute('aria-pressed',String(value.enabled));toggle.disabled=value.busy||['checking','unavailable','denied','unknown'].includes(value.phase)||!value.configured||!value.supported;
    check.querySelector('span')!.textContent=retrying()?'Повторить открытие':'Проверить';
    check.disabled=value.busy||value.phase==='checking';check.hidden=!retrying()&&!['unavailable','unknown','denied'].includes(value.phase);
    disable.hidden=value.phase!=='unknown';disable.disabled=value.busy;
  };
  const unsubscribe=controller.subscribe(render);
  return {dispose(){unsubscribe();root.remove();}};
}
