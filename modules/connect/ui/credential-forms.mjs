const material=(document,tag,role)=>{const node=document.createElement(tag);node.setAttribute('data-glass-role',role);node.setAttribute('data-glass-safe-area','off');return node;};
function input(host, title, { type = 'text', autocomplete = 'off', maxLength = 512, value = '' } = {}) {
  const document=host.ownerDocument??globalThis.document;
  const label=document.createElement('label'),field=material(document,'input','field');
  label.textContent = title; field.type = type; field.autocomplete = autocomplete; field.maxLength = maxLength;
  field.value = value; field.required = true; field.spellcheck = false; field.setAttribute('autocapitalize', 'none');
  label.append(field); host.append(label); return field;
}
function form(host, kind, { login = '', configuring = false } = {}) {
  const document=host.ownerDocument??globalThis.document;
  const details=document.createElement('details'),title=material(document,'summary','control');
  title.textContent = kind === 'password' ? 'Логин и пароль' : 'Фраза';
  const content = document.createElement('form'); content.dataset.credentialKind = kind;
  const name = kind === 'password' ? input(content, 'Логин', { autocomplete: 'username', maxLength: 160, value: login }) : null;
  const secret = input(content, kind === 'password' ? 'Пароль' : 'Фраза на любом языке', {
    type: 'password', autocomplete: configuring ? 'new-password' : kind === 'password' ? 'current-password' : 'off', maxLength: kind === 'password' ? 512 : 2048,
  });
  if (configuring) {
    const hint = document.createElement('p'); hint.className = 'connect-muted';
    hint.textContent = kind === 'password' ? 'От 15 символов. Можно использовать пробелы и любой язык.' : 'От 20 символов. Выберите длинную уникальную фразу на любом языке.';
    content.append(hint);
  }
  const submit=material(document,'button','control'); submit.type = 'submit'; submit.textContent = configuring ? 'Сохранить способ входа' : 'Открыть аккаунт';
  content.append(submit); details.append(title, content); host.append(details); return { details, content, name, secret, title };
}

export function mountCredentialLogin(host, { client, label, guard,begin,resume,signal,onComplete,activate,onDenied }) {
  for (const kind of ['password', 'phrase']) {
    const fields = form(host, kind);
    fields.content.addEventListener('submit', guard(async () => {
      begin();
      try {
        const run=snapshot=>client.loginWithCredential(kind,{...(fields.name?{login:fields.name.value}:{}),secret:fields.secret.value},label,{signal,...(snapshot?{expectedState:{accountId:snapshot.accountId,deviceId:snapshot.deviceId,localRevision:snapshot.localRevision}}:{})});
        const result=activate?await activate(run):await run();
        if(!result||signal.aborted){if(!signal.aborted)onDenied?.();return;}
        if (!signal.aborted) onComplete(result);
      } catch(error){if(error?.code==='ACTIVE_PROFILE_CHANGED'){if(!signal.aborted)onDenied?.();}else await resume();throw error;
      } finally { fields.secret.value = ''; }
    }));
  }
}

export async function mountCredentialSettings(host, { client, accountId, guard, isCurrent, onSaved }) {
  const document=host.ownerDocument??globalThis.document;
  const status=await client.credentialStatus(accountId);
  if (!isCurrent()) return;
  host.replaceChildren();
  for (const kind of ['password', 'phrase']) {
    const fields = form(host, kind, { login: status.password?.login ?? '', configuring: true });
    if (status[kind]?.enabled) fields.title.textContent += ' · включён';
    const refresh = () => mountCredentialSettings(host, { client, accountId, guard, isCurrent, onSaved });
    fields.content.addEventListener('submit', guard(async () => {
      try {
        await client.setAccountCredential(kind, { ...(fields.name ? { login: fields.name.value } : {}), secret: fields.secret.value }, accountId);
        if (isCurrent()) { onSaved('Способ входа сохранён. Он доступен на других устройствах.'); await refresh(); }
      } finally { fields.secret.value = ''; }
    }));
    if (status[kind]?.enabled) {
      const remove=material(document,'button','control'); remove.type = 'button'; remove.textContent = 'Отключить этот способ';
      remove.addEventListener('click', guard(async () => {
        await client.removeAccountCredential(kind, accountId);
        if (isCurrent()) { onSaved('Способ отключён. Устройства и остальные способы входа сохранены.'); await refresh(); }
      }));
      fields.content.append(remove);
    }
  }
}
