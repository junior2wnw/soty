function input(host, title, { type = 'text', autocomplete = 'off', maxLength = 512, value = '' } = {}) {
  const label = document.createElement('label'), field = document.createElement('input');
  label.textContent = title; field.type = type; field.autocomplete = autocomplete; field.maxLength = maxLength;
  field.value = value; field.required = true; field.spellcheck = false; field.setAttribute('autocapitalize', 'none');
  label.append(field); host.append(label); return field;
}
function form(host, kind, { login = '', configuring = false } = {}) {
  const details = document.createElement('details'), title = document.createElement('summary');
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
  const submit = document.createElement('button'); submit.type = 'submit'; submit.textContent = configuring ? 'Сохранить способ входа' : 'Открыть аккаунт';
  content.append(submit); details.append(title, content); host.append(details); return { details, content, name, secret, title };
}

export function mountCredentialLogin(host, { client, label, guard, begin, resume, signal, onComplete }) {
  for (const kind of ['password', 'phrase']) {
    const fields = form(host, kind);
    fields.content.addEventListener('submit', guard(async () => {
      begin();
      try {
        const result = await client.loginWithCredential(kind, { ...(fields.name ? { login: fields.name.value } : {}), secret: fields.secret.value }, label, { signal });
        if (!signal.aborted) onComplete(result);
      } catch (error) { await resume(); throw error;
      } finally { fields.secret.value = ''; }
    }));
  }
}

export async function mountCredentialSettings(host, { client, accountId, guard, isCurrent, onSaved }) {
  const status = await client.credentialStatus(accountId);
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
      const remove = document.createElement('button'); remove.type = 'button'; remove.textContent = 'Отключить этот способ';
      remove.addEventListener('click', guard(async () => {
        await client.removeAccountCredential(kind, accountId);
        if (isCurrent()) { onSaved('Способ отключён. Устройства и остальные способы входа сохранены.'); await refresh(); }
      }));
      fields.content.append(remove);
    }
  }
}
