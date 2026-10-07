async (page) => {
  const popup = page.context().pages().find(candidate => candidate.url().split('?')[0].endsWith('/soty/authorize'));
  if (!popup) throw new Error('native_popup_missing');
  await popup.goBack();
  const form = popup.getByRole('checkbox', { name: 'Подключить этот выбранный ресурс с моими текущими правами' });
  await form.check();
  const observed = popup.waitForRequest(request => request.url().split('?')[0].endsWith('/soty/authorize'));
  await popup.getByRole('button', { name: 'Войти через Соты', exact: true }).click();
  const request = await observed, headers = await request.allHeaders();
  return { synthetic: true, method: request.method(), origin: headers.origin ?? 'absent', contentType: headers['content-type']?.split(';')[0], credentialsLogged: false };
}
