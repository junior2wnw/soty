import { ArrowDownToLine, ArrowRight, Check, CircleCheck, File, KeyRound, MessageSquare, Monitor, Plus } from 'lucide-react';
import { useEffect, useState } from 'react';
import { installerFile, request } from '../api';
import { downloadFile, priceLabel, rubles, shortModel } from '../format';
import type { ApiKey, ConnectionTab, ConnectProfile, Dashboard, Model } from '../types';
import { CopyField, InlineError, ModelAvatar, Spinner, Tabs, Toggle } from './Shared';

export function ConnectCard({ model, models, tab, onTabChange, auto, onAutoChange, onConnect, disabled }: { model?: Model; models: Model[]; tab: ConnectionTab; onTabChange: (tab: ConnectionTab) => void; auto: boolean; onAutoChange: (value: boolean) => void; onConnect: () => void; disabled?: boolean }) {
  const preview = (model ? [model, ...models.filter((candidate) => candidate.id !== model.id)] : models).slice(0, 3);
  return <section className="card connect-card" aria-labelledby="connect-heading"><h2 id="connect-heading" className="connect-title">{model ? shortModel(model) : 'Подключение'}</h2><Tabs selected={tab} onChange={onTabChange} />
    <div className="routing-diagram" aria-label="Один ключ для всех доступных моделей"><div className="key-node"><div><KeyRound size={37} strokeWidth={1.6} /></div><span>Мой ключ</span></div><svg className="route-lines" viewBox="0 0 146 188" fill="none" aria-hidden="true"><path d="M0 91H49Q67 91 67 72V44Q67 22 89 22H144M0 98H144M0 105H49Q67 105 67 125V151Q67 174 89 174H144" stroke="currentColor" strokeWidth="1.2" /><path d="m139 17 5 5-5 5m0 66 5 5-5 5m0 66 5 5-5 5" stroke="currentColor" strokeWidth="1.2" /></svg><div className="model-nodes">{preview.map((item) => <div key={item.id} className="model-node"><ModelAvatar model={item} small /><span><strong>{item.name}</strong><small>{item.version || 'Модель'}</small></span></div>)}{preview.length === 0 && <span className="muted">Ожидаем каталог</span>}</div>{models.length > 3 && <span className="more-models">+{models.length - 3} моделей</span>}</div>
    <button className="primary-button full-width" onClick={onConnect} disabled={disabled || !model}>{tab === 'chat' ? 'Открыть чат' : 'Подключить'}</button><div className="connect-auto"><Toggle checked={auto} onChange={onAutoChange} label="Авто" description="Смена модели при сбое" disabled={disabled} /></div>
  </section>;
}

export function ConnectPanel({ model, data, tab, onTabChange, onChat, onNewKey, auto, onAutoChange, secret, secretKeyId, notify, onChange }: { model?: Model; data: Dashboard; tab: ConnectionTab; onTabChange: (tab: ConnectionTab) => void; onChat: () => void; onNewKey: () => void; auto: boolean; onAutoChange: (value: boolean) => void; secret?: string; secretKeyId?: string; notify: (message: string) => void; onChange: () => void }) {
  const [profile, setProfile] = useState<ConnectProfile | null>(null);
  const [profileError, setProfileError] = useState<string | null>(null);
  const [reload, setReload] = useState(0);
  const [selectedKey, setSelectedKey] = useState(secretKeyId || data.keys.find((key) => key.status === 'active')?.id || '');
  const [enteredKey, setEnteredKey] = useState(secret || '');
  const [busy, setBusy] = useState(false);
  const [downloaded, setDownloaded] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const activeKeys = data.keys.filter((key) => key.status === 'active');
  const allowed = data.capabilities.managementEnabled;

  useEffect(() => {
    if (!model || tab !== 'api') return;
    let active = true;
    const controller = new AbortController();
    setProfileError(null); setProfile(null);
    request<ConnectProfile>(`/api/connect/profile?model=${encodeURIComponent(model.id)}${selectedKey ? `&keyId=${encodeURIComponent(selectedKey)}` : ''}`, { signal: controller.signal }).then((result) => { if (active) setProfile(result); }).catch((problem) => { if (active) setProfileError((problem as Error).message); });
    return () => { active = false; controller.abort(); };
  }, [model?.id, selectedKey, tab, reload]);

  async function downloadInstaller() {
    if (!model || busy) return;
    setBusy(true); setError(null);
    try { const blob = await installerFile(model.id, auto); downloadFile(blob, 'Soty-OpenCode.cmd', 'application/octet-stream'); setDownloaded(true); onChange(); notify('Откройте файл в течение 10 минут'); }
    catch (problem) { setError((problem as Error).message); }
    finally { setBusy(false); }
  }

  function downloadConfig() {
    if (!profile || !model) return;
    const config = {
      "$schema": "https://opencode.ai/config.json",
      provider: { soty: { npm: '@ai-sdk/openai-compatible', name: 'Соты', options: { baseURL: profile.baseUrl, apiKey: '{env:SOTY_API_KEY}' }, models: { [model.id]: { name: shortModel(model) } } } },
      model: `soty/${model.id}`,
    };
    downloadFile(JSON.stringify(config, null, 2) + '\n', 'opencode.soty.json', 'application/json;charset=utf-8');
    notify('Настройки скачаны. Ключ берётся из SOTY_API_KEY.');
  }

  return <div className={`connect-panel-content ${tab}`} aria-busy={busy}><Tabs selected={tab} onChange={onTabChange} />
    {!model ? <div className="panel-empty"><Monitor size={38} /><h3>Сначала выберите модель</h3></div> : tab === 'opencode' ? <>
      <div className="opencode-illustration" aria-hidden="true"><svg viewBox="0 0 230 150" fill="none"><rect x="32" y="16" width="166" height="106" rx="7" stroke="currentColor" strokeWidth="1.5" /><rect x="38" y="23" width="154" height="99" rx="3" stroke="#d4d4d1" /><path d="M16 122H91l2 3h44l2-3h75q-1 10-12 10H29q-12 0-13-10Z" fill="white" stroke="currentColor" strokeWidth="1.5" /><path d="m115 45 12 7v14l-12 7-12-7V52Z" stroke="#ffcb37" strokeWidth="3" /><path d="M115 86v22" stroke="#999" strokeWidth="1.6" strokeDasharray="3 5" /></svg></div>
      <div className="opencode-title"><h3>OpenCode</h3><span>Windows</span></div><div className="install-steps"><div><span><ArrowDownToLine size={28} strokeWidth={1.7} /></span><p>Скачать</p></div><ArrowRight className="step-arrow" size={29} strokeWidth={1} /><div><span><File size={28} strokeWidth={1.7} /></span><p>Открыть</p></div><ArrowRight className="step-arrow" size={29} strokeWidth={1} /><div><span><CircleCheck size={29} strokeWidth={1.7} /></span><p>Готово</p></div></div>
      <div className="selected-model-summary"><ModelAvatar model={model} small /><span><strong>{shortModel(model)}</strong><small>{priceLabel(model)}</small></span><span className="selection-dot checked"><Check size={18} /></span></div><div className="personal-key-summary"><span className="personal-key-icon"><KeyRound size={29} strokeWidth={1.6} /></span><span><strong>Личный ключ</strong><small>Создастся при установке</small></span></div>
      <InlineError>{error}</InlineError><button className="primary-button full-width installer-button" onClick={downloadInstaller} disabled={!allowed || !data.capabilities.installerEnabled || busy}>{busy ? <Spinner label="Готовим файл" /> : <><ArrowDownToLine size={25} />{downloaded ? 'Скачать ещё раз' : 'Скачать установщик'}</>}</button>{downloaded ? <p className="download-hint"><Check size={16} />Откройте скачанный файл в течение 10 минут.</p> : <p className="installer-compatibility">{data.capabilities.installerEnabled ? 'Windows 10 / 11 · x64 / ARM64' : 'Установщик ещё готовится'}</p>}
      <div className="panel-balance"><span>Баланс</span><strong>{data.wallet ? `${rubles(data.wallet.availableMicrorubles)} ₽` : '—'}</strong></div>
    </> : tab === 'api' ? <div className="api-connect"><div className="selected-model-summary"><ModelAvatar model={model} small /><span><strong>{shortModel(model)}</strong><small>{priceLabel(model)}</small></span><span className="selection-dot checked"><Check size={18} /></span></div><p className="panel-intro">Три значения для вашего приложения.</p>{profileError ? <><InlineError>{profileError}</InlineError><button className="secondary-button full-width" onClick={() => setReload((value) => value + 1)}>Повторить</button></> : !profile ? <div className="profile-loading"><Spinner label="Готовим настройки" /></div> : <>
      <CopyField label="Base URL" value={profile.baseUrl} /><CopyField label="Model ID" value={profile.model || model.id} />
      {activeKeys.length > 0 && <label className="form-field"><span>Ключ</span><select value={selectedKey} onChange={(event) => { setSelectedKey(event.target.value); setEnteredKey(''); }}>{activeKeys.map((key: ApiKey) => <option value={key.id} key={key.id}>{key.name} · {key.prefix}…</option>)}</select></label>}
      <CopyField label="API key" value={enteredKey} secret onChange={setEnteredKey} placeholder="Вставьте сохранённый ключ" />
      <button className="secondary-button full-width" onClick={onNewKey} disabled={!allowed}><Plus size={20} />{activeKeys.length ? 'Создать новый ключ' : 'Создать первый ключ'}</button><div className="api-format-note">OpenAI Chat Completions<span>/chat/completions</span></div><button className="text-button full-width" onClick={downloadConfig}><ArrowDownToLine size={18} />Настройки OpenCode</button>
    </>}<Toggle checked={auto} onChange={onAutoChange} label="Авто" description="Другую модель при сбое разрешает настройка ключа" /></div> : <div className="chat-connect"><div className="chat-welcome-icon"><MessageSquare size={45} strokeWidth={1.2} /></div><h3>{shortModel(model)}</h3><p className="panel-intro">Попробуйте модель прямо здесь.</p><button className="primary-button full-width" onClick={onChat} disabled={!allowed || !data.capabilities.chatEnabled}>Открыть чат<ArrowRight size={21} /></button><Toggle checked={auto} onChange={onAutoChange} label="Авто" description="Другая модель, если выбранная не отвечает" /><p className="field-help">Диалог хранится только в открытой вкладке.</p></div>}
  </div>;
}
