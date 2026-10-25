import { ArrowDownToLine, Check, ChevronLeft, ChevronRight, CodeXml, Download, KeyRound, MoreHorizontal, Pause, Play, Plus, RefreshCw, Search, ShieldCheck, Smartphone, UserRound, WifiOff } from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { getDashboard, mutation } from './api';
import { keyStatus, micro, rubles } from './format';
import type { ApiKey, ChatMessage, ConnectionTab, Dashboard, Panel } from './types';
import Chat from './components/Chat';
import { ConnectCard, ConnectPanel } from './components/Connect';
import Dialog from './components/Dialog';
import KeyPanel from './components/KeyPanel';
import { ModelDetails, Models } from './components/Models';
import TopupPanel from './components/TopupPanel';
import { UsageCard, UsageDetails } from './components/UsageChart';
import WorkspaceInfo from './components/WorkspaceInfo';

interface Preferences { model: string; tab: ConnectionTab; days: number; auto: boolean }
interface Route { panel: Panel; keyId?: string; chat: boolean }
interface InstallPrompt extends Event { prompt(): Promise<void>; userChoice: Promise<{ outcome: 'accepted' | 'dismissed' }> }

function preferences(): Preferences {
  try {
    const saved = JSON.parse(localStorage.getItem('soty-ui-preferences') || '{}');
    return { model: typeof saved.model === 'string' ? saved.model : '', tab: ['chat', 'api', 'opencode'].includes(saved.tab) ? saved.tab : 'api', days: [7, 30, 90].includes(saved.days) ? saved.days : 7, auto: saved.auto === true };
  } catch { return { model: '', tab: 'api', days: 7, auto: false }; }
}

function currentRoute(): Route {
  const parts = location.hash.replace(/^#/, '').split('/');
  const panels: Panel[] = ['connect', 'key', 'new-key', 'topup', 'models', 'usage', 'workspace', 'install'];
  let keyId: string | undefined;
  try { keyId = parts[1] ? decodeURIComponent(parts[1]) : undefined; } catch { /* A malformed deep link must not break the application. */ }
  return { panel: panels.includes(parts[0] as Panel) ? parts[0] as Panel : null, keyId, chat: parts[0] === 'chat' || history.state?.sotyBase === 'chat' };
}

function Logo() { return <span className="brand"><svg viewBox="0 0 38 42" fill="none" aria-hidden="true"><path d="M19 4 34 12.5v17L19 38 4 29.5v-17Z" stroke="currentColor" strokeWidth="4" strokeLinejoin="round" /></svg><span>соты</span></span>; }

function LoadingDashboard() { return <div className="dashboard loading-dashboard" aria-label="Загрузка кабинета" aria-busy="true"><div className="skeleton title-skeleton" /><div className="model-grid">{[0, 1, 2].map((item) => <div key={item} className="card skeleton model-skeleton" />)}</div><div className="workspace-grid"><div className="card loading-card"><div className="skeleton medium-skeleton" /><div className="skeleton amount-skeleton" /><div className="loading-chart"><div /><div /><div /></div></div><div className="card loading-card"><div className="skeleton medium-skeleton" /><div className="skeleton tabs-skeleton" /><div className="skeleton button-skeleton" /></div></div><div className="card loading-keys"><div className="skeleton medium-skeleton" /></div></div>; }

function KeysCard({ keys, enabled, onCreate, onEdit, onChange, notify }: { keys: ApiKey[]; enabled: boolean; onCreate: () => void; onEdit: (key: ApiKey) => void; onChange: () => void; notify: (message: string) => void }) {
  const [search, setSearch] = useState('');
  const [page, setPage] = useState(0);
  const [busyId, setBusyId] = useState<string | null>(null);
  const filtered = keys.filter((key) => `${key.name} ${key.prefix}`.toLowerCase().includes(search.toLowerCase()));
  const effectivePage = Math.min(page, Math.max(0, Math.ceil(filtered.length / 10) - 1));
  const visible = filtered.slice(effectivePage * 10, effectivePage * 10 + 10);
  async function pause(key: ApiKey) {
    if (busyId || (key.status !== 'active' && key.status !== 'paused')) return;
    setBusyId(key.id);
    try { await mutation(`/api/keys/${encodeURIComponent(key.id)}`, 'PATCH', { status: key.status === 'paused' ? 'active' : 'paused' }); onChange(); notify(key.status === 'paused' ? 'Ключ снова активен' : 'Ключ на паузе'); }
    catch (problem) { notify((problem as Error).message); }
    finally { setBusyId(null); }
  }
  return <section className="card keys-card" aria-labelledby="keys-heading"><div className="keys-heading"><h2 id="keys-heading">Ключи</h2>{keys.length > 8 && <label className="key-search"><Search size={16} /><input value={search} onChange={(event) => { setSearch(event.target.value); setPage(0); }} placeholder="Найти ключ" aria-label="Найти ключ" /></label>}<button className="icon-button add-key" onClick={onCreate} disabled={!enabled} aria-label="Создать ключ"><Plus size={22} strokeWidth={1.8} /></button></div>
    {visible.length > 0 ? <div className="key-list">{visible.map((key) => <div key={key.id} className={`key-row ${key.status}`}><span className="key-row-icon" aria-hidden="true">{/code/i.test(key.name) ? <CodeXml size={23} strokeWidth={1.55} /> : <UserRound size={23} strokeWidth={1.55} />}</span><button className="key-name" onClick={() => onEdit(key)} title={`${key.name} · ${key.prefix}…`}><span>{key.name}</span>{key.status !== 'active' && <small>{keyStatus(key.status).label}</small>}</button><span className="key-spend">{rubles(key.spentMicrorubles)} ₽</span><div className="key-row-actions"><button className="icon-button key-pause" onClick={() => pause(key)} disabled={!enabled || busyId !== null || (key.status !== 'active' && key.status !== 'paused')} aria-label={key.status === 'paused' ? `Возобновить ${key.name}` : `Приостановить ${key.name}`}>{busyId === key.id ? <RefreshCw className="spin" size={17} /> : key.status === 'paused' ? <Play size={18} strokeWidth={1.6} /> : <Pause size={18} strokeWidth={1.6} />}</button><button className="icon-button" onClick={() => onEdit(key)} aria-label={`Настройки ключа ${key.name}`}><MoreHorizontal size={22} /></button></div></div>)}</div> : <div className="keys-empty"><KeyRound size={28} strokeWidth={1.4} /><div><strong>{search ? 'Ключ не найден' : 'Один ключ — все модели'}</strong><span>{search ? 'Попробуйте другое название' : 'Создайте первый ключ для приложения или агента'}</span></div>{!search && <button className="secondary-button" onClick={onCreate} disabled={!enabled}>Создать ключ<Plus size={18} /></button>}</div>}
    {filtered.length > 10 && <div className="key-pagination"><span>{effectivePage * 10 + 1}–{Math.min(effectivePage * 10 + 10, filtered.length)} из {filtered.length}</span><button className="icon-button" onClick={() => setPage(effectivePage - 1)} disabled={effectivePage === 0} aria-label="Предыдущие ключи"><ChevronLeft size={18} /></button><button className="icon-button" onClick={() => setPage(effectivePage + 1)} disabled={(effectivePage + 1) * 10 >= filtered.length} aria-label="Следующие ключи"><ChevronRight size={18} /></button></div>}
  </section>;
}

export default function App() {
  const [prefs, setPrefs] = useState<Preferences>(preferences);
  const [data, setData] = useState<Dashboard | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(true);
  const [online, setOnline] = useState(navigator.onLine);
  const [route, setRoute] = useState<Route>(currentRoute);
  const [toast, setToast] = useState<string | null>(null);
  const [transientSecret, setTransientSecret] = useState<string | undefined>();
  const [transientKeyId, setTransientKeyId] = useState<string | undefined>();
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [installPrompt, setInstallPrompt] = useState<InstallPrompt | null>(null);
  const [installed, setInstalled] = useState(window.matchMedia('(display-mode: standalone)').matches);
  const refreshRef = useRef<() => void>(() => undefined);
  const toastTimeout = useRef<number | undefined>(undefined);

  const notify = useCallback((message: string) => { setToast(message); clearTimeout(toastTimeout.current); toastTimeout.current = window.setTimeout(() => setToast(null), 4200); }, []);
  const refresh = useCallback(() => refreshRef.current(), []);
  const updatePrefs = (changes: Partial<Preferences>) => setPrefs((previous) => ({ ...previous, ...changes }));

  useEffect(() => { try { localStorage.setItem('soty-ui-preferences', JSON.stringify(prefs)); } catch { /* The workspace does not depend on local storage. */ } }, [prefs]);
  useEffect(() => {
    let active = true;
    let running = false;
    const controller = new AbortController();
    async function load() {
      if (running) return;
      running = true;
      if (active) setRefreshing(true);
      try { const result = await getDashboard(prefs.days, controller.signal); if (active) { setData(result); setError(null); } }
      catch (problem) { if (active) setError((problem as Error).message); }
      finally { running = false; if (active) setRefreshing(false); }
    }
    refreshRef.current = load;
    void load();
    const interval = window.setInterval(() => { if (document.visibilityState === 'visible') void load(); }, 10_000);
    const onVisible = () => { if (document.visibilityState === 'visible') void load(); };
    const onOnline = () => { setOnline(true); void load(); };
    const onOffline = () => setOnline(false);
    document.addEventListener('visibilitychange', onVisible); window.addEventListener('online', onOnline); window.addEventListener('offline', onOffline);
    return () => { active = false; controller.abort(); clearInterval(interval); document.removeEventListener('visibilitychange', onVisible); window.removeEventListener('online', onOnline); window.removeEventListener('offline', onOffline); };
  }, [prefs.days]);
  useEffect(() => {
    const changed = () => setRoute(currentRoute());
    window.addEventListener('popstate', changed); window.addEventListener('hashchange', changed);
    return () => { window.removeEventListener('popstate', changed); window.removeEventListener('hashchange', changed); };
  }, []);
  useEffect(() => { if (route.panel !== 'connect') { setTransientSecret(undefined); setTransientKeyId(undefined); } }, [route.panel]);
  useEffect(() => {
    const before = (event: Event) => { event.preventDefault(); setInstallPrompt(event as InstallPrompt); };
    const installedEvent = () => { setInstalled(true); setInstallPrompt(null); notify('Соты установлены'); };
    window.addEventListener('beforeinstallprompt', before); window.addEventListener('appinstalled', installedEvent);
    return () => { window.removeEventListener('beforeinstallprompt', before); window.removeEventListener('appinstalled', installedEvent); };
  }, [notify]);

  function openPanel(panel: Exclude<Panel, null>, keyId?: string) {
    const url = `${location.pathname}${location.search}#${panel}${keyId ? `/${encodeURIComponent(keyId)}` : ''}`;
    const state = { sotyPanel: true, sotyBase: route.chat ? 'chat' : '' };
    if (route.panel) history.replaceState(state, '', url); else history.pushState(state, '', url);
    setRoute(currentRoute());
  }
  function closePanel() {
    setTransientSecret(undefined); setTransientKeyId(undefined);
    if (history.state?.sotyPanel) history.back();
    else { history.replaceState({}, '', `${location.pathname}${location.search}${route.chat ? '#chat' : ''}`); setRoute(currentRoute()); }
  }
  function showChat() {
    if (route.panel) history.replaceState({}, '', '#chat'); else history.pushState({}, '', '#chat');
    setRoute(currentRoute());
  }
  function hideChat() { history.replaceState({}, '', `${location.pathname}${location.search}`); setRoute(currentRoute()); }
  async function installApp() {
    if (installPrompt) { await installPrompt.prompt(); await installPrompt.userChoice; setInstallPrompt(null); }
    else openPanel('install');
  }
  const model = data?.models.find((candidate) => candidate.id === prefs.model) || data?.models[0];
  const key = data?.keys.find((candidate) => candidate.id === route.keyId);
  const management = data?.capabilities.managementEnabled || false;
  const panelTitle = route.panel === 'connect' ? 'Подключить' : route.panel === 'new-key' ? 'Новый ключ' : route.panel === 'key' ? 'Настройки ключа' : route.panel === 'topup' ? 'Пополнить' : route.panel === 'models' ? 'Модели и цены' : route.panel === 'usage' ? 'Статистика' : route.panel === 'install' ? 'Соты как приложение' : 'Ваш кабинет';

  return <><a className="skip-link" href="#main">К содержимому</a><header className="topbar"><div className="topbar-content"><a className="brand-link" href="#" onClick={(event) => { event.preventDefault(); if (route.panel) closePanel(); else hideChat(); }} aria-label="Соты — кабинет"><Logo /></a><button className="project-badge" onClick={() => openPanel('workspace')}>Демо</button><div className="header-actions">{!installed && <button className="icon-button pwa-install" onClick={installApp} aria-label="Установить Соты как приложение" title="Установить приложение"><Download size={21} strokeWidth={1.8} /></button>}<button className="wallet-button" onClick={() => openPanel('topup')} disabled={!data} aria-label="Баланс и пополнение">{data?.wallet ? `${rubles(data.wallet.availableMicrorubles)} ₽` : '— ₽'}</button><button className="icon-button topup-button" onClick={() => openPanel('topup')} disabled={!management} aria-label="Пополнить баланс"><Plus size={27} strokeWidth={1.7} /></button><button className="avatar-button" onClick={() => openPanel('workspace')} aria-label="Мой временный кабинет">А</button></div></div></header>
    {!data ? error ? <main className="initial-error" id="main"><div className="error-symbol">{online ? <RefreshCw size={32} strokeWidth={1.4} /> : <WifiOff size={32} strokeWidth={1.4} />}</div><h1>{online ? 'Кабинет временно недоступен' : 'Нет подключения к сети'}</h1><p>{error}</p><button className="primary-button" onClick={refresh} disabled={refreshing}><RefreshCw size={19} className={refreshing ? 'spin' : ''} />Попробовать ещё раз</button></main> : <LoadingDashboard /> : <main className="dashboard" id="main"><Models models={data.models} selectedId={model?.id || ''} onSelect={(id) => updatePrefs({ model: id })} onDetails={() => openPanel('models')} />
      {route.chat ? <Chat model={model} messages={messages} setMessages={setMessages} auto={prefs.auto} setAuto={(auto) => updatePrefs({ auto })} onClose={hideChat} onChange={refresh} onTopup={() => openPanel('topup')} balanceEmpty={!data.wallet || micro(data.wallet.availableMicrorubles) <= 0n} /> : <div className="workspace-grid"><UsageCard usage={data.usage} days={prefs.days} onDaysChange={(days) => updatePrefs({ days })} onDetails={() => openPanel('usage')} /><ConnectCard model={model} models={data.models} tab={prefs.tab} onTabChange={(tab) => updatePrefs({ tab })} auto={prefs.auto} onAutoChange={(auto) => updatePrefs({ auto })} onConnect={() => prefs.tab === 'chat' ? showChat() : openPanel('connect')} disabled={!management} /></div>}
      <KeysCard keys={data.keys} enabled={management} onCreate={() => openPanel('new-key')} onEdit={(selected) => openPanel('key', selected.id)} onChange={refresh} notify={notify} /><footer className="dashboard-footer"><button className={`refresh-status ${error || !online ? 'has-error' : ''}`} onClick={refresh} disabled={refreshing}><span className={`status-dot ${refreshing ? 'updating' : ''}`} />{!online ? 'Нет сети' : error ? 'Данные не обновились' : data.updatedAt ? `Обновлено ${new Date(data.updatedAt).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}` : 'Обновление каждые 10 секунд'}<RefreshCw size={12} className={refreshing ? 'spin' : ''} /></button><span>Демо · без реальной оплаты</span></footer>{error && <div className="stale-notice" role="status">{error} Показаны последние полученные данные.</div>}
    </main>}
    {route.panel && <Dialog title={panelTitle} onClose={closePanel} compact={route.panel === 'topup' || route.panel === 'workspace' || route.panel === 'install'} className={route.panel === 'connect' ? 'connect-dialog' : ''}>
      {route.panel === 'workspace' ? <WorkspaceInfo data={data} installed={installed} onInstall={installApp} onModels={() => openPanel('models')} onChange={refresh} notify={notify} />
        : route.panel === 'install' ? <div className="install-app-panel"><div className="app-icon"><Logo /></div><h3>Всегда под рукой</h3><p className="panel-intro">Открывайте кабинет с рабочего стола.</p>{installPrompt ? <button className="primary-button full-width" onClick={installApp}><ArrowDownToLine size={20} />Установить Соты</button> : <div className="install-instructions"><p><strong>Chrome / Edge</strong><span>Меню браузера → «Установить Соты»</span></p><p><strong>iPhone / iPad</strong><span>Поделиться → «На экран Домой»</span></p><p className="field-help">Для ответов моделей нужен интернет. Данные кабинета не сохраняются в офлайн-кеше.</p></div>}</div>
          : !data ? <p className="panel-intro">Дождитесь загрузки кабинета.</p>
            : route.panel === 'connect' ? <ConnectPanel model={model} data={data} tab={prefs.tab} onTabChange={(tab) => updatePrefs({ tab })} onChat={showChat} onNewKey={() => openPanel('new-key')} auto={prefs.auto} onAutoChange={(auto) => updatePrefs({ auto })} secret={transientSecret} secretKeyId={transientKeyId} notify={notify} onChange={refresh} />
              : route.panel === 'new-key' && management ? <KeyPanel models={data.models} initialAuto={prefs.auto} onChange={refresh} onClose={closePanel} onConnect={(secret, created) => { setTransientSecret(secret); setTransientKeyId(created?.id); updatePrefs({ tab: 'api' }); openPanel('connect'); }} notify={notify} />
                : route.panel === 'key' && key && management ? <KeyPanel key={key.id} currentKey={key} models={data.models} onChange={refresh} onClose={closePanel} onConnect={(secret, created) => { setTransientSecret(secret); setTransientKeyId(created?.id); updatePrefs({ tab: 'api' }); openPanel('connect'); }} notify={notify} />
                  : route.panel === 'topup' ? <TopupPanel data={data} model={model} onChange={refresh} notify={notify} onClose={closePanel} />
                    : route.panel === 'models' ? <ModelDetails models={data.models} selectedId={model?.id || ''} onSelect={(id) => { updatePrefs({ model: id }); notify('Модель выбрана'); }} />
                      : route.panel === 'usage' ? <UsageDetails usage={data.usage} days={prefs.days} /> : <p className="panel-intro">Этот ключ недоступен в текущем кабинете.</p>}
    </Dialog>}
    <div className="toast-region" role="status" aria-live="polite">{toast && <div className="toast"><Check size={18} /><span>{toast}</span></div>}</div>
  </>;
}
