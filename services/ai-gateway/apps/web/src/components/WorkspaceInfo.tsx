import { Check, ChevronDown, ChevronRight, ShieldCheck, Smartphone } from 'lucide-react';
import { useEffect, useState } from 'react';
import { mutation } from '../api';
import { rubles } from '../format';
import type { Dashboard } from '../types';
import { InlineError, Spinner } from './Shared';

export default function WorkspaceInfo({ data, installed, onInstall, onModels, onChange, notify }: { data: Dashboard | null; installed: boolean; onInstall: () => void; onModels: () => void; onChange: () => void; notify: (message: string) => void }) {
  const [routingMode, setRoutingMode] = useState(data?.settings.routingMode || 'fallback');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => { if (data?.settings.routingMode) setRoutingMode(data.settings.routingMode); }, [data?.settings.routingMode]);
  async function saveRouting(mode: string) {
    if (busy || mode === routingMode) return;
    setBusy(true); setError(null);
    try { await mutation('/api/settings', 'PATCH', { routingMode: mode }); setRoutingMode(mode); onChange(); notify('Настройка ответов сохранена'); }
    catch (problem) { setError((problem as Error).message); }
    finally { setBusy(false); }
  }
  return <div className="workspace-info" aria-busy={busy}><div className="workspace-emblem"><ShieldCheck size={38} strokeWidth={1.4} /></div><h3>Временный кабинет</h3><p className="panel-intro">В этом браузере. Без регистрации.</p><div className="workspace-facts"><div><Check size={19} /><span>Свои ключи, расход и настройки</span></div><div><Check size={19} /><span>Данные отделены от других посетителей</span></div></div><p className="subtle-note text-only">Не очищайте данные сайта: это ваш доступ к кабинету. Привязка к устройству появится позже.</p>{data?.wallet && <dl className="details-grid"><div><dt>Доступно</dt><dd>{rubles(data.wallet.availableMicrorubles)} ₽</dd></div><div><dt>В обработке</dt><dd>{rubles(data.wallet.reservedMicrorubles)} ₽</dd></div></dl>}<button className="secondary-button full-width" onClick={onInstall} disabled={installed}><Smartphone size={21} />{installed ? 'Приложение установлено' : 'Установить приложение'}</button>
    {data && <button className="workspace-catalog" onClick={onModels}>Модели и цены<ChevronRight size={18} /></button>}
    {data?.capabilities.managementEnabled && <details className="workspace-advanced"><summary>Настройки ответов<ChevronDown size={17} /></summary><div className="routing-options" role="radiogroup" aria-label="Способ получения ответа"><button role="radio" type="button" aria-checked={routingMode === 'fallback'} className={routingMode === 'fallback' ? 'selected' : ''} onClick={() => saveRouting('fallback')} disabled={busy}><strong>Резерв</strong><span>По очереди при сбое</span></button><button role="radio" type="button" aria-checked={routingMode === 'race'} className={routingMode === 'race' ? 'selected' : ''} onClick={() => saveRouting('race')} disabled={busy}><strong>Параллельно</strong><span>Первый готовый ответ</span></button></div><p className="field-help">Параллельный режим может запустить два запроса у поставщиков. В вашем расходе учитывается один полученный ответ. Другая модель — только при разрешённом «Авто».</p>{busy && <Spinner label="Сохраняем" />}<InlineError>{error}</InlineError></details>}
  </div>;
}
