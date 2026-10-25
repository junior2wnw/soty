import { ArrowRight, Check, ChevronDown, KeyRound, Pause, Play, RefreshCw, ShieldCheck, Trash2 } from 'lucide-react';
import { useState } from 'react';
import { mutation } from '../api';
import { dateLabel, inputToMicro, keyStatus, rubles, rublesInput, shortModel } from '../format';
import type { ApiKey, KeyLimits, Model } from '../types';
import { CopyField, InlineError, Spinner, Toggle } from './Shared';

interface Props {
  currentKey?: ApiKey;
  models: Model[];
  onChange: () => void;
  onClose: () => void;
  onConnect: (secret: string, key?: ApiKey) => void;
  notify: (message: string) => void;
  initialAuto?: boolean;
}

function expiryInput(value?: string | null): string {
  if (!value) return '';
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? '' : new Date(date.valueOf() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
}

export default function KeyPanel({ currentKey, models, onChange, onClose, onConnect, notify, initialAuto = false }: Props) {
  const [name, setName] = useState(currentKey?.name || 'Мой агент');
  const [daily, setDaily] = useState(rublesInput(currentKey?.limits.dailyMicrorubles));
  const [monthly, setMonthly] = useState(rublesInput(currentKey?.limits.monthlyMicrorubles));
  const [total, setTotal] = useState(rublesInput(currentKey?.limits.totalMicrorubles));
  const [rpm, setRpm] = useState(currentKey?.limits.rpm?.toString() ?? '');
  const [tpm, setTpm] = useState(currentKey?.limits.tpm?.toString() ?? '');
  const [concurrency, setConcurrency] = useState(currentKey?.limits.concurrency?.toString() ?? '');
  const [allowedModels, setAllowedModels] = useState<string[]>(currentKey?.allowedModels || []);
  const [allModels, setAllModels] = useState(currentKey?.allowedModels == null);
  const [autoFallback, setAutoFallback] = useState(currentKey?.autoFallback ?? initialAuto);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [createdSecret, setCreatedSecret] = useState<string | null>(null);
  const [createdKey, setCreatedKey] = useState<ApiKey | null>(null);
  const [revokeConfirm, setRevokeConfirm] = useState(false);
  const [advanced, setAdvanced] = useState(false);
  const [expiresAt, setExpiresAt] = useState(expiryInput(currentKey?.expiresAt));
  const [rotateConfirm, setRotateConfirm] = useState(false);
  const [rotated, setRotated] = useState(false);
  const revoked = currentKey?.status === 'revoked';
  const pending = currentKey?.status === 'pending_activation';
  const status = keyStatus(currentKey?.status || 'active');

  function numeric(value: string): number | null {
    if (!value.trim()) return null;
    if (!/^\d+$/.test(value) || !Number.isSafeInteger(Number(value))) throw new Error('Лимиты запросов должны быть целыми неотрицательными числами.');
    return Number(value);
  }

  async function save(event: React.FormEvent) {
    event.preventDefault();
    if (busy) return;
    setError(null);
    let limits: KeyLimits;
    try { limits = { dailyMicrorubles: inputToMicro(daily), monthlyMicrorubles: inputToMicro(monthly), totalMicrorubles: inputToMicro(total), rpm: numeric(rpm), tpm: numeric(tpm), concurrency: numeric(concurrency) }; }
    catch (problem) { setError((problem as Error).message); return; }
    if (!name.trim()) { setError('Назовите ключ, чтобы отличать его от других.'); return; }
    if (!allModels && allowedModels.length === 0) { setError('Выберите хотя бы одну модель.'); return; }
    setBusy(true);
    try {
      const body = { name: name.trim(), limits, allowedModels: allModels ? null : allowedModels, autoFallback, ...(pending ? {} : { expiresAt: expiresAt ? new Date(expiresAt).toISOString() : null }) };
      if (currentKey) {
        await mutation(`/api/keys/${encodeURIComponent(currentKey.id)}`, 'PATCH', body);
        onChange(); notify('Настройки ключа сохранены'); onClose();
      } else {
        const result = await mutation<{ key: ApiKey; secret: string }>('/api/keys', 'POST', body, crypto.randomUUID());
        setCreatedKey(result.key); setCreatedSecret(result.secret); onChange();
      }
    } catch (problem) { setError((problem as Error).message); onChange(); }
    finally { setBusy(false); }
  }

  async function rotate() {
    if (!currentKey || busy || currentKey.status !== 'active') return;
    setBusy(true); setError(null);
    try {
      const result = await mutation<{ key: ApiKey; secret: string; overlapHours: number }>(`/api/keys/${encodeURIComponent(currentKey.id)}/rotate`, 'POST', {}, crypto.randomUUID());
      setCreatedKey(result.key); setCreatedSecret(result.secret); setRotated(true); onChange();
    } catch (problem) { setError((problem as Error).message); onChange(); }
    finally { setBusy(false); }
  }

  async function changeStatus(status: 'active' | 'paused' | 'revoked') {
    if (!currentKey || busy) return;
    setBusy(true); setError(null);
    try {
      if (status === 'revoked') await mutation(`/api/keys/${encodeURIComponent(currentKey.id)}`, 'DELETE');
      else await mutation(`/api/keys/${encodeURIComponent(currentKey.id)}`, 'PATCH', { status });
      onChange(); notify(status === 'revoked' ? 'Ключ отозван. Статистика сохранена.' : status === 'paused' ? 'Ключ на паузе' : 'Ключ снова активен'); onClose();
    } catch (problem) { setError((problem as Error).message); }
    finally { setBusy(false); }
  }

  if (createdSecret) return <div className="key-created"><div className="success-orbit"><ShieldCheck size={46} strokeWidth={1.4} /></div><h3>{rotated ? 'Новый секрет готов' : 'Ключ готов'}</h3><p className="panel-intro">{createdKey?.name || name}</p><CopyField label="Ваш API-ключ" value={createdSecret} secret /><div className="subtle-note"><KeyRound size={20} /><span>Скопируйте сейчас. После закрытия мы больше не покажем этот ключ.{rotated ? ' Старый секрет действует ещё 24 часа. Статистика и лимиты сохранены.' : ''}</span></div><button className="primary-button full-width" onClick={() => onConnect(createdSecret, createdKey || undefined)}>Настроить подключение<ArrowRight size={21} /></button><button className="text-button centered" onClick={onClose}>Готово</button></div>;

  return <form onSubmit={save} className="key-form" aria-busy={busy}>
    {currentKey && <div className="key-summary"><div className="key-summary-icon"><KeyRound size={28} strokeWidth={1.5} /></div><div><span className={`status-pill ${status.tone}`}>{status.label}</span><p>{currentKey.prefix}</p></div><strong>{rubles(currentKey.spentMicrorubles)} ₽</strong></div>}
    <label className="form-field"><span>Название</span><input data-autofocus value={name} onChange={(event) => setName(event.target.value)} placeholder="Например, OpenCode" maxLength={80} disabled={revoked || busy} required /></label>
    <fieldset disabled={revoked || busy} className="budget-fields"><legend>Бюджет</legend><div className="budget-inputs"><label className="form-field"><span>В день</span><div className="unit-input"><input value={daily} inputMode="decimal" onChange={(event) => setDaily(event.target.value)} placeholder="Без лимита" aria-label="Бюджет в день, рубли" /><span>₽</span></div></label><label className="form-field"><span>В месяц</span><div className="unit-input"><input value={monthly} inputMode="decimal" onChange={(event) => setMonthly(event.target.value)} placeholder="Без лимита" aria-label="Бюджет в месяц, рубли" /><span>₽</span></div></label></div><p className="field-help">Пусто — без дополнительного лимита. Ноль — запрет расходов.</p></fieldset>
    <fieldset className="model-permissions" disabled={revoked || busy}><legend>Доступ к моделям</legend><label className="checkbox-row"><input type="checkbox" checked={allModels} onChange={(event) => { setAllModels(event.target.checked); if (!event.target.checked && allowedModels.length === 0) setAllowedModels(models.slice(0, 1).map((model) => model.id)); }} /><span>Все текущие и новые модели</span></label>{!allModels && <div className="allowed-models">{models.map((model) => <label className="checkbox-row" key={model.id}><input type="checkbox" checked={allowedModels.includes(model.id)} onChange={(event) => setAllowedModels((list) => event.target.checked ? [...list, model.id] : list.filter((id) => id !== model.id))} /><span>{shortModel(model)}</span></label>)}</div>}</fieldset>
    <Toggle checked={autoFallback} onChange={setAutoFallback} label="Авто" description="Разрешить другую модель при сбое" disabled={revoked || busy} />
    <button type="button" className="advanced-toggle" onClick={() => setAdvanced((value) => !value)} aria-expanded={advanced}>Дополнительно<ChevronDown size={18} className={advanced ? 'rotate' : ''} /></button>
    {advanced && <fieldset disabled={revoked || busy} className="advanced-fields"><label className="form-field"><span>За всё время, ₽</span><input value={total} inputMode="decimal" onChange={(event) => setTotal(event.target.value)} placeholder="Без лимита" /></label><div className="budget-inputs"><label className="form-field"><span>Запросов / мин</span><input value={rpm} inputMode="numeric" onChange={(event) => setRpm(event.target.value)} placeholder="Без лимита" /></label><label className="form-field"><span>Токенов / мин</span><input value={tpm} inputMode="numeric" onChange={(event) => setTpm(event.target.value)} placeholder="Без лимита" /></label></div><label className="form-field"><span>Одновременно</span><input value={concurrency} inputMode="numeric" onChange={(event) => setConcurrency(event.target.value)} placeholder="Без лимита" /></label><label className="form-field"><span>Действует до · ваше время</span><input type="datetime-local" value={expiresAt} onChange={(event) => setExpiresAt(event.target.value)} disabled={pending} /><small className="field-help">Пусто — без срока действия.</small></label>{currentKey && <dl className="key-dates"><div><dt>Создан</dt><dd>{dateLabel(currentKey.createdAt, true)}</dd></div><div><dt>Последний запрос</dt><dd>{dateLabel(currentKey.lastUsedAt, true)}</dd></div></dl>}{currentKey?.status === 'active' && <button type="button" className="secondary-button full-width" onClick={() => setRotateConfirm((value) => !value)}><RefreshCw size={18} />Обновить секрет ключа</button>}</fieldset>}
    {rotateConfirm && <div className="subtle-note rotation-confirm"><strong>Обновить секрет «{currentKey?.name}»?</strong><p>Старый будет работать ещё 24 часа. Замените его в приложениях. Статистика и лимиты останутся.</p><button type="button" className="secondary-button full-width" onClick={rotate} disabled={busy}>Получить новый секрет</button></div>}
    <InlineError>{error}</InlineError>
    {!revoked && <button className="primary-button full-width" disabled={busy} type="submit">{busy ? <Spinner label="Сохраняем" /> : currentKey ? <><Check size={20} />Сохранить</> : <><KeyRound size={21} />Создать ключ</>}</button>}
    {currentKey && !revoked && <div className="key-actions"><button type="button" className="secondary-button" disabled={busy || (currentKey.status !== 'active' && currentKey.status !== 'paused')} onClick={() => changeStatus(currentKey.status === 'paused' ? 'active' : 'paused')}>{currentKey.status === 'paused' ? <Play size={19} /> : <Pause size={19} />}{currentKey.status === 'paused' ? 'Возобновить' : 'Пауза'}</button><button type="button" className="secondary-button danger-text" disabled={busy} onClick={() => setRevokeConfirm((value) => !value)}><Trash2 size={18} />Отозвать</button></div>}
    {revokeConfirm && <div className="revoke-confirm" role="alert"><strong>Отозвать «{currentKey?.name}»?</strong><p>Подключённые к этому ключу приложения перестанут получать ответы. История расходов останется.</p><button type="button" className="danger-button" disabled={busy} onClick={() => changeStatus('revoked')}>Да, отозвать ключ</button><button type="button" className="text-button" onClick={() => setRevokeConfirm(false)}>Отмена</button></div>}
  </form>;
}
