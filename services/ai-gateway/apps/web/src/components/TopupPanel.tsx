import { Check, CreditCard, FlaskConical } from 'lucide-react';
import { useState } from 'react';
import { mutation } from '../api';
import { inputToMicro, micro, rubles } from '../format';
import type { Dashboard, Model } from '../types';
import { InlineError, Spinner } from './Shared';

export default function TopupPanel({ data, model, onChange, notify, onClose }: { data: Dashboard; model?: Model; onChange: () => void; notify: (message: string) => void; onClose: () => void }) {
  const [amount, setAmount] = useState('500');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [complete, setComplete] = useState(false);
  let amountMicro: string | null = null;
  try { amountMicro = inputToMicro(amount); } catch { /* Invalid input stays editable until submit. */ }
  const price = model && model.inputPriceMicrorublesPerMillion === model.outputPriceMicrorublesPerMillion ? micro(model.inputPriceMicrorublesPerMillion) : 0n;
  const tokensMillions = price > 0n && amountMicro ? Number(micro(amountMicro) * 100n / price) / 100 : null;
  async function topup() {
    if (busy) return;
    setError(null);
    if (!amountMicro || micro(amountMicro) <= 0n) { setError('Введите положительную сумму, не более двух знаков после запятой.'); return; }
    setBusy(true);
    try { await mutation('/api/topups/demo', 'POST', { amountMicrorubles: amountMicro }, crypto.randomUUID()); setComplete(true); onChange(); notify('Демо-баланс пополнен'); }
    catch (problem) { setError((problem as Error).message); onChange(); }
    finally { setBusy(false); }
  }
  if (complete) return <div className="topup-complete"><div className="success-orbit"><Check size={45} strokeWidth={1.5} /></div><h3>Готово</h3><strong>+{rubles(amountMicro)} ₽</strong><p className="panel-intro">Демо-баланс. Деньги с карты не списывались.</p><button className="primary-button full-width" onClick={onClose}>Продолжить</button></div>;
  return <div className="topup-panel" aria-busy={busy}><div className="topup-amount"><label><span className="sr-only">Сумма пополнения, рубли</span><input value={amount} onChange={(event) => setAmount(event.target.value)} inputMode="decimal" maxLength={10} aria-label="Сумма пополнения, рубли" /><span>₽</span></label></div><div className="topup-presets">{['100', '500', '2000'].map((preset) => <button key={preset} className={amount === preset ? 'selected' : ''} onClick={() => setAmount(preset)}>{Number(preset).toLocaleString('ru-RU')} ₽</button>)}</div><p className="token-estimate">{tokensMillions !== null ? `≈ ${tokensMillions.toLocaleString('ru-RU', { maximumFractionDigits: 2 })} млн токенов` : 'Баланс для знакомства с сервисом'}</p><div className="payment-methods"><div className="payment-method selected"><span className="radio-mark" /><FlaskConical size={25} strokeWidth={1.5} /><strong>Демо</strong></div><div className="payment-method unavailable"><CreditCard size={27} strokeWidth={1.5} /><span>Карта / СБП<small>Скоро</small></span></div></div><InlineError>{error}</InlineError><button className="primary-button full-width" onClick={topup} disabled={busy || !data.capabilities.managementEnabled || !data.capabilities.demoTopupEnabled}>{busy ? <Spinner label="Пополняем" /> : `Пополнить на ${amountMicro ? rubles(amountMicro) : '…'} ₽`}</button><p className="demo-payment-note">Демо · без оплаты</p></div>;
}
