import type { Model } from './types';

const integer = new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 0 });

export function micro(value?: string | number | null): bigint {
  try { return BigInt(value ?? '0'); } catch { return 0n; }
}

export function rubles(value?: string | null, decimals: 'auto' | 'always' = 'auto'): string {
  const amount = micro(value);
  const negative = amount < 0n;
  const kopecks = ((negative ? -amount : amount) + 5_000n) / 10_000n;
  const whole = kopecks / 100n;
  const fraction = kopecks % 100n;
  return `${negative ? '−' : ''}${integer.format(whole)}${fraction || decimals === 'always' ? `,${fraction.toString().padStart(2, '0')}` : ''}`;
}

export function rublesInput(value?: string | null): string {
  if (value === null || value === undefined) return '';
  const amount = micro(value);
  return `${amount / 1_000_000n}${amount % 1_000_000n ? `.${(amount % 1_000_000n).toString().padStart(6, '0').replace(/0+$/, '')}` : ''}`;
}

export function inputToMicro(value: string): string | null {
  const normalized = value.replace(',', '.').replace(/\s/g, '');
  if (!normalized) return null;
  if (!/^\d{1,12}(\.\d{0,2})?$/.test(normalized)) throw new Error('Введите сумму в рублях: не более двух знаков после запятой.');
  const [whole, fraction = ''] = normalized.split('.');
  return (BigInt(whole) * 1_000_000n + BigInt(fraction.padEnd(6, '0'))).toString();
}

export function compactTokens(value: string | number): string {
  const count = Number(value);
  if (!Number.isFinite(count)) return '—';
  if (count >= 1_000_000) return `${new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 2 }).format(count / 1_000_000)} млн`;
  if (count >= 10_000) return `${new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 1 }).format(count / 1_000)} тыс.`;
  return integer.format(count);
}

export function dateLabel(value?: string | null, withTime = false): string {
  if (!value) return 'Ещё не использовался';
  const date = new Date(/^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T12:00:00` : value);
  if (Number.isNaN(date.valueOf())) return '—';
  return new Intl.DateTimeFormat('ru-RU', withTime ? { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' } : { day: 'numeric', month: 'short' }).format(date);
}

export function keyStatus(status: string): { label: string; tone: string } {
  if (status === 'active') return { label: 'Активен', tone: 'good' };
  if (status === 'paused') return { label: 'Пауза', tone: 'warn' };
  if (status === 'pending_activation') return { label: 'Ожидает установки', tone: 'muted' };
  if (status === 'expired') return { label: 'Срок истёк', tone: 'warn' };
  return { label: 'Отозван', tone: 'muted' };
}

export function dayLabel(value: string): string {
  const date = new Date(`${value.slice(0, 10)}T12:00:00`);
  return Number.isNaN(date.valueOf()) ? '' : String(date.getDate());
}

export function shortModel(model: Model): string {
  return [model.name, model.version].filter(Boolean).join(' ');
}

export function modelInitial(model: Model): string {
  return (model.name.replace(/[^\p{L}\p{N}]/gu, '')[0] || '?').toUpperCase();
}

export function priceLabel(model: Model): string {
  const input = model.inputPriceMicrorublesPerMillion;
  const output = model.outputPriceMicrorublesPerMillion;
  if (input == null || output == null) return 'Тариф уточняется';
  if (micro(input) === micro(output)) return `${rubles(input)} ₽ / 1 млн`;
  return `${rubles(input)} / ${rubles(output)} ₽`;
}

export function health(status: string): { label: string; tone: string } {
  switch (status?.toLowerCase()) {
    case 'healthy': case 'available': case 'online': case 'ready': case 'operational': return { label: 'Работает', tone: 'good' };
    case 'degraded': case 'slow': return { label: 'Медленно', tone: 'warn' };
    case 'unhealthy': case 'error': case 'down': case 'outage': case 'failing': return { label: 'Сбои', tone: 'warn' };
    case 'removed': case 'retired': return { label: 'Снята', tone: 'muted' };
    case 'unavailable': return { label: 'Недоступна', tone: 'warn' };
    case 'recovering': return { label: 'Проверяем', tone: 'warn' };
    case 'credential_error': return { label: 'Настройка', tone: 'warn' };
    case 'funding_required': return { label: 'Нет средств', tone: 'warn' };
    case 'rate_limited': return { label: 'Загружена', tone: 'warn' };
    case 'qualifying': case 'testing': case 'new': return { label: 'Проверяем', tone: 'muted' };
    case 'pilot': return { label: 'Пилот', tone: 'muted' };
    default: return { label: 'Нет данных', tone: 'muted' };
  }
}

export function downloadFile(content: BlobPart, filename: string, type = 'text/plain;charset=utf-8'): void {
  const objectUrl = URL.createObjectURL(new Blob([content], { type }));
  const link = document.createElement('a');
  link.href = objectUrl;
  link.download = filename;
  link.click();
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 30_000);
}

export async function copyText(value: string): Promise<boolean> {
  try { await navigator.clipboard.writeText(value); return true; } catch { return false; }
}
