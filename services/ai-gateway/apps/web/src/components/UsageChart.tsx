import { ArrowDownToLine, ChartNoAxesCombined, ChevronDown, ChevronRight } from 'lucide-react';
import { useState } from 'react';
import { compactTokens, dateLabel, dayLabel, downloadFile, micro, rubles } from '../format';
import type { Usage } from '../types';

export function UsageCard({ usage, days, onDaysChange, onDetails }: { usage: Usage; days: number; onDaysChange: (days: number) => void; onDetails: () => void }) {
  const [selectedDay, setSelectedDay] = useState<string | null>(null);
  const rows = usage.daily || [];
  const max = rows.reduce((highest, row) => micro(row.spentMicrorubles) > highest ? micro(row.spentMicrorubles) : highest, 0n);
  const chosen = rows.find((row) => row.date === selectedDay);
  const responsePercent = usage.requests > 0 ? new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 1 }).format(usage.successfulRequests / usage.requests * 100) + '%' : '—';
  return <section className="card spend-card" aria-labelledby="spend-heading">
    <div className="spend-heading"><h2 id="spend-heading">Расход</h2><label className="period-select"><span className="sr-only">Период статистики</span><select value={days} onChange={(event) => { setSelectedDay(null); onDaysChange(Number(event.target.value)); }}><option value={7}>7 дней</option><option value={30}>30 дней</option><option value={90}>90 дней</option></select><ChevronDown size={16} aria-hidden="true" /></label><button className="icon-button usage-details-button" onClick={onDetails} aria-label="Подробная статистика"><ChevronRight size={20} /></button></div>
    <div className="spend-total" aria-label={`Расход ${rubles(chosen?.spentMicrorubles ?? usage.spentMicrorubles)} рублей`}>{rubles(chosen?.spentMicrorubles ?? usage.spentMicrorubles)} ₽</div>
    {chosen && <button className="selected-day" onClick={() => setSelectedDay(null)}>{dateLabel(chosen.date)} · Весь период</button>}
    <div className={`usage-chart ${max === 0n ? 'is-empty' : ''}`} role="group" aria-label="Расход по дням">
      {max === 0n && <div className="chart-empty"><ChartNoAxesCombined size={28} strokeWidth={1.35} /><span>Здесь появится расход</span></div>}
      <div className={`chart-bars ${rows.length > 14 ? 'many-bars' : ''}`}>
        {rows.map((row, index) => {
          const value = micro(row.spentMicrorubles);
          const percent = max > 0n ? Number(value * 10000n / max) / 100 : 0;
          const active = selectedDay ? row.date === selectedDay : index === rows.length - 1;
          const showLabel = rows.length <= 14 || index % 5 === 0 || index === rows.length - 1;
          return <button className={`bar-column ${active ? 'active' : ''}`} key={row.date} onClick={() => setSelectedDay(row.date === selectedDay ? null : row.date)} aria-pressed={selectedDay === row.date} aria-label={`${dateLabel(row.date)}: ${rubles(row.spentMicrorubles)} рублей`} title={`${dateLabel(row.date)} · ${rubles(row.spentMicrorubles)} ₽`}>
            <span className="bar-track"><span className="bar-stack" style={{ height: `${percent}%` }}>{max > 0n && rows.length <= 14 && <span className="bar-value">{rubles(row.spentMicrorubles, 'always')}</span>}<span className={`bar-fill ${value === 0n ? 'zero' : ''}`} /></span></span>
            <span className="bar-date" aria-hidden="true">{showLabel ? dayLabel(row.date) : ''}</span>
          </button>;
        })}
      </div>
    </div>
    <div className="usage-summary"><button onClick={onDetails}><strong>{compactTokens(usage.tokens || 0)}</strong><span>Токены</span></button><button onClick={onDetails}><strong>{responsePercent}</strong><span>Ответы</span></button></div>
  </section>;
}

export function UsageDetails({ usage, days }: { usage: Usage; days: number }) {
  const exportCsv = () => {
    const csv = ['Дата;Расход (₽);Токены;Запросы;Успешные ответы', ...usage.daily.map((row) => [row.date, rubles(row.spentMicrorubles, 'always').replace(/\s/g, ''), row.tokens ?? 0, row.requests ?? 0, row.successfulRequests ?? 0].join(';'))].join('\r\n');
    downloadFile('\uFEFF' + csv, `soty-usage-${days}-days.csv`, 'text/csv;charset=utf-8');
  };
  return <div><div className="details-total"><span>За {days} дней</span><strong>{rubles(usage.spentMicrorubles)} ₽</strong></div><dl className="details-grid statistics-grid"><div><dt>Токены</dt><dd>{compactTokens(usage.tokens || 0)}</dd></div><div><dt>Запросы</dt><dd>{new Intl.NumberFormat('ru-RU').format(usage.requests || 0)}</dd></div><div><dt>Успешные ответы</dt><dd>{new Intl.NumberFormat('ru-RU').format(usage.successfulRequests || 0)}</dd></div><div><dt>p95</dt><dd>{usage.p95Ms != null ? `${(usage.p95Ms / 1000).toLocaleString('ru-RU', { maximumFractionDigits: 1 })} с` : '—'}</dd></div></dl><p className="panel-intro">«Ответы» — доля успешно завершённых запросов. Проверки моделей не входят в ваш расход.</p>
      <div className="usage-table-wrap"><table className="usage-table"><thead><tr><th>День</th><th>Токены</th><th>Расход</th></tr></thead><tbody>{usage.daily.map((row) => <tr key={row.date}><td>{dateLabel(row.date)}</td><td>{compactTokens(row.tokens ?? 0)}</td><td>{rubles(row.spentMicrorubles)} ₽</td></tr>)}</tbody></table></div><button className="secondary-button full-width" onClick={exportCsv}><ArrowDownToLine size={20} />Скачать статистику</button></div>;
}
