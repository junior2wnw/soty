import { Check, ChevronRight, Search } from 'lucide-react';
import { useState } from 'react';
import { dateLabel, health, priceLabel, rubles, shortModel } from '../format';
import type { Model } from '../types';
import { ModelAvatar } from './Shared';

export function Models({ models, selectedId, onSelect, onDetails }: { models: Model[]; selectedId: string; onSelect: (id: string) => void; onDetails: () => void }) {
  const [search, setSearch] = useState('');
  const visible = models.filter((model) => `${model.name} ${model.version} ${model.id}`.toLowerCase().includes(search.toLowerCase()));
  return <section className="models-section" aria-labelledby="models-heading">
    <div className="section-heading models-heading"><h1 id="models-heading">Модели</h1><button className="text-button" onClick={onDetails}>Все цены <ChevronRight size={17} /></button>{models.length > 6 && <label className="model-search"><Search size={18} /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Найти модель" aria-label="Найти модель" /></label>}</div>
    {models.length === 0 ? <div className="model-empty card"><span className="empty-mark">◇</span><strong>Каталог обновляется</strong><span>Модели появятся после проверки соединения.</span></div> : <div className="model-grid">
      {visible.map((model) => {
        const state = health(model.status);
        const selected = selectedId === model.id;
        return <button key={model.id} className={`model-card ${selected ? 'selected' : ''}`} onClick={() => onSelect(model.id)} aria-pressed={selected} aria-label={`${shortModel(model)}, ${priceLabel(model)}, ${state.label}`}>
          <ModelAvatar model={model} />
          <span className="model-description"><strong title={model.name}>{model.name}</strong><span className="model-version">{model.version || model.id.split('/').pop()}</span><span className="model-price">{priceLabel(model)}</span></span>
          <span className={`selection-dot ${selected ? 'checked' : ''}`} aria-hidden="true">{selected && <Check size={18} strokeWidth={2.5} />}</span>
          <span className={`status-pill ${state.tone}`}>{state.label}</span>
        </button>;
      })}
      {visible.length === 0 && <div className="model-empty card"><strong>Не нашли модель</strong><span>Попробуйте другое название.</span></div>}
    </div>}
  </section>;
}

export function ModelDetails({ models, selectedId, onSelect }: { models: Model[]; selectedId: string; onSelect: (id: string) => void }) {
  return <div className="model-details-list"><p className="panel-intro">За 1 млн токенов. Стоимость зависит от входа и ответа.</p>{models.map((model) => {
    const state = health(model.status);
    return <div className={`model-details-card ${model.id === selectedId ? 'selected' : ''}`} key={model.id}>
      <button className="model-details-top" onClick={() => onSelect(model.id)} aria-pressed={model.id === selectedId}><ModelAvatar model={model} small /><span><strong>{shortModel(model)}</strong><span className={`status-text ${state.tone}`}>{state.label}</span></span>{selectedId === model.id && <span className="selection-dot checked"><Check size={16} /></span>}</button>
      <dl className="details-grid"><div><dt>Вход</dt><dd>{model.inputPriceMicrorublesPerMillion != null ? `${rubles(model.inputPriceMicrorublesPerMillion)} ₽` : 'Уточняется'}</dd></div><div><dt>Ответ</dt><dd>{model.outputPriceMicrorublesPerMillion != null ? `${rubles(model.outputPriceMicrorublesPerMillion)} ₽` : 'Уточняется'}</dd></div><div><dt>Контекст</dt><dd>{model.contextWindow ? new Intl.NumberFormat('ru-RU').format(model.contextWindow) : '—'}</dd></div><div><dt>Проверка</dt><dd>{model.checkedAt ? dateLabel(model.checkedAt, true) : 'Нет свежих данных'}</dd></div></dl>
      <p className="model-id">{model.id}</p>
      {model.capabilities && model.capabilities.length > 0 && <div className="capability-chips">{model.capabilities.map((capability) => <span key={capability}>{capability}</span>)}</div>}
    </div>;
  })}{models.length === 0 && <p className="muted">Каталог пока пуст. Проверка соединения выполняется на сервере.</p>}</div>;
}
