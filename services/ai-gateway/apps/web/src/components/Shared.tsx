import { Check, CodeXml, Copy, Eye, EyeOff, LoaderCircle, MessageSquare, Terminal } from 'lucide-react';
import { useState } from 'react';
import { copyText, modelInitial } from '../format';
import type { ConnectionTab, Model } from '../types';

export function ModelAvatar({ model, small = false }: { model: Model; small?: boolean }) {
  return <span className={`model-avatar ${small ? 'small' : ''}`} aria-hidden="true">{modelInitial(model)}</span>;
}

export function Tabs({ selected, onChange }: { selected: ConnectionTab; onChange: (tab: ConnectionTab) => void }) {
  return <div className="connection-tabs" role="tablist" aria-label="Способ подключения">
    {([{ id: 'chat', text: 'Чат', Icon: MessageSquare }, { id: 'api', text: 'API', Icon: CodeXml }, { id: 'opencode', text: 'OpenCode', Icon: Terminal }] as const).map(({ id, text, Icon }) =>
      <button key={id} role="tab" aria-selected={selected === id} tabIndex={selected === id ? 0 : -1} className={selected === id ? 'active' : ''} onClick={() => onChange(id)} onKeyDown={(event) => {
        const order: ConnectionTab[] = ['chat', 'api', 'opencode'];
        const index = order.indexOf(id);
        const next = event.key === 'ArrowRight' ? (index + 1) % 3 : event.key === 'ArrowLeft' ? (index + 2) % 3 : event.key === 'Home' ? 0 : event.key === 'End' ? 2 : -1;
        if (next >= 0) { event.preventDefault(); onChange(order[next]); event.currentTarget.parentElement?.querySelectorAll<HTMLButtonElement>('[role="tab"]')[next]?.focus(); }
      }}><Icon size={25} strokeWidth={1.75} /><span>{text}</span></button>)}
  </div>;
}

export function Toggle({ checked, onChange, label, description, disabled = false }: { checked: boolean; onChange: (value: boolean) => void; label: string; description?: string; disabled?: boolean }) {
  return <label className={`toggle-row ${disabled ? 'disabled' : ''}`}>
    <input type="checkbox" role="switch" checked={checked} onChange={(event) => onChange(event.target.checked)} disabled={disabled} />
    <span className="toggle-track" aria-hidden="true"><span /></span>
    <span className="toggle-copy"><span>{label}</span>{description && <small>{description}</small>}</span>
  </label>;
}

export function CopyField({ label, value, secret = false, onChange, placeholder }: { label: string; value: string; secret?: boolean; onChange?: (value: string) => void; placeholder?: string }) {
  const [copied, setCopied] = useState(false);
  const [failed, setFailed] = useState(false);
  const [revealed, setRevealed] = useState(false);
  const copy = async () => {
    const success = await copyText(value);
    setCopied(success); setFailed(!success);
    window.setTimeout(() => setCopied(false), 2200);
  };
  return <div className="copy-field"><span className="field-label">{label}</span><div className="copy-value"><input type={secret && !revealed ? 'password' : 'text'} value={value} readOnly={!onChange} onChange={(event) => onChange?.(event.target.value)} placeholder={placeholder} aria-label={label} autoComplete="off" spellCheck={false} onFocus={(event) => event.currentTarget.select()} />{secret && <button className="icon-button" type="button" onClick={() => setRevealed((current) => !current)} aria-label={revealed ? 'Скрыть ключ' : 'Показать ключ'}>{revealed ? <EyeOff size={18} /> : <Eye size={18} />}</button>}<button className="icon-button" type="button" onClick={copy} aria-label={`Скопировать ${label}`} disabled={!value}><span className="sr-only">{copied ? 'Скопировано' : 'Скопировать'}</span>{copied ? <Check size={20} /> : <Copy size={20} />}</button></div>{failed && <small className="muted">Выделите значение и скопируйте вручную.</small>}</div>;
}

export function Spinner({ label = 'Загрузка' }: { label?: string }) { return <span className="inline-spinner"><LoaderCircle className="spin" size={19} /><span>{label}</span></span>; }

export function InlineError({ children }: { children?: string | null }) { return children ? <div role="alert" className="inline-error">{children}</div> : null; }
