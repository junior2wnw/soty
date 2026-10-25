import { ArrowLeft, ArrowUp, Check, Copy, MessageSquare, RefreshCw, Square, Trash2 } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { request } from '../api';
import { copyText, shortModel } from '../format';
import type { ChatMessage, Model } from '../types';
import { InlineError, ModelAvatar, Spinner, Toggle } from './Shared';

interface ChatResponse {
  model?: string;
  choices?: { message?: { content?: string | { type: string; text?: string }[] }; finish_reason?: string }[];
}

function AssistantMessage({ content }: { content: string }) {
  return <ReactMarkdown remarkPlugins={[remarkGfm]} components={{ a: ({ children, href }) => <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>, pre: ({ children }) => <pre tabIndex={0}>{children}</pre> }}>{content}</ReactMarkdown>;
}

export default function Chat({ model, messages, setMessages, auto, setAuto, onClose, onChange, onTopup, balanceEmpty }: { model?: Model; messages: ChatMessage[]; setMessages: (messages: ChatMessage[]) => void; auto: boolean; setAuto: (auto: boolean) => void; onClose: () => void; onChange: () => void; onTopup: () => void; balanceEmpty: boolean }) {
  const [draft, setDraft] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [truncated, setTruncated] = useState(false);
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  const mountedRef = useRef(true);

  useEffect(() => { mountedRef.current = true; return () => { mountedRef.current = false; abortRef.current?.abort(); }; }, []);
  useEffect(() => { const node = scrollRef.current; if (node) node.scrollTop = node.scrollHeight; }, [messages.length, busy]);

  async function send(retry = false) {
    if (!model || busy || balanceEmpty || (!draft.trim() && !retry)) return;
    const next: ChatMessage[] = retry ? messages : [...messages, { role: 'user', content: draft.trim() }];
    if (next.length === 0 || next[next.length - 1].role !== 'user') return;
    setMessages(next); setDraft(''); setBusy(true); setError(null); setTruncated(false);
    if (textareaRef.current) textareaRef.current.style.height = '';
    const controller = new AbortController(); abortRef.current = controller;
    try {
      const response = await request<ChatResponse>('/api/chat', { method: 'POST', signal: controller.signal, body: JSON.stringify({ model: model.id, messages: next.map(({ role, content }) => ({ role, content })), autoFallback: auto, max_tokens: 2048, stream: false }) }, 75_000);
      if (!mountedRef.current) return;
      const choice = response.choices?.[0];
      const raw = choice?.message?.content;
      const content = Array.isArray(raw) ? raw.filter((part) => part.type === 'text').map((part) => part.text || '').join('\n') : raw;
      if (!content?.trim()) throw new Error('Модель вернула пустой ответ. Повторите запрос или выберите другую модель.');
      setMessages([...next, { role: 'assistant', content, actualModel: response.model }]); setTruncated(choice?.finish_reason === 'length');
    } catch (problem) {
      if (mountedRef.current) setError(controller.signal.aborted ? 'Запрос остановлен.' : (problem as Error).message);
    } finally { if (mountedRef.current) { setBusy(false); onChange(); } abortRef.current = null; }
  }

  const copy = async (content: string, index: number) => { if (await copyText(content)) { setCopiedIndex(index); window.setTimeout(() => setCopiedIndex(null), 1800); } };
  return <section className="card chat-workspace"><div className="chat-heading"><button className="icon-button" onClick={onClose} aria-label="Вернуться в кабинет"><ArrowLeft size={22} /></button>{model && <><ModelAvatar model={model} small /><div><h2>{shortModel(model)}</h2><span>Чат</span></div></>}<button className="icon-button clear-chat" onClick={() => { setMessages([]); setError(null); setTruncated(false); }} disabled={busy || messages.length === 0} aria-label="Очистить диалог"><Trash2 size={20} /></button></div>
    <div className="chat-messages" ref={scrollRef} aria-live="polite" aria-relevant="additions">
      {messages.length === 0 && <div className="chat-empty"><div className="chat-welcome-icon"><MessageSquare size={43} strokeWidth={1.25} /></div><h3>С чего начнём?</h3><p>Выберите идею или напишите свой вопрос.</p><div className="chat-suggestions">{['Объясни простыми словами', 'Помоги с кодом', 'Продумай идею'].map((suggestion) => <button key={suggestion} onClick={() => { setDraft(suggestion + ': '); textareaRef.current?.focus(); }}>{suggestion}</button>)}</div></div>}
      {messages.map((message, index) => <div key={index} className={`chat-message ${message.role}`}><div className="message-content">{message.role === 'assistant' ? <AssistantMessage content={message.content} /> : message.content}</div>{message.role === 'assistant' && <div className="message-tools"><span>{message.actualModel?.split('/').pop() || (model ? shortModel(model) : '')}</span><button className="icon-button" onClick={() => copy(message.content, index)} aria-label="Скопировать ответ">{copiedIndex === index ? <Check size={16} /> : <Copy size={16} />}</button></div>}</div>)}
      {busy && <div className="chat-thinking"><Spinner label="Модель думает" /><span>Ответ может занять до минуты</span></div>}{truncated && <p className="chat-limit-note">Ответ достиг лимита длины. Попросите продолжить.</p>}
    </div>
    <div className="chat-composer"><InlineError>{error}</InlineError>{error && messages[messages.length - 1]?.role === 'user' && <button className="text-button retry-chat" onClick={() => send(true)} disabled={busy}><RefreshCw size={16} />Повторить запрос</button>}{balanceEmpty && <div className="chat-balance-note"><span>Для первого ответа нужен демо-баланс.</span><button className="text-button" onClick={onTopup}>Пополнить</button></div>}<form onSubmit={(event) => { event.preventDefault(); void send(); }} className="chat-input-row"><textarea ref={textareaRef} rows={1} value={draft} onChange={(event) => { setDraft(event.target.value); event.target.style.height = 'auto'; event.target.style.height = `${Math.min(event.target.scrollHeight, 150)}px`; }} onKeyDown={(event) => { if (event.key === 'Enter' && !event.shiftKey && !event.nativeEvent.isComposing) { event.preventDefault(); void send(); } }} placeholder="Ваш вопрос…" aria-label="Ваше сообщение" disabled={busy || !model} maxLength={32000} />{busy ? <button className="send-button stop" type="button" onClick={() => abortRef.current?.abort()} aria-label="Остановить запрос"><Square size={16} fill="currentColor" /></button> : <button className="send-button" type="submit" disabled={!draft.trim() || !model || balanceEmpty} aria-label="Отправить сообщение"><ArrowUp size={23} /></button>}</form><div className="chat-bottom"><Toggle checked={auto} onChange={setAuto} label="Авто" disabled={busy} /><span>Диалог исчезнет при закрытии вкладки</span></div></div>
  </section>;
}
