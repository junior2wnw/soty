import type { ConnectClient } from '../../modules/connect/browser/index.mjs';
import type { WorldAssistantSelection, WorldProjectEditorPort } from '../world/types';
import { button, el } from '../world/dom';
import { createVoiceOrbClient, type VoiceOrbView } from './voice-orb-client.mjs';

export function mountVoiceOrbPanel(host: HTMLElement, options: {
  client: ConnectClient; accountId: string; isCurrent(): boolean;
  acquireCaptureLease(): (() => void) | null;
  readSelection(): WorldAssistantSelection | null;
  openProjectEditor?: WorldProjectEditorPort;
}) {
  const root = el('section', 'sw-voice-orb'); root.setAttribute('aria-label', 'Голосовой помощник');
  const title = el('h2', '', 'Поговорим?'), state = el('p', 'sw-muted'); state.setAttribute('role', 'status');
  const disclosure = el('p', 'sw-muted', 'Микрофон включается по нажатию. Голосовой разговор использует ваш голосовой лимит.');
  const audio = el('audio'); audio.autoplay = true; audio.controls = true; audio.setAttribute('aria-label', 'Голос помощника');
  const actions = el('div', 'sw-row sw-voice-orb-actions');
  const start = button('Говорить', 'sparkle', 'sw-button-primary', () => { void voice.start(); });
  const mute = button('Выключить микрофон', 'hidden', 'sw-button-quiet', () => voice.mute());
  const interrupt = button('Прервать', 'close', 'sw-button-quiet', () => { void voice.interrupt(); });
  const resume = button('Слушать ответ', 'sparkle', 'sw-button-quiet', () => { void voice.resumePlayback(); });
  const check = button('Проверить поручение', 'refresh', 'sw-button-quiet', () => { void voice.check(); });
  const end = button('Завершить', 'exit', 'sw-button-quiet', () => { void voice.end(); });
  const permission = el('label', 'sw-assistant-note-permission'), allowNote = el('input'); allowNote.type = 'checkbox';
  permission.append(allowNote, el('span', '', 'Разрешить записку в следующем голосовом поручении'));
  const captions = el('div', 'sw-voice-orb-captions'), input = el('p'), output = el('p'), result = el('p');
  input.setAttribute('aria-label', 'Ваша речь'); output.setAttribute('aria-label', 'Речь помощника'); result.setAttribute('aria-label', 'Результат поручения');
  const editorStatus = el('p', 'sw-muted'); editorStatus.setAttribute('role', 'status');
  captions.append(input, output, result, editorStatus); actions.append(start, mute, interrupt, resume, check, end);
  root.append(title, state, disclosure, actions, permission, audio, captions); host.append(root);
  const assign = (node: HTMLElement, text: string): void => { if (node.textContent !== text) node.textContent = text; };
  const changed = (value: VoiceOrbView): void => {
    const label = !value.configured ? 'Голос пока не подключён. Доступен текстовый помощник.'
      : value.code === 'voice_orb_busy' ? 'Сейчас микрофон занят другим разговором.'
      : value.code === 'voice_orb_accounting_pending' ? 'Разговор закрыт. Расход ещё уточняется.'
      : value.phase === 'unknown' ? 'Поручение или завершение не подтверждено. Автоматического повтора нет.'
      : ({ checking: 'Проверяем голосовой режим…', idle: 'Нажмите «Говорить».', connecting: 'Подключаем разговор…',
        listening: value.muted ? 'Микрофон выключен.' : 'Слушаю.', working: 'Помощник выполняет поручение…', ending: 'Завершаем разговор…',
        disabled: 'Голос пока не подключён.' } as Record<string, string>)[value.phase] ?? 'Голосовой разговор';
    assign(state, label); start.disabled = !value.configured || value.active;
    mute.hidden = interrupt.hidden = end.hidden = !value.active; mute.textContent = value.muted ? 'Включить микрофон' : 'Выключить микрофон';
    mute.setAttribute('aria-pressed', String(value.muted));
    resume.hidden = !value.active || !value.outputMuted && !value.playbackBlocked; check.hidden = !value.active || value.phase !== 'unknown';
    allowNote.disabled = !value.active || value.phase === 'working' || value.phase === 'ending'; permission.hidden = !value.configured;
    if (!value.active) allowNote.checked = false;
    audio.hidden = !value.active; disclosure.hidden = !value.configured;
    assign(editorStatus, value.editor?.state === 'opened' ? 'Проект открыт в редакторе.'
      : value.editor?.state === 'opening' ? 'Открываем проект…' : value.editor ? 'Открытие редактора не подтверждено.' : '');
    assign(input, value.input ? `Вы: ${value.input}` : ''); assign(output, value.output ? `Помощник: ${value.output}` : ''); assign(result, value.answer);
    host.ownerDocument.dispatchEvent(new CustomEvent('soty:voice-orb-state', { detail: {
      accountId: options.accountId, active: value.active, phase: value.phase, microphone: value.microphone,
    } }));
  };
  const voice = createVoiceOrbClient({ ...options, audio, onChange: changed,
    takeNotePermission() { const selected = allowNote.checked; allowNote.checked = false; return selected; } });
  const unobserve = voice.subscribe(changed), refreshed = voice.refresh();
  return {
    async startFromOrb(): Promise<void> { await refreshed; if (options.isCurrent() && voice.view().configured) await voice.start(); },
    active: (): boolean => voice.view().active,
    dispose(): void { unobserve(); voice.dispose(); root.remove(); },
  };
}
