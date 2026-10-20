import { el } from './dom';
import { icon } from './icons';

export type AudienceMode = 'private' | 'friends' | 'link' | 'public';
export const audienceChoices = [
  { value: 'private', label: 'Только я', short: 'Я', symbol: 'lock' },
  { value: 'friends', label: 'Друзья', short: 'Друзья', symbol: 'people' },
  { value: 'link', label: 'По ссылке', short: 'Ссылка', symbol: 'link' },
  { value: 'public', label: 'Все', short: 'Все', symbol: 'world' },
] as const;

/** Controlled editor only. The owner keeps its existing draft, consent,
 * durable intent and confirmed server readback; a radio never grants access. */
export function createAudienceControl(options: {
  value: AudienceMode | null;
  onChange(value: AudienceMode): void;
  descriptions: Record<AudienceMode, string>;
}): { element: HTMLFieldSetElement; update(value: AudienceMode | null, disabled?: boolean): void } {
  const element = el('fieldset', 'sw-audience-control'), legend = el('legend', '', 'Кому видно');
  const track = el('div', 'sw-audience-track'), description = el('p', 'sw-audience-description');
  description.id = `audience-description-${crypto.randomUUID()}`;
  element.setAttribute('aria-describedby', description.id);
  const name = `audience-${crypto.randomUUID()}`;
  const inputs = audienceChoices.map(choice => {
    const label = el('label', 'sw-audience-choice'), input = el('input');
    input.type = 'radio'; input.name = name; input.value = choice.value;
    input.setAttribute('aria-label', choice.label); input.title = choice.label;
    input.addEventListener('change', () => { if (input.checked && !input.disabled) options.onChange(choice.value); });
    label.append(input, icon(choice.symbol), el('span', '', choice.short)); track.append(label);
    return input;
  });
  const update = (value: AudienceMode | null, disabled = false): void => {
    for (const input of inputs) { input.checked = input.value === value; input.disabled = disabled; }
    description.textContent = value === null ? 'Особый доступ: выбранные люди и сообщества.' : options.descriptions[value];
    element.dataset.audience = value ?? 'custom';
  };
  element.append(legend, track, description); update(options.value);
  return { element, update };
}
