import { useEffect, useRef, useState } from 'react';
import type { TimeRange } from '../../shared/types';
import {
  canonicalRange,
  NS_PER_SECOND,
  nsToISO,
  type TimeEdge,
  withRangeEdgeNs,
} from '../../shared/precise-time';
import { calendarRangeRepresentable, withCalendarCoordinates } from '../utils';
import { formatOffset, instantLabel, parseDuration } from '../../shared/universal-timeline';

export type TimeContext = { anchorNs: string; label: string };

type Props = {
  value: TimeRange;
  onChange: (value: TimeRange) => void;
  timeContext?: TimeContext;
  onInvalidChange?: (invalid: boolean) => void;
  calendarOnly?: boolean;
  showResolution?: boolean;
};

/** Human relative editor over lossless absolute epoch-nanosecond coordinates. */
export default function PreciseRangeFields({
  value,
  onChange,
  timeContext,
  onInvalidChange,
  calendarOnly = false,
  showResolution = true,
}: Props) {
  const coordinates = canonicalRange(value);
  const existing = coordinates.start ?? coordinates.end;
  const calendarAnchor = existing !== null && nsToISO(existing, value.timezone) !== null;
  const fallbackAnchor = calendarAnchor
    ? existing! - (((existing! % NS_PER_SECOND) + NS_PER_SECOND) % NS_PER_SECOND)
    : 0n;
  const initialAnchor = useRef(timeContext ? BigInt(timeContext.anchorNs) : fallbackAnchor);
  const initialLabel = useRef(
    timeContext?.label ??
      (calendarAnchor
        ? instantLabel(fallbackAnchor, value.timezone, true)
        : '1970 · точка отсчёта'),
  );
  const contextAnchor = initialAnchor.current;
  const contextLabel = initialLabel.current;
  const [startOffset, setStartOffset] = useState(() =>
    coordinates.start === null ? '' : formatOffset(coordinates.start - contextAnchor),
  );
  const [endOffset, setEndOffset] = useState(() =>
    coordinates.end === null ? '' : formatOffset(coordinates.end - contextAnchor),
  );
  const [earliestOffset, setEarliestOffset] = useState(() =>
    coordinates.earliest === null ? '' : formatOffset(coordinates.earliest - contextAnchor),
  );
  const [latestOffset, setLatestOffset] = useState(() =>
    coordinates.latest === null ? '' : formatOffset(coordinates.latest - contextAnchor),
  );
  const [resolutionOffset, setResolutionOffset] = useState(() =>
    value.precise?.resolutionNs ? formatOffset(BigInt(value.precise.resolutionNs)) : '',
  );
  const [error, setError] = useState('');
  const calendarGuard = useRef(false);
  const lastEmitted = useRef<string | null>(null);
  const preciseStart = value.precise?.start ?? '';
  const preciseEnd = value.precise?.end ?? '';
  const preciseEarliest = value.precise?.earliest ?? '';
  const preciseLatest = value.precise?.latest ?? '';
  const preciseResolution = value.precise?.resolutionNs ?? '';
  const identity = (range: TimeRange, anchor: bigint) => {
    const current = canonicalRange(range);
    return `${range.timezone}|${anchor}|${current.start ?? ''}|${current.end ?? ''}|${current.earliest ?? ''}|${current.latest ?? ''}`;
  };

  useEffect(() => {
    if (lastEmitted.current === identity(value, contextAnchor)) {
      lastEmitted.current = null;
      return;
    }
    const next = canonicalRange(value);
    setStartOffset(next.start === null ? '' : formatOffset(next.start - contextAnchor));
    setEndOffset(next.end === null ? '' : formatOffset(next.end - contextAnchor));
    setEarliestOffset(next.earliest === null ? '' : formatOffset(next.earliest - contextAnchor));
    setLatestOffset(next.latest === null ? '' : formatOffset(next.latest - contextAnchor));
    setResolutionOffset(
      value.precise?.resolutionNs ? formatOffset(BigInt(value.precise.resolutionNs)) : '',
    );
    setError('');
    onInvalidChange?.(false);
  }, [
    timeContext?.anchorNs,
    value.timezone,
    preciseStart,
    preciseEnd,
    preciseEarliest,
    preciseLatest,
    preciseResolution,
  ]);

  function emit(next: TimeRange): boolean {
    if (calendarOnly && !calendarRangeRepresentable(next)) {
      calendarGuard.current = true;
      setError('Выберите дату в диапазоне календаря 0001–9999.');
      return false;
    }
    calendarGuard.current = false;
    lastEmitted.current = identity(next, contextAnchor);
    onChange(calendarOnly ? withCalendarCoordinates(next) : next);
    return true;
  }

  function updateEdge(edge: TimeEdge, raw: string) {
    const values = {
      start: startOffset,
      end: endOffset,
      earliest: earliestOffset,
      latest: latestOffset,
    };
    values[edge] = raw;
    const invalid =
      Object.values(values).some((entry) => entry.trim() && parseDuration(entry) === null) ||
      (Boolean(resolutionOffset.trim()) && (parseDuration(resolutionOffset) ?? 0n) <= 0n);
    if (!raw.trim()) {
      const next = withRangeEdgeNs(value, edge, null);
      const nextCoordinates = canonicalRange(next);
      if (
        nextCoordinates.start === null &&
        nextCoordinates.end === null &&
        nextCoordinates.earliest === null &&
        nextCoordinates.latest === null
      ) {
        const { precise: _precise, ...undated } = next;
        if (!emit({ ...undated, precision: 'unknown' })) {
          onInvalidChange?.(true);
          return;
        }
      } else {
        if (!emit(next)) {
          onInvalidChange?.(true);
          return;
        }
      }
      setError(invalid ? 'Исправьте остальные поля точного времени перед сохранением.' : '');
      onInvalidChange?.(invalid || calendarGuard.current);
      return;
    }
    const duration = parseDuration(raw);
    if (duration === null) {
      setError('Проверьте формат точного времени: например, 2 млн лет, 20 мс или 0,1 мкс.');
      onInvalidChange?.(invalid);
      return;
    }
    if (!emit(withRangeEdgeNs(value, edge, contextAnchor + duration))) {
      onInvalidChange?.(true);
      return;
    }
    setError(invalid ? 'Исправьте остальные поля точного времени перед сохранением.' : '');
    onInvalidChange?.(invalid || calendarGuard.current);
  }

  function changeOffset(edge: TimeEdge, raw: string) {
    if (edge === 'start') setStartOffset(raw);
    else if (edge === 'end') setEndOffset(raw);
    else if (edge === 'earliest') setEarliestOffset(raw);
    else setLatestOffset(raw);
    updateEdge(edge, raw);
  }

  function changeResolution(raw: string) {
    setResolutionOffset(raw);
    const resolution = raw.trim() ? parseDuration(raw) : null;
    const offsets = [startOffset, endOffset, earliestOffset, latestOffset];
    const invalidOffsets = offsets.some((entry) => entry.trim() && parseDuration(entry) === null);
    const invalidResolution = Boolean(raw.trim()) && (resolution === null || resolution <= 0n);
    onInvalidChange?.(invalidOffsets || invalidResolution);
    if (invalidResolution) {
      setError('Разрешение должно быть точным положительным интервалом, например 1 мкс.');
      return;
    }
    const precise = {
      ...value.precise,
      scale: 'unix-nanoseconds' as const,
      start: value.precise?.start ?? coordinates.start?.toString() ?? null,
      end: value.precise?.end ?? coordinates.end?.toString() ?? null,
    };
    if (resolution === null) delete precise.resolutionNs;
    else precise.resolutionNs = resolution.toString();
    emit({ ...value, precise });
    setError(invalidOffsets ? 'Исправьте остальные поля точного времени перед сохранением.' : '');
  }

  return (
    <div className="precise-range-fields">
      <div className="precise-anchor-field" aria-label="Точка отсчёта точного времени">
        <span>Отсчёт от</span>
        <strong>{contextLabel}</strong>
        <small>{value.timezone}</small>
      </div>
      <div className="precise-offset-grid">
        <label>
          Начало
          <input
            type="text"
            inputMode="text"
            aria-label="Смещение начала от точки отсчёта"
            placeholder="например, 20 мс"
            value={startOffset}
            onChange={(event) => {
              changeOffset('start', event.target.value);
            }}
          />
        </label>
        <label>
          Окончание <span className="field-optional">необязательно</span>
          <input
            type="text"
            inputMode="text"
            aria-label="Смещение окончания от точки отсчёта"
            placeholder="например, 2 мс"
            value={endOffset}
            onChange={(event) => {
              changeOffset('end', event.target.value);
            }}
          />
        </label>
        {(value.precision === 'approximate' ||
          value.earliest != null ||
          value.latest != null ||
          value.precise?.earliest != null ||
          value.precise?.latest != null) && (
          <>
            <label>
              Не раньше
              <input
                type="text"
                inputMode="text"
                aria-label="Смещение нижней границы от точки отсчёта"
                placeholder="например, −2 млн лет"
                value={earliestOffset}
                onChange={(event) => changeOffset('earliest', event.target.value)}
              />
            </label>
            <label>
              Не позже
              <input
                type="text"
                inputMode="text"
                aria-label="Смещение верхней границы от точки отсчёта"
                placeholder="например, 20 мс"
                value={latestOffset}
                onChange={(event) => changeOffset('latest', event.target.value)}
              />
            </label>
          </>
        )}
      </div>
      <p className="precise-range-hint">
        Можно вводить годы, дни, часы, минуты, секунды и доли секунды. Значение сохраняется точно.
      </p>
      {showResolution && (
        <label className="precise-resolution-field">
          Разрешение <span className="field-optional">необязательно</span>
          <input
            type="text"
            inputMode="text"
            aria-label="Точность измерения"
            title="Шаг точности сохраняемого значения, например 1 мкс"
            placeholder="например, 1 мкс"
            value={resolutionOffset}
            onChange={(event) => changeResolution(event.target.value)}
          />
        </label>
      )}
      {error && (
        <p className="precise-range-error" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
