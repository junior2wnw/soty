import {
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type ReactNode,
  type PointerEvent as ReactPointerEvent,
} from 'react';
import { DateTime } from 'luxon';
import {
  AlertTriangle,
  ArrowLeft,
  ArrowRight,
  CalendarDays,
  Clock3,
  FileText,
  Flag,
  Layers3,
  LocateFixed,
  Plus,
  Repeat2,
  TrendingUp,
} from 'lucide-react';
import type {
  Entity,
  Occurrence,
  PlanChange,
  PlannerSnapshot,
  TimeRange,
} from '../../shared/types';
import { expandRecurrence, MAX_RECURRENCE_OCCURRENCES, parseTime } from '../../shared/engine';
import { factualRangeForDrawing } from '../../shared/factual-range';
import { displayRangeEnd } from '../../shared/local-range';
import { recurrenceNavigation, type OccurrenceSearch } from '../../shared/recurrence-navigation';
import {
  MIN_SPAN,
  normalizeViewport,
  panViewport,
  timelineTicks,
  viewportBounds,
  zoomViewport,
  zoomToSpan,
  type TimelineViewport,
  project,
  timeAt,
  fromMillis,
  MS,
  SECOND,
  MINUTE,
  DAY as DAY_NS,
  YEAR,
  minNs,
  maxNs,
  clampNs,
  instantLabel,
  formatDuration,
  focusRange,
} from '../../shared/universal-timeline';
import {
  advanceWheelZoom,
  retargetWheelZoom,
  wheelZoomDelta,
  type WheelZoomMotion,
} from '../../shared/smooth-zoom';
import { packTimeline, TIMELINE_START, TIMELINE_END } from '../../shared/timeline';
import {
  canonicalRange as readCoordinates,
  isoToNs,
  nsToISO,
  hasTime,
  shiftRangeNs,
  withRangeEdgeNs,
  rangeFromNs,
} from '../../shared/precise-time';
import {
  canEdit,
  dateLabel,
  kindLabels,
  rangeLabel,
  requiresAttention,
  statusLabels,
} from '../utils';

// Retained only to read previously saved views. The canvas has one shared axis and no grouping modes.
export type GroupMode = 'process' | 'type' | 'owner' | 'workspace';
type Props = {
  snapshot: PlannerSnapshot;
  entities: Entity[];
  viewport: TimelineViewport;
  onNavigate: (viewport: TimelineViewport) => void;
  selectedId: string | null;
  onSelect: (entity: Entity) => void;
  onDeselect?: () => void;
  onCreate: (at: bigint) => void;
  onPropose: (entity: Entity, range: TimeRange) => void;
  detail: ReactNode;
  readOnly: boolean;
  proposed?: PlanChange[];
};
type Layer = 'plan' | 'forecast' | 'actual' | 'proposal' | 'deadline';
type RangeLayer = { layer: Layer; range: TimeRange };
type Shape = RangeLayer & {
  left: number;
  right: number;
  atStart: bigint;
  atEnd: bigint;
  point: boolean;
  openStart: boolean;
  openEnd: boolean;
  mainVisible: boolean;
  uncertaintyLeft?: number;
  uncertaintyRight?: number;
};
type EventItem = {
  kind: 'event';
  key: string;
  entity: Entity;
  shapes: Shape[];
  left: number;
  right: number;
  labelLeft: number;
  labelWidth: number;
  atStart: bigint;
  atEnd: bigint;
  occurrenceIndex?: number;
  base: boolean;
};
type OccurrenceContext = {
  entityId: string;
  entityVersion: number;
  index: number;
  range: TimeRange;
};
type ClusterItem = {
  kind: 'cluster';
  key: string;
  entities: Entity[];
  left: number;
  right: number;
  atStart: bigint;
  atEnd: bigint;
  count: number | null;
  lowerBound?: boolean;
  ruleCount?: number;
  exceptions?: number;
  layers: Layer[];
  summary: string;
  verified: boolean;
};
type Item = EventItem | ClusterItem;
type EditGesture = {
  pointerId: number;
  entity: Entity;
  range: TimeRange;
  edge: 'move' | 'start' | 'end';
  x: number;
  span: bigint;
  width: number;
  delta: bigint;
};
type PanGesture = {
  pointerId: number;
  x: number;
  y: number;
  width: number;
  viewport: TimelineViewport;
  touch: boolean;
  locked: 'pending' | 'pan' | 'vertical';
};
type PinchGesture = {
  distance: number;
  midpoint: number;
  fraction: number;
  width: number;
  viewport: TimelineViewport;
};
const ICONS = {
  point: CalendarDays,
  period: Clock3,
  process: Layers3,
  note: FileText,
  metric: TrendingUp,
};
const LAYER_TOP: Record<Layer, number> = {
  plan: 28,
  forecast: 72,
  actual: 62,
  proposal: 82,
  deadline: 34,
};
const LAYER_NAMES: Record<Layer, string> = {
  plan: 'План',
  forecast: 'Прогноз',
  actual: 'Факт',
  proposal: 'Предложение',
  deadline: 'Обязательство',
};
const LANE_HEIGHT = 96;
const DAY = 86_400_000;
const MAX_VISIBLE_SERIES_ITEMS = 600;
const MAX_VISIBLE_TOTAL_ITEMS = 4_000;
const clamp = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));
const stamp = (value: string | null | undefined, zone: string) =>
  parseTime(value, zone)?.toMillis() ?? null;
// Snapshot ranges are immutable; reuse date parsing while the camera moves.
const coordinateCache = new WeakMap<TimeRange, ReturnType<typeof readCoordinates>>();
const canonicalRange = (range: TimeRange) => {
  let value = coordinateCache.get(range);
  if (!value) {
    value = readCoordinates(range);
    coordinateCache.set(range, value);
  }
  return value;
};

function editedRange(range: TimeRange, nsDelta: bigint, edge: EditGesture['edge']): TimeRange {
  const coordinates = canonicalRange(range);
  const candidate = edge === 'end' ? coordinates.end : coordinates.start;
  if (
    range.precise ||
    (range.precision === 'exact' && nsDelta % MS !== 0n) ||
    (candidate != null && nsToISO(candidate + nsDelta, range.timezone) == null)
  ) {
    if (edge === 'move') return shiftRangeNs(range, nsDelta);
    return withRangeEdgeNs(range, edge, candidate == null ? null : candidate + nsDelta);
  }
  const delta = Number(nsDelta / MS);
  const shift = (value: string | null | undefined) =>
    value == null
      ? value
      : (parseTime(value, range.timezone)
          ?.plus(
            range.precision === 'day'
              ? { days: delta / DAY }
              : range.precision === 'month'
                ? { months: delta / (30 * DAY) }
                : { milliseconds: delta },
          )
          .toISO() ?? value);
  return {
    ...range,
    start: edge === 'end' ? range.start : shift(range.start)!,
    end: edge === 'start' ? range.end : shift(range.end)!,
    ...(edge === 'move' && range.earliest !== undefined ? { earliest: shift(range.earliest) } : {}),
    ...(edge === 'move' && range.latest !== undefined ? { latest: shift(range.latest) } : {}),
  };
}

function preciseLabel(range: TimeRange): string {
  if (range.precise) {
    const time = canonicalRange(range);
    if (time.start == null && time.end == null) return 'Без даты';
    if (time.start == null) return 'До ' + instantLabel(time.end!, range.timezone, true);
    if (time.end == null || time.start === time.end)
      return instantLabel(time.start, range.timezone, true);
    return (
      instantLabel(time.start, range.timezone, true) +
      ' — ' +
      instantLabel(time.end, range.timezone, true) +
      ' · ' +
      formatDuration(time.end - time.start)
    );
  }
  const format =
    range.precision === 'exact'
      ? 'd MMM yyyy, HH:mm:ss'
      : range.precision === 'month'
        ? 'LLLL yyyy'
        : 'd MMM yyyy';
  const label = (value: string | null) =>
    value
      ? range.precision === 'exact' && isoToNs(value, range.timezone) != null
        ? instantLabel(isoToNs(value, range.timezone)!, range.timezone, true)
        : (parseTime(value, range.timezone)?.setLocale('ru').toFormat(format) ??
          'Некорректная дата')
      : '';
  const prefix =
    range.precision === 'approximate'
      ? 'Приблизительно: '
      : range.precision === 'unknown'
        ? 'Точность не задана: '
        : '';
  if (!range.start && !range.end) return 'Без даты';
  if (!range.start) return `${prefix}До ${label(displayRangeEnd(range))}; начало не задано`;
  if (!range.end) return `${prefix}${label(range.start)}`;
  return `${prefix}${label(range.start)} — ${label(displayRangeEnd(range))}`;
}

function timezoneLabel(range: TimeRange): string {
  if (range.precision !== 'exact') return range.timezone;
  const start = parseTime(range.start ?? range.end, range.timezone),
    end = parseTime(range.end, range.timezone);
  if (!start) return range.timezone;
  const offset = `UTC${start.toFormat('ZZ')}`;
  return `${range.timezone} · ${offset}${end && end.offset !== start.offset ? ` → UTC${end.toFormat('ZZ')}` : ''}`;
}

function shapeOf(
  entity: Entity,
  value: RangeLayer,
  bounds: { start: bigint; end: bigint },
  width: number,
): Shape | null {
  const range = value.layer === 'actual' ? factualRangeForDrawing(value.range) : value.range;
  let { start, end, earliest, latest } = canonicalRange(range);
  if (value.layer === 'actual' && range.precise && (start == null || end == null)) {
    start = start ?? end;
    end = start;
  }
  if (start == null && end == null) return null;
  const durationKind = entity.kind === 'period' || entity.kind === 'process';
  const openStart = start == null && end != null;
  const openEnd = start != null && end == null && durationKind && value.layer !== 'deadline';
  // Day/month precision is a band on the calendar, never a fabricated midnight appointment.
  if (
    !range.precise &&
    start != null &&
    ['day', 'month'].includes(range.precision) &&
    value.layer !== 'deadline'
  ) {
    const unit = range.precision === 'month' ? 'month' : 'day';
    start = fromMillis(parseTime(range.start, range.timezone)!.startOf(unit).toMillis());
    if (end != null) {
      const last = parseTime(range.end, range.timezone)!;
      const boundary = last.startOf(unit);
      end =
        last.toMillis() === boundary.toMillis() && fromMillis(boundary.toMillis()) > start
          ? fromMillis(boundary.toMillis())
          : fromMillis(boundary.plus(unit === 'month' ? { months: 1 } : { days: 1 }).toMillis());
    } else if (!durationKind)
      end = fromMillis(
        parseTime(range.start, range.timezone)!
          .startOf(unit)
          .plus(unit === 'month' ? { months: 1 } : { days: 1 })
          .toMillis(),
      );
  }
  const atStart = start ?? bounds.start,
    atEnd = end ?? (openEnd ? bounds.end : atStart);
  if (atEnd < atStart) return null;
  const point = atEnd === atStart && !openStart && !openEnd;
  const mainVisible = point
    ? atStart >= bounds.start && atStart < bounds.end
    : atStart < bounds.end && atEnd > bounds.start;
  const uncertaintyVisible =
    earliest != null &&
    latest != null &&
    latest >= earliest &&
    earliest < bounds.end &&
    latest >= bounds.start;
  if (!mainVisible && !uncertaintyVisible) return null;
  const x = (at: bigint) =>
    clamp((Number(at - bounds.start) / Number(bounds.end - bounds.start)) * width, 0, width);
  const uncertaintyLeft = uncertaintyVisible ? x(earliest!) : undefined,
    uncertaintyRight = uncertaintyVisible ? x(latest!) : undefined;
  const left = mainVisible ? x(atStart) : uncertaintyLeft!,
    right = mainVisible ? x(atEnd) : uncertaintyRight!;
  return {
    ...value,
    left,
    right,
    atStart,
    atEnd,
    point,
    openStart,
    openEnd,
    mainVisible,
    uncertaintyLeft,
    uncertaintyRight,
  };
}

function eventItem(
  entity: Entity,
  ranges: RangeLayer[],
  bounds: { start: bigint; end: bigint },
  width: number,
  key: string,
  base: boolean,
  occurrenceIndex?: number,
): EventItem | null {
  const shapes = ranges.flatMap((value) => {
    const shape = shapeOf(entity, value, bounds, width);
    return shape ? [shape] : [];
  });
  if (!shapes.length) return null;
  const first = Math.min(...shapes.map((s) => Math.min(s.left, s.uncertaintyLeft ?? s.left))),
    last = Math.max(...shapes.map((s) => Math.max(s.right, s.uncertaintyRight ?? s.right)));
  const metric =
    entity.kind === 'metric' && entity.fields.value != null
      ? ` · ${entity.fields.value} ${entity.fields.unit ?? ''}`
      : '';
  const mainShape = shapes.find((shape) => shape.layer === 'plan') ?? shapes[0]!;
  const inlineRoom = mainShape.point ? 0 : mainShape.right - mainShape.left - 16;
  const labelWidth = Math.min(
    width,
    clamp(
      (entity.title.length + metric.length) * 6.7 + 52 + (entity.recurrence ? 16 : 0),
      110,
      Math.max(width < 600 ? 160 : 190, Math.min(420, inlineRoom)),
    ),
  );
  const labelLeft = clamp(
    mainShape.left + (mainShape.point ? 14 : inlineRoom > 100 ? 4 : 0),
    0,
    Math.max(0, width - labelWidth),
  );
  return {
    kind: 'event',
    key,
    entity,
    shapes,
    left: Math.min(first, labelLeft),
    right: Math.max(last, labelLeft + labelWidth),
    labelLeft,
    labelWidth,
    atStart: minNs(...shapes.map((shape) => shape.atStart)),
    atEnd: maxNs(...shapes.map((shape) => shape.atEnd)),
    occurrenceIndex,
    base,
  };
}

function recurrenceEnvelope(
  entity: Entity,
): { start: number; end: number; exactEnd: boolean } | null {
  const rule = entity.recurrence,
    base = parseTime(entity.plan.start, entity.plan.timezone);
  if (!rule || !base) return null;
  const duration = Math.max(
    0,
    (stamp(entity.plan.end, entity.plan.timezone) ?? base.toMillis()) - base.toMillis(),
  );
  let last = TIMELINE_END,
    exactEnd = false;
  if (rule.until) {
    const until = parseTime(rule.until, entity.plan.timezone);
    if (until) last = (rule.until.length === 10 ? until.endOf('day') : until).toMillis() + duration;
  }
  if (rule.count) {
    // For an unexpanded RFC series this is a conservative envelope, explicitly labelled below.
    const factor = rule.calendarPolicy === 'skip-invalid' ? 8 : 1;
    let ending: DateTime;
    if (rule.frequency === 'week') {
      const days = [...new Set(rule.weekdays?.length ? rule.weekdays : [base.weekday])].sort(
        (a, b) => a - b,
      );
      const firstWeek = days.filter((day) => day >= base.weekday);
      const ordinal = rule.count - 1;
      const week =
        ordinal < firstWeek.length ? 0 : 1 + Math.floor((ordinal - firstWeek.length) / days.length);
      const day =
        ordinal < firstWeek.length
          ? firstWeek[ordinal]
          : days[(ordinal - firstWeek.length) % days.length];
      ending = base
        .startOf('week')
        .plus({ weeks: week * rule.interval * factor, days: day - 1 })
        .set({
          hour: base.hour,
          minute: base.minute,
          second: base.second,
          millisecond: base.millisecond,
        });
    } else
      ending = base.plus(
        rule.frequency === 'day'
          ? { days: (rule.count - 1) * rule.interval * factor }
          : { months: (rule.count - 1) * rule.interval * factor },
      );
    if (ending.isValid) {
      last = Math.min(last, ending.toMillis() + duration);
      exactEnd = factor === 1;
    }
  }
  return { start: base.toMillis(), end: last, exactEnd };
}

function clusterItem(
  key: string,
  entities: Entity[],
  from: bigint,
  to: bigint,
  bounds: { start: bigint; end: bigint },
  width: number,
  extra: Omit<ClusterItem, 'kind' | 'key' | 'entities' | 'left' | 'right' | 'atStart' | 'atEnd'>,
): ClusterItem {
  const first = clamp(
      (Number(from - bounds.start) / Number(bounds.end - bounds.start)) * width,
      0,
      width,
    ),
    last = clamp((Number(to - bounds.start) / Number(bounds.end - bounds.start)) * width, 0, width);
  const labelWidth = Math.min(width, key.startsWith('density:') ? 44 : width < 600 ? 150 : 190),
    left = Math.min(first, Math.max(0, width - labelWidth));
  return {
    kind: 'cluster',
    key,
    entities,
    left,
    right: Math.max(last, left + labelWidth),
    atStart: from,
    atEnd: to,
    ...extra,
  };
}

export default function Timeline({
  snapshot,
  entities,
  viewport,
  onNavigate,
  selectedId,
  onSelect,
  onDeselect,
  onCreate,
  onPropose,
  detail,
  readOnly,
  proposed,
}: Props) {
  const plot = useRef<HTMLDivElement>(null),
    canvas = useRef<HTMLDivElement>(null),
    detailElement = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(800),
    [moving, setMoving] = useState(false),
    [edit, setEdit] = useState<{
      entityId: string;
      delta: bigint;
      edge: EditGesture['edge'];
    } | null>(null);
  const [detailRequest, setDetailRequest] = useState(0);
  const [occurrenceContext, setOccurrenceContext] = useState<OccurrenceContext | null>(null);
  const view = normalizeViewport(viewport),
    bounds = viewportBounds(view);
  const laneHeight = proposed?.length ? 112 : LANE_HEIGHT;
  const viewRef = useRef(view),
    committedView = useRef(viewport),
    pendingView = useRef<TimelineViewport | null>(null),
    submittedView = useRef<TimelineViewport | null>(null),
    submittedViews = useRef<TimelineViewport[]>([]),
    zoomMotion = useRef<WheelZoomMotion | null>(null),
    navigateRef = useRef(onNavigate),
    frame = useRef<number | null>(null);
  viewRef.current = pendingView.current ?? submittedView.current ?? view;
  navigateRef.current = onNavigate;
  const pointers = useRef(new Map<number, { x: number; y: number }>()),
    pan = useRef<PanGesture | null>(null),
    pinch = useRef<PinchGesture | null>(null),
    editing = useRef<EditGesture | null>(null);
  const suppressClickUntil = useRef(0);
  const previousLanes = useRef(new Map<string, number>());
  const [hover, setHover] = useState<{ x: number; at: bigint } | null>(null);
  const zone =
    snapshot.settings.notifications.timezone || snapshot.workspaces[0]?.timezone || 'UTC';
  const now = isoToNs(snapshot.serverTime, zone) ?? fromMillis(Date.now());
  const selected = snapshot.entities.find((entity) => entity.id === selectedId);
  const selectedIsSeries = !!selected?.recurrence;
  const activeOccurrence =
    selectedIsSeries &&
    occurrenceContext?.entityId === selectedId &&
    occurrenceContext.entityVersion === selected?.version
      ? occurrenceContext
      : null;
  const seriesNavigation = useMemo(
    () =>
      selectedIsSeries && selected
        ? recurrenceNavigation(selected, activeOccurrence?.range.start)
        : null,
    [selectedId, selected?.version, selectedIsSeries, activeOccurrence?.range.start],
  );
  const typeMap = useMemo(
    () => new Map(snapshot.types.map((type) => [type.id, type])),
    [snapshot.types],
  );
  const connections = snapshot.dependencies.filter(
    (dep) => dep.fromId === selectedId || dep.toId === selectedId,
  );
  const related = new Set(connections.flatMap((dep) => [dep.fromId, dep.toId]));
  const signals = new Set(
    snapshot.signals.filter(requiresAttention).map((signal) => signal.entityId),
  );

  useEffect(() => {
    setOccurrenceContext((current) =>
      current &&
      (current.entityId !== selectedId ||
        !selectedIsSeries ||
        current.entityVersion !== selected?.version)
        ? null
        : current,
    );
  }, [selectedId, selectedIsSeries, selected?.version]);

  function publishView(next: TimelineViewport) {
    submittedView.current = next;
    submittedViews.current.push(next);
    if (submittedViews.current.length > 32) submittedViews.current.shift();
    viewRef.current = next;
    navigateRef.current(next);
  }
  function scheduleNavigation() {
    if (frame.current != null) return;
    frame.current = requestAnimationFrame((now) => {
      frame.current = null;
      const pending = pendingView.current;
      if (pending) {
        pendingView.current = null;
        publishView(pending);
      } else if (zoomMotion.current) {
        const next = advanceWheelZoom(viewRef.current, zoomMotion.current, now);
        zoomMotion.current = next.motion;
        if (next.view.center !== viewRef.current.center || next.view.span !== viewRef.current.span)
          publishView(next.view);
        if (zoomMotion.current) scheduleNavigation();
      }
    });
  }
  function stopZoom() {
    zoomMotion.current = null;
    if (!pendingView.current && frame.current != null) {
      cancelAnimationFrame(frame.current);
      frame.current = null;
    }
  }
  function navigate(next: TimelineViewport) {
    stopZoom();
    pendingView.current = normalizeViewport(next);
    viewRef.current = pendingView.current;
    scheduleNavigation();
  }
  useLayoutEffect(() => {
    if (viewport !== committedView.current) {
      const submittedIndex = submittedViews.current.indexOf(viewport);
      if (submittedIndex >= 0) {
        submittedViews.current.splice(0, submittedIndex + 1);
        if (submittedView.current === viewport) submittedView.current = null;
      } else {
        // A parent fit/slider/date command takes precedence over queued gestures.
        stopZoom();
        pendingView.current = null;
        submittedView.current = null;
        submittedViews.current = [];
        if (frame.current != null) cancelAnimationFrame(frame.current);
        frame.current = null;
      }
      committedView.current = viewport;
    }
    viewRef.current = pendingView.current ?? submittedView.current ?? view;
  }, [viewport]);
  useEffect(() => {
    const element = plot.current;
    if (!element) return;
    const measure = () => setWidth(Math.max(1, element.getBoundingClientRect().width));
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(element);
    const wheel = (event: WheelEvent) => {
      const rect = element.getBoundingClientRect();
      if (!rect.width) return;
      const multiplier = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? rect.height : 1;
      if (
        (!event.ctrlKey &&
          !event.metaKey &&
          Math.abs(event.deltaX) > Math.abs(event.deltaY) * 0.7) ||
        event.shiftKey
      ) {
        event.preventDefault();
        navigate(
          panViewport(viewRef.current, (event.deltaX || event.deltaY) * multiplier, rect.width),
        );
      } else {
        event.preventDefault();
        const delta = wheelZoomDelta(event.deltaY, event.deltaMode, rect.height);
        if (!delta) return;
        setHover(null);
        const motion = retargetWheelZoom(
          viewRef.current,
          zoomMotion.current,
          delta,
          (event.clientX - rect.left) / rect.width,
          performance.now(),
        );
        if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
          if (motion) navigate(zoomToSpan(motion.origin, motion.targetSpan, motion.fraction));
        } else {
          zoomMotion.current = motion;
          if (motion) scheduleNavigation();
        }
      }
    };
    element.addEventListener('wheel', wheel, { passive: false });
    return () => {
      observer.disconnect();
      element.removeEventListener('wheel', wheel);
      if (frame.current != null) cancelAnimationFrame(frame.current);
      frame.current = null;
      zoomMotion.current = null;
    };
  }, []);
  useEffect(() => {
    if (!selectedId) return;
    let secondFrame: number | undefined;
    const firstFrame = requestAnimationFrame(() => {
      secondFrame = requestAnimationFrame(() => {
        const element = detailElement.current;
        if (!element) return;
        const rect = element.getBoundingClientRect();
        if (rect.top < 0 || rect.top + Math.min(rect.height, 160) > window.innerHeight)
          element.scrollIntoView({
            block: 'start',
            behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches
              ? 'auto'
              : 'smooth',
          });
      });
    });
    return () => {
      cancelAnimationFrame(firstFrame);
      if (secondFrame != null) cancelAnimationFrame(secondFrame);
    };
  }, [selectedId, detailRequest]);

  const layout = useMemo(() => {
    const started = performance.now();
    const currentBounds = viewportBounds(view),
      items: Item[] = [],
      undated: Entity[] = [];
    const seen = new Set<string>();
    let budget = MAX_VISIBLE_TOTAL_ITEMS;
    for (const original of entities) {
      let entity = original;
      if (edit?.entityId === entity.id) {
        entity = { ...entity, plan: editedRange(entity.plan, edit.delta, edit.edge) };
      }
      if (!hasTime(entity.plan)) undated.push(original);
      const other: RangeLayer[] = [];
      if (entity.forecast) other.push({ layer: 'forecast', range: entity.forecast });
      if (entity.actual) other.push({ layer: 'actual', range: entity.actual });
      const proposal = proposed?.find((change) => change.entityId === entity.id);
      if (proposal) other.push({ layer: 'proposal', range: proposal.plan });
      if (entity.dueAt)
        other.push({
          layer: 'deadline',
          range: {
            start: entity.dueAt,
            end: null,
            timezone: entity.plan.timezone,
            precision: 'exact',
          },
        });
      const envelope = recurrenceEnvelope(entity);
      if (!entity.recurrence || !envelope) {
        const item = eventItem(
          entity,
          [{ layer: 'plan', range: entity.plan }, ...other],
          currentBounds,
          width,
          entity.id,
          true,
        );
        if (item) {
          items.push(item);
          seen.add(entity.id);
        }
        continue;
      }
      let facts = eventItem(entity, other, currentBounds, width, `${entity.id}:records`, true);
      const range = entity.plan,
        rule = entity.recurrence;
      const calendarBounds = {
        start: Math.max(TIMELINE_START, Math.floor(Number(currentBounds.start) / Number(MS))),
        end: Math.min(TIMELINE_END, Math.ceil(Number(currentBounds.end) / Number(MS))),
      };
      if (
        calendarBounds.end > calendarBounds.start &&
        envelope.start < calendarBounds.end &&
        envelope.end >= calendarBounds.start
      ) {
        const gap =
          (rule.frequency === 'day'
            ? DAY
            : rule.frequency === 'week'
              ? (7 * DAY) / Math.max(1, rule.weekdays?.length ?? 1)
              : 28 * DAY) * rule.interval;
        const duration = Math.max(
          0,
          (stamp(range.end, range.timezone) ?? envelope.start) - envelope.start,
        );
        const estimated = Math.ceil((Number(view.span / MS) + duration) / gap) + 3;
        const base = parseTime(range.start, range.timezone)!;
        const distantSteps =
          rule.frequency === 'month'
            ? Math.max(
                0,
                DateTime.fromMillis(calendarBounds.start, { zone: range.timezone }).diff(
                  base,
                  'months',
                ).months / rule.interval,
              )
            : Math.max(0, (calendarBounds.start - envelope.start) / gap);
        const boundedSize = Math.min(estimated, rule.count ?? Infinity);
        const canEnumerate =
          boundedSize <= Math.min(MAX_VISIBLE_SERIES_ITEMS, budget) &&
          (rule.calendarPolicy !== 'skip-invalid' || distantSteps <= 4_000);
        let occurrences: ReturnType<typeof expandRecurrence> | null = null;
        if (canEnumerate) {
          try {
            occurrences = expandRecurrence(
              entity,
              DateTime.fromMillis(calendarBounds.start, { zone: range.timezone }).toISO()!,
              DateTime.fromMillis(calendarBounds.end, { zone: range.timezone }).toISO()!,
            );
          } catch {
            occurrences = null;
          }
        }
        if (occurrences && occurrences.length < MAX_RECURRENCE_OCCURRENCES) {
          budget -= occurrences.length;
          if (!occurrences.length && !facts) {
            const at = clampNs(fromMillis(envelope.start), currentBounds.start, currentBounds.end);
            items.push(
              clusterItem(`${entity.id}:empty-series`, [original], at, at, currentBounds, width, {
                count: 0,
                layers: [],
                summary: 'Нет вхождений в этом окне · открыть серию',
                verified: true,
              }),
            );
            seen.add(entity.id);
          }
          const compact =
            occurrences.length > 36 ||
            (occurrences.length > 1 && (gap / Number(view.span / MS)) * width < 80);
          const reveal =
            compact && entity.id === selectedId
              ? activeOccurrence
                ? occurrences.filter((occurrence) => occurrence.index === activeOccurrence.index)
                : occurrences.slice(0, 1)
              : compact
                ? []
                : occurrences;
          for (const occurrence of reveal) {
            const ranges: RangeLayer[] = [
              { layer: 'plan', range: { ...range, start: occurrence.start, end: occurrence.end } },
            ];
            if (occurrence.index === 0) {
              ranges.push(...other);
              facts = null;
            }
            const item = eventItem(
              entity,
              ranges,
              currentBounds,
              width,
              occurrence.id,
              occurrence.index === 0,
              occurrence.index,
            );
            if (item) {
              items.push(item);
              seen.add(entity.id);
            }
          }
          const revealedIds = new Set(reveal.map((occurrence) => occurrence.id));
          const clustered = compact
            ? occurrences.filter((occurrence) => !revealedIds.has(occurrence.id))
            : [];
          if (clustered.length) {
            const first = clustered[0]!,
              last = clustered.at(-1)!;
            items.push(
              clusterItem(
                `${entity.id}:repeats`,
                [original],
                isoToNs(first.start, range.timezone)!,
                isoToNs(last.end ?? last.start, range.timezone)!,
                currentBounds,
                width,
                {
                  count: clustered.length,
                  layers: ['plan'],
                  summary: `${clustered.length} вхождений в окне`,
                  verified: true,
                },
              ),
            );
            seen.add(entity.id);
          }
        } else {
          const from = maxNs(fromMillis(envelope.start), currentBounds.start),
            to = Number.isFinite(envelope.end)
              ? minNs(fromMillis(envelope.end), currentBounds.end)
              : currentBounds.end;
          items.push(
            clusterItem(
              `${entity.id}:series`,
              [original],
              from,
              maxNs(from, to),
              currentBounds,
              width,
              {
                count: occurrences?.length ?? null,
                lowerBound: !!occurrences?.length,
                ruleCount: rule.count,
                exceptions: rule.exceptions.length,
                layers: ['plan'],
                summary: rule.count
                  ? `${rule.count} повторов в правиле${rule.exceptions.length ? ` · ${rule.exceptions.length} исключений указано` : ''}`
                  : `Повтор ${rule.frequency === 'day' ? 'по дням' : rule.frequency === 'week' ? 'по неделям' : 'по месяцам'}${rule.until ? ` · до ${dateLabel(rule.until, range.timezone)}` : ' · конец серии не задан'}`,
                verified: false,
              },
            ),
          );
          seen.add(entity.id);
          if (entity.id === selectedId) {
            const baseItem = eventItem(
              entity,
              [{ layer: 'plan', range: range }, ...other],
              currentBounds,
              width,
              `${entity.id}:selected`,
              true,
            );
            if (baseItem) {
              items.push(baseItem);
              facts = null;
            }
          }
        }
      }
      if (facts) {
        items.push(facts);
        seen.add(entity.id);
      }
    }
    // At distant scales, colliding short objects share a density marker; selected objects stay distinct.
    const compressible = items
      .filter(
        (item): item is EventItem =>
          item.kind === 'event' &&
          item.entity.id !== selectedId &&
          (Number(item.atEnd - item.atStart) / Number(view.span)) * width < 12,
      )
      .sort((a, b) => (a.atStart < b.atStart ? -1 : a.atStart > b.atStart ? 1 : 0));
    const compressed = new Set<string>(),
      clusters: ClusterItem[] = [];
    for (let i = 0; i < compressible.length;) {
      const members = [compressible[i++]!];
      while (
        i < compressible.length &&
        (Number(compressible[i]!.atStart - members[0]!.atStart) / Number(view.span)) * width < 32
      )
        members.push(compressible[i++]!);
      if (members.length < 2) continue;
      const byId = new Map(members.map((item) => [item.entity.id, item.entity]));
      members.forEach((item) => compressed.add(item.key));
      clusters.push(
        clusterItem(
          `density:${members.map((item) => item.key).join('|')}`,
          [...byId.values()],
          minNs(...members.map((item) => item.atStart)),
          maxNs(...members.map((item) => item.atEnd)),
          currentBounds,
          width,
          {
            count: members.length,
            layers: [
              ...new Set(members.flatMap((item) => item.shapes.map((shape) => shape.layer))),
            ],
            summary: `${byId.size} ${byId.size === 1 ? 'объект' : 'объектов'} · ${members.length} вхождений`,
            verified: true,
          },
        ),
      );
    }
    const packed = packTimeline(
      [...items.filter((item) => !compressed.has(item.key)), ...clusters],
      12,
      previousLanes.current,
    );
    const earlier: { entity: Entity; at: bigint }[] = [],
      later: { entity: Entity; at: bigint }[] = [];
    for (const entity of entities) {
      if (seen.has(entity.id)) continue;
      const times = [entity.plan, entity.actual, entity.forecast].flatMap((range) =>
        range ? Object.values(canonicalRange(range)).filter((at): at is bigint => at != null) : [],
      );
      const due = isoToNs(entity.dueAt, entity.plan.timezone);
      if (due != null) times.push(due);
      const envelope = recurrenceEnvelope(entity);
      if (envelope?.exactEnd && Number.isFinite(envelope.end)) times.push(fromMillis(envelope.end));
      if (!times.length) continue;
      const min = minNs(...times),
        max = maxNs(...times);
      if (max < currentBounds.start) earlier.push({ entity, at: max });
      else if (min >= currentBounds.end) later.push({ entity, at: min });
    }
    earlier.sort((a, b) => (a.at > b.at ? -1 : a.at < b.at ? 1 : 0));
    later.sort((a, b) => (a.at < b.at ? -1 : a.at > b.at ? 1 : 0));
    return {
      // Packing order changes with clipped label widths. Keep DOM order chronological
      // so React does not move nodes and cancel their vertical transitions each frame.
      items: packed.sort((a, b) =>
        a.atStart < b.atStart ? -1 : a.atStart > b.atStart ? 1 : a.key.localeCompare(b.key),
      ),
      undated,
      earlier,
      later,
      visibleObjects: seen.size,
      layoutMs: performance.now() - started,
      height: Math.max(
        264,
        (Math.max(-1, ...packed.map((item) => item.lane)) + 1) * laneHeight + 30,
      ),
    };
  }, [
    entities,
    view.center,
    view.span,
    width,
    selectedId,
    activeOccurrence?.index,
    proposed,
    edit,
    laneHeight,
  ]);
  const ticks = useMemo(
    () => timelineTicks(view, width, zone),
    [view.center, view.span, width, zone],
  );
  useLayoutEffect(() => {
    previousLanes.current = new Map(layout.items.map((item) => [item.key, item.lane]));
  }, [layout]);
  const percent = (at: bigint) => project(at, view);

  function beginEdit(event: ReactPointerEvent, entity: Entity, edge: EditGesture['edge']) {
    if (
      event.pointerType !== 'mouse' ||
      event.button !== 0 ||
      readOnly ||
      !canEdit(snapshot, entity.workspaceId) ||
      canonicalRange(entity.plan).start == null ||
      entity.status === 'done' ||
      (entity.actual && canonicalRange(entity.actual).end != null) ||
      (entity.actual && canonicalRange(entity.actual).start != null && edge !== 'end')
    )
      return false;
    event.stopPropagation();
    event.preventDefault();
    stopZoom();
    editing.current = {
      pointerId: event.pointerId,
      entity,
      range: structuredClone(entity.plan),
      edge,
      x: event.clientX,
      width,
      span: viewRef.current.span,
      delta: 0n,
    };
    plot.current?.setPointerCapture(event.pointerId);
    return true;
  }
  function pointerDown(event: ReactPointerEvent<HTMLDivElement>) {
    if (event.pointerType === 'mouse' && event.button !== 0) return;
    stopZoom();
    if (!pointers.current.size) suppressClickUntil.current = 0;
    pointers.current.set(event.pointerId, { x: event.clientX, y: event.clientY });
    const rect = event.currentTarget.getBoundingClientRect();
    if (pointers.current.size === 2 && !editing.current) {
      const [a, b] = [...pointers.current.values()];
      pinch.current = {
        distance: Math.max(1, Math.hypot(a.x - b.x, a.y - b.y)),
        midpoint: (a.x + b.x) / 2,
        fraction: clamp(((a.x + b.x) / 2 - rect.left) / rect.width, 0, 1),
        width: rect.width,
        viewport: viewRef.current,
      };
      pan.current = null;
      suppressClickUntil.current = performance.now() + 500;
      setMoving(true);
      for (const id of pointers.current.keys()) event.currentTarget.setPointerCapture(id);
    } else if (!editing.current && pointers.current.size === 1)
      pan.current = {
        pointerId: event.pointerId,
        x: event.clientX,
        y: event.clientY,
        width: rect.width,
        viewport: viewRef.current,
        touch: event.pointerType !== 'mouse',
        locked: 'pending',
      };
  }
  function pointerMove(event: ReactPointerEvent<HTMLDivElement>) {
    const currentEdit = editing.current;
    if (currentEdit?.pointerId === event.pointerId) {
      const quantum = currentEdit.range.precise?.resolutionNs
        ? BigInt(currentEdit.range.precise.resolutionNs)
        : currentEdit.range.precision === 'day'
          ? DAY_NS
          : currentEdit.range.precision === 'month'
            ? 30n * DAY_NS
            : maxNs(1n, currentEdit.span / BigInt(Math.max(1, Math.round(currentEdit.width))));
      const pixels = BigInt(Math.round((event.clientX - currentEdit.x) * 1_000_000));
      const delta = (pixels * currentEdit.span) / BigInt(Math.round(currentEdit.width * 1_000_000));
      currentEdit.delta = (delta / quantum) * quantum;
      setEdit({
        entityId: currentEdit.entity.id,
        delta: currentEdit.delta,
        edge: currentEdit.edge,
      });
      if (Math.abs(event.clientX - currentEdit.x) > 3)
        suppressClickUntil.current = performance.now() + 500;
      return;
    }
    if (!pointers.current.has(event.pointerId)) return;
    pointers.current.set(event.pointerId, { x: event.clientX, y: event.clientY });
    const pinching = pinch.current;
    if (pinching && pointers.current.size >= 2) {
      const [a, b] = [...pointers.current.values()],
        distance = Math.max(1, Math.hypot(a.x - b.x, a.y - b.y)),
        midpoint = (a.x + b.x) / 2;
      navigate(
        panViewport(
          zoomViewport(pinching.viewport, distance / pinching.distance, pinching.fraction),
          -(midpoint - pinching.midpoint),
          pinching.width,
        ),
      );
      suppressClickUntil.current = performance.now() + 500;
      event.preventDefault();
      return;
    }
    const current = pan.current;
    if (!current || current.pointerId !== event.pointerId || current.locked === 'vertical') return;
    const dx = event.clientX - current.x,
      dy = event.clientY - current.y;
    if (current.locked === 'pending') {
      if (Math.max(Math.abs(dx), Math.abs(dy)) < 5) return;
      if (current.touch && Math.abs(dy) > Math.abs(dx) * 1.15) {
        current.locked = 'vertical';
        return;
      }
      current.locked = 'pan';
      event.currentTarget.setPointerCapture(event.pointerId);
      setMoving(true);
    }
    suppressClickUntil.current = performance.now() + 500;
    navigate(panViewport(current.viewport, -dx, current.width));
  }
  function pointerEnd(event: ReactPointerEvent<HTMLDivElement>, cancelled = false) {
    const currentEdit = editing.current;
    if (currentEdit?.pointerId === event.pointerId) {
      editing.current = null;
      setEdit(null);
      if (!cancelled && currentEdit.delta) {
        const next = editedRange(currentEdit.range, currentEdit.delta, currentEdit.edge);
        if (
          canonicalRange(next).start == null ||
          canonicalRange(next).end == null ||
          canonicalRange(next).end! >= canonicalRange(next).start!
        )
          onPropose(currentEdit.entity, next);
      }
    }
    pointers.current.delete(event.pointerId);
    pinch.current = null;
    pan.current = null;
    setMoving(false);
    if (pointers.current.size === 1 && !cancelled) {
      const [id, point] = [...pointers.current.entries()][0];
      pan.current = {
        pointerId: id,
        x: point.x,
        y: point.y,
        width,
        viewport: viewRef.current,
        touch: true,
        locked: 'pending',
      };
    }
    if (event.currentTarget.hasPointerCapture(event.pointerId))
      event.currentTarget.releasePointerCapture(event.pointerId);
  }
  function select(entity: Entity, guardGesture = true, occurrence?: OccurrenceContext) {
    if (!guardGesture || performance.now() >= suppressClickUntil.current) {
      setOccurrenceContext(occurrence ?? null);
      onSelect(entity);
      setDetailRequest((previous) => previous + 1);
    }
  }
  function selectItem(item: EventItem, allowDeselect = true) {
    if (performance.now() < suppressClickUntil.current) return;
    const range = item.shapes.find((shape) => shape.layer === 'plan')?.range;
    const occurrence: OccurrenceContext | undefined =
      item.entity.recurrence && item.occurrenceIndex != null && range
        ? {
            entityId: item.entity.id,
            entityVersion: item.entity.version,
            index: item.occurrenceIndex,
            range: { ...range },
          }
        : undefined;
    const sameOccurrence = occurrence
      ? activeOccurrence?.index === occurrence.index &&
        activeOccurrence.range.start === range?.start &&
        activeOccurrence.range.end === range?.end
      : !activeOccurrence;
    if (allowDeselect && onDeselect && item.entity.id === selectedId && sameOccurrence) {
      setOccurrenceContext(null);
      onDeselect();
      return;
    }
    select(item.entity, true, occurrence);
  }
  function visitOccurrence(occurrence: Occurrence) {
    if (!selected) return;
    const range = { ...selected.plan, start: occurrence.start, end: occurrence.end };
    select(selected, false, {
      entityId: selected.id,
      entityVersion: selected.version,
      index: occurrence.index,
      range,
    });
    navigate({ ...viewRef.current, center: isoToNs(occurrence.start, range.timezone)! });
  }
  function occurrenceActionLabel(value: OccurrenceSearch, direction: 'previous' | 'next') {
    const label = direction === 'previous' ? 'Предыдущее повторение' : 'Следующее повторение';
    if (!value.occurrence || !selected) return label;
    return `${label}: ${occurrenceIntervalLabel(value.occurrence)}`;
  }
  function occurrenceIntervalLabel(occurrence: Occurrence) {
    if (!selected) return '';
    const range = { ...selected.plan, start: occurrence.start, end: occurrence.end };
    return `${preciseLabel(range)} · ${timezoneLabel(range)}`;
  }
  function locate(entity: Entity) {
    const occurrence = activeOccurrence?.entityId === entity.id ? activeOccurrence : null;
    const range =
      occurrence?.range ??
      [entity.plan, entity.actual, entity.forecast].find((value) => value && hasTime(value));
    if (range) navigate(focusRange(range, viewRef.current));
  }
  function revealCluster(item: ClusterItem) {
    if (performance.now() < suppressClickUntil.current) return;
    setOccurrenceContext(null);
    if (item.count === 0) {
      select(item.entities[0]);
      return;
    }
    const uniqueEntities = new Map(item.entities.map((entity) => [entity.id, entity]));
    if (uniqueEntities.size === 1) select(item.entities[0]);
    const insideNow = now >= item.atStart && now <= item.atEnd;
    const center = !item.verified && insideNow ? now : (item.atStart + item.atEnd) / 2n;
    const range = maxNs(MIN_SPAN, item.atEnd - item.atStart);
    const span = item.verified
      ? minNs(viewRef.current.span / 5n, maxNs(MIN_SPAN, (range * 8n) / 5n))
      : minNs(viewRef.current.span / 6n, 31n * DAY_NS);
    navigate({ center, span });
  }

  return (
    <section
      className="uni-surface"
      data-viewport-center={view.center.toString()}
      data-viewport-span={view.span.toString()}
      data-time-unit="nanoseconds"
      data-layout-ms={layout.layoutMs.toFixed(3)}
      data-visible-objects={layout.visibleObjects}
      data-drawn-items={layout.items.length}
      aria-label="Непрерывная временная шкала"
    >
      <div className="uni-meta">
        <span className="sr-only">{layout.visibleObjects} объектов на шкале</span>
        <div className="uni-legend" aria-label="Слои времени">
          <span className="uni-legend-plan">План</span>
          <span className="uni-legend-actual">Факт</span>
          <span className="uni-legend-forecast">Прогноз</span>
        </div>
        <div className="uni-edges">
          {layout.earlier.length > 0 && (
            <button
              onClick={() => navigate({ ...viewRef.current, center: layout.earlier[0].at })}
              aria-label={`Раньше окна: ${layout.earlier.length} объектов. Перейти к ближайшему`}
            >
              <ArrowLeft size={14} />
              {layout.earlier.length}
            </button>
          )}
          {layout.later.length > 0 && (
            <button
              onClick={() => navigate({ ...viewRef.current, center: layout.later[0].at })}
              aria-label={`Позже окна: ${layout.later.length} объектов. Перейти к ближайшему`}
            >
              {layout.later.length}
              <ArrowRight size={14} />
            </button>
          )}
        </div>
      </div>
      <div
        className={`uni-plot ${moving ? 'uni-moving' : ''}`}
        ref={plot}
        onPointerDown={pointerDown}
        onPointerMove={(event) => {
          pointerMove(event);
          if (!pointers.current.size) {
            const rect = event.currentTarget.getBoundingClientRect();
            const x = clamp(event.clientX - rect.left, 0, rect.width);
            setHover({ x, at: timeAt(viewRef.current, x / rect.width) });
          }
        }}
        onPointerLeave={() => setHover(null)}
        onPointerUp={(event) => pointerEnd(event)}
        onPointerCancel={(event) => pointerEnd(event, true)}
        style={{ touchAction: 'pan-y' }}
      >
        <div className="uni-axis">
          {ticks.map((tick) => (
            <div
              key={tick.at.toString()}
              className={`uni-tick ${tick.major ? 'uni-major' : ''}`}
              style={
                {
                  left: `${percent(tick.at)}%`,
                  '--tick-nudge': `${Math.max(0, 65 - project(tick.at, view, width)) - Math.max(0, project(tick.at, view, width) - width + 65)}px`,
                } as CSSProperties
              }
            >
              <span>{tick.label}</span>
              {tick.context && <small>{tick.context}</small>}
            </div>
          ))}
          {percent(now) >= 0 && percent(now) <= 100 && (
            <div className="uni-now" style={{ left: `${percent(now)}%` }}>
              <span>Сейчас</span>
            </div>
          )}
        </div>
        <div
          className="uni-canvas"
          ref={canvas}
          style={{ height: layout.height }}
          onDoubleClick={(event) => {
            if (
              readOnly ||
              (event.target as HTMLElement).closest('button,.uni-item') ||
              performance.now() < suppressClickUntil.current
            )
              return;
            const rect = event.currentTarget.getBoundingClientRect(),
              current = viewportBounds(viewRef.current);
            onCreate(
              timeAt(viewRef.current, clamp((event.clientX - rect.left) / rect.width, 0, 1)),
            );
          }}
        >
          {ticks.map((tick) => (
            <div
              key={tick.at.toString()}
              className={`uni-grid ${tick.major ? 'uni-major' : ''}`}
              style={{ left: `${percent(tick.at)}%` }}
            />
          ))}
          {percent(now) >= 0 && percent(now) <= 100 && (
            <div className="uni-now-line" style={{ left: `${percent(now)}%` }} />
          )}
          {hover && (
            <div className="uni-focus-line" style={{ left: hover.x }}>
              <span style={{ transform: hover.x > width - 180 ? 'translateX(-100%)' : undefined }}>
                {instantLabel(hover.at, zone, view.span < DAY_NS)}
              </span>
            </div>
          )}
          {layout.items.map((item) => {
            if (item.kind === 'cluster') {
              const ids = [...new Set(item.entities.map((entity) => entity.id))],
                first = item.entities[0];
              const density = item.key.startsWith('density:');
              const title =
                ids.length === 1 ? first.title : `${ids.length} объектов рядом во времени`;
              const count = item.count == null ? '…' : `${item.lowerBound ? '≥' : ''}${item.count}`;
              const firstDate = instantLabel(item.atStart, zone);
              const lastDate = instantLabel(item.atEnd, zone);
              return (
                <button
                  key={item.key}
                  className={`uni-item uni-cluster ${density ? 'uni-density' : ''} ${!item.verified ? 'uni-summary' : ''} ${item.count === 0 ? 'uni-empty-series' : ''} ${ids.includes(selectedId ?? '') ? 'uni-selected' : ''}`}
                  style={
                    {
                      left: item.left,
                      width: item.right - item.left,
                      top: item.lane * laneHeight + 16,
                      height: density ? 48 : laneHeight - 8,
                      '--event-color': typeMap.get(first.typeId)?.color ?? '#719584',
                    } as CSSProperties
                  }
                  data-entity-id={ids.length === 1 ? first.id : undefined}
                  data-timeline-key={item.key}
                  data-entity-ids={ids.join(',')}
                  data-layer="cluster"
                  data-cluster-items={
                    item.count == null ? 'unknown' : `${item.lowerBound ? '>=' : ''}${item.count}`
                  }
                  data-cluster-objects={ids.length}
                  data-cluster-verified={item.verified ? 'true' : 'false'}
                  onClick={() => revealCluster(item)}
                  aria-label={`${title}. ${item.summary}. ${firstDate} — ${lastDate}. ${item.verified ? 'Раскрыть вхождения' : 'Сводка правила; приблизить'}`}
                  title={`${title}. ${item.summary}. ${item.verified ? 'Нажмите, чтобы раскрыть вхождения.' : 'Сводка правила; точное число вхождений в этом окне не подсчитано. Нажмите, чтобы приблизить.'}`}
                >
                  <span className="uni-cluster-title">
                    {density ? (
                      <strong>{count}</strong>
                    ) : (
                      <>
                        <Repeat2 size={15} />
                        {first.title}
                        <span className="uni-cluster-badge" title={item.summary}>
                          {count}
                        </span>
                      </>
                    )}
                  </span>
                  <small className="uni-cluster-note">
                    {firstDate} — {lastDate}
                  </small>
                  <span className="uni-cluster-bands">
                    {item.layers
                      .filter((layer) => layer !== 'deadline')
                      .map((layer) => (
                        <i
                          key={layer}
                          className={`uni-cluster-band uni-${layer}`}
                          data-layer={layer}
                        />
                      ))}
                  </span>
                </button>
              );
            }
            const entity = item.entity,
              Icon = ICONS[entity.kind],
              isSelected =
                entity.id === selectedId &&
                (!activeOccurrence || item.occurrenceIndex === activeOccurrence.index),
              workspace = snapshot.workspaces.find(
                (workspace) => workspace.id === entity.workspaceId,
              );
            const color = typeMap.get(entity.typeId)?.color ?? '#719584',
              label =
                entity.kind === 'metric' && entity.fields.value != null
                  ? `${entity.title} · ${entity.fields.value} ${entity.fields.unit ?? ''}`
                  : entity.title;
            const canChange =
              !readOnly &&
              canEdit(snapshot, entity.workspaceId) &&
              entity.status !== 'done' &&
              !(entity.actual && canonicalRange(entity.actual).end != null);
            return (
              <div
                key={item.key}
                className={`uni-item uni-event-item uni-${entity.kind} ${entity.status} ${isSelected ? 'uni-selected' : ''} ${related.has(entity.id) ? 'uni-related' : ''} ${signals.has(entity.id) ? 'uni-attention' : ''}`}
                style={
                  {
                    left: item.left,
                    width: item.right - item.left,
                    top: item.lane * laneHeight + 16,
                    height: laneHeight - 8,
                    '--event-color': color,
                  } as CSSProperties
                }
                data-entity-id={entity.id}
                data-timeline-key={item.key}
                data-occurrence-index={item.occurrenceIndex}
                data-selected-occurrence={isSelected && activeOccurrence ? 'true' : undefined}
              >
                <button
                  className="uni-label"
                  style={{ left: item.labelLeft - item.left, width: item.labelWidth }}
                  onClick={() => selectItem(item)}
                  onPointerDown={(event) => {
                    if (event.shiftKey) beginEdit(event, entity, 'move');
                  }}
                  aria-label={`${label}. ${typeMap.get(entity.typeId)?.label ?? kindLabels[entity.kind]}${workspace ? ', ' + workspace.name : ''}. ${item.shapes.map((shape) => `${LAYER_NAMES[shape.layer]}: ${preciseLabel(shape.range)}`).join('. ')}. ${statusLabels[entity.status]}`}
                  title={`${label} · ${item.shapes.map((shape) => `${LAYER_NAMES[shape.layer]}: ${preciseLabel(shape.range)}`).join(' · ')}${entity.recurrence ? ' · повторяющаяся серия' : ''}`}
                >
                  <span className="uni-item-name">
                    <Icon size={14} />
                    <strong>{label}</strong>
                    {entity.recurrence && <Repeat2 size={12} />}
                    {signals.has(entity.id) && <AlertTriangle size={13} />}
                  </span>
                  <small className="uni-item-context">
                    {typeMap.get(entity.typeId)?.label ?? kindLabels[entity.kind]}
                    {workspace ? ` · ${workspace.name}` : ''}
                    {item.occurrenceIndex != null ? ` · повтор ${item.occurrenceIndex + 1}` : ''}
                  </small>
                </button>
                {item.shapes.map((shape) => (
                  <div key={shape.layer}>
                    {shape.uncertaintyLeft != null && shape.uncertaintyRight != null && (
                      <span
                        className={`uni-uncertainty uni-${shape.layer}`}
                        style={{
                          left: shape.uncertaintyLeft - item.left,
                          width: Math.max(1, shape.uncertaintyRight - shape.uncertaintyLeft),
                          top: LAYER_TOP[shape.layer] - 4,
                        }}
                        title={`Диапазон неопределённости: ${dateLabel(shape.range.earliest, shape.range.timezone)} — ${dateLabel(shape.range.latest, shape.range.timezone)}`}
                      />
                    )}
                    {shape.mainVisible && (
                      <span
                        className={`uni-layer uni-${shape.layer} ${shape.point ? 'uni-point' : 'uni-period'} ${shape.openStart ? 'uni-open-start' : ''} ${shape.openEnd ? 'uni-open-end' : ''} ${['approximate', 'unknown'].includes(shape.range.precision) ? 'uni-uncertain' : ''} ${shape.range.precision === 'day' ? 'uni-day' : ''} ${shape.range.precision === 'month' ? 'uni-month' : ''}`}
                        style={{
                          left: shape.left - item.left - (shape.point ? 4 : 0),
                          width: shape.point ? 12 : Math.max(1, shape.right - shape.left),
                          top: LAYER_TOP[shape.layer],
                        }}
                        data-entity-id={entity.id}
                        data-layer={shape.layer}
                        data-start={shape.range.start ?? ''}
                        data-start-ns={shape.atStart.toString()}
                        data-end={shape.range.end ?? ''}
                        data-end-ns={shape.atEnd.toString()}
                        data-precision={shape.range.precision}
                        onClick={() => selectItem(item)}
                        onPointerDown={(event) => {
                          if (event.shiftKey && shape.layer === 'plan')
                            beginEdit(event, entity, 'move');
                        }}
                        aria-hidden="true"
                        title={`${LAYER_NAMES[shape.layer]} · ${preciseLabel(shape.range)}${shape.openEnd ? ' · окончание не задано' : ''}`}
                      >
                        {shape.layer === 'deadline' ? (
                          <Flag size={13} />
                        ) : shape.point ? (
                          <i />
                        ) : (
                          <span />
                        )}
                      </span>
                    )}
                    {isSelected &&
                      item.base &&
                      shape.layer === 'plan' &&
                      shape.mainVisible &&
                      canChange && (
                        <>
                          {canonicalRange(shape.range).start != null &&
                            (!entity.actual || canonicalRange(entity.actual).start == null) && (
                              <button
                                className="uni-resize uni-resize-start"
                                style={{
                                  left: shape.left - item.left - 6,
                                  top: 38,
                                  height: 28,
                                  bottom: 'auto',
                                }}
                                onPointerDown={(event) => beginEdit(event, entity, 'start')}
                                onClick={() => selectItem(item, false)}
                                aria-label={`Изменить начало плана ${entity.title}; перетаскивание мышью`}
                              />
                            )}
                          {canonicalRange(shape.range).end != null && (
                            <button
                              className="uni-resize uni-resize-end"
                              style={{
                                left: shape.right - item.left - 6,
                                top: 38,
                                height: 28,
                                bottom: 'auto',
                              }}
                              onPointerDown={(event) => beginEdit(event, entity, 'end')}
                              onClick={() => selectItem(item, false)}
                              aria-label={`Изменить окончание плана ${entity.title}; перетаскивание мышью`}
                            />
                          )}
                        </>
                      )}
                  </div>
                ))}
                {edit?.entityId === entity.id && (
                  <span className="uni-edit-date">Предложение · {preciseLabel(entity.plan)}</span>
                )}
              </div>
            );
          })}
          {!layout.items.length && (
            <div className="uni-empty">
              <Clock3 size={22} />
              <p>
                {entities.length
                  ? 'В этом окне нет датированных объектов'
                  : 'Время для вашей истории'}
              </p>
              <small>
                {entities.length
                  ? 'Перемещайте шкалу или добавьте событие в нужный момент.'
                  : 'События, поездки, работа и идеи будут на одной шкале.'}
              </small>
              {!readOnly && (
                <button onClick={() => onCreate(view.center)}>
                  <Plus size={15} />
                  Добавить здесь
                </button>
              )}
            </div>
          )}
        </div>
      </div>
      {layout.undated.length > 0 && (
        <div className="uni-undated" aria-label="Объекты без даты плана">
          <span className="uni-undated-heading">
            <Clock3 size={14} />
            Без даты · {layout.undated.length}
          </span>
          <div className="uni-undated-items">
            {layout.undated.map((entity) => (
              <button
                key={entity.id}
                className={`uni-undated-chip ${entity.id === selectedId ? 'uni-selected' : ''}`}
                onClick={() => select(entity, false)}
                data-entity-id={entity.id}
                style={
                  {
                    '--event-color': typeMap.get(entity.typeId)?.color ?? '#719584',
                  } as CSSProperties
                }
              >
                <FileText size={14} />
                <span>{entity.title}</span>
                {entity.actual && <small>есть факт</small>}
                {entity.forecast && <small>есть прогноз</small>}
              </button>
            ))}
          </div>
        </div>
      )}
      {selected && (
        <div className="uni-selection" data-entity-id={selected.id}>
          <span>
            <strong>{selected.title}</strong>
            <small>
              {activeOccurrence
                ? `Повторение ${activeOccurrence.index + 1} · ${preciseLabel(activeOccurrence.range)} · ${timezoneLabel(activeOccurrence.range)}`
                : preciseLabel(selected.plan)}
            </small>
          </span>
          {seriesNavigation && (
            <div className="uni-occurrence-nav" aria-label="Повторения выбранной серии">
              <button
                onClick={() => visitOccurrence(seriesNavigation.previous.occurrence!)}
                disabled={!seriesNavigation.previous.occurrence}
                aria-label={occurrenceActionLabel(seriesNavigation.previous, 'previous')}
                title={occurrenceActionLabel(seriesNavigation.previous, 'previous')}
              >
                <ArrowLeft size={16} />
              </button>
              <button
                onClick={() => visitOccurrence(seriesNavigation.next.occurrence!)}
                disabled={!seriesNavigation.next.occurrence}
                aria-label={occurrenceActionLabel(seriesNavigation.next, 'next')}
                title={occurrenceActionLabel(seriesNavigation.next, 'next')}
              >
                <ArrowRight size={16} />
              </button>
              <small className="uni-occurrence-status" role="status">
                {seriesNavigation.current.status === 'limited' ||
                seriesNavigation.previous.status === 'limited' ||
                seriesNavigation.next.status === 'limited'
                  ? 'Поиск соседних повторений ограничен; уточните правило серии.'
                  : seriesNavigation.current.status === 'invalid'
                    ? 'Проверьте даты и правило серии.'
                    : !seriesNavigation.current.occurrence
                      ? 'По правилу нет вхождений.'
                      : !activeOccurrence
                        ? `Первое вхождение: ${occurrenceIntervalLabel(seriesNavigation.current.occurrence)}`
                        : `Повторение ${activeOccurrence.index + 1}`}
              </small>
            </div>
          )}
          {[selected.plan, selected.actual, selected.forecast].some(
            (range) => range && hasTime(range),
          ) ? (
            <button onClick={() => locate(selected)}>
              <LocateFixed size={15} />
              {activeOccurrence ? 'Показать повторение' : 'Показать дату'}
            </button>
          ) : null}
        </div>
      )}
      {selectedId && detail && (
        <div className="uni-detail" ref={detailElement}>
          {activeOccurrence && (
            <div
              className="uni-occurrence-context"
              role="status"
              aria-live="polite"
              data-entity-id={activeOccurrence.entityId}
              data-occurrence-index={activeOccurrence.index}
              data-start={activeOccurrence.range.start ?? ''}
              data-end={activeOccurrence.range.end ?? ''}
              data-timezone={activeOccurrence.range.timezone}
              data-precision={activeOccurrence.range.precision}
            >
              <strong>Выбрано повторение {activeOccurrence.index + 1}</strong>
              <span>{preciseLabel(activeOccurrence.range)}</span>
              <span>{timezoneLabel(activeOccurrence.range)}</span>
              <small>Параметры всей серии</small>
            </div>
          )}
          {detail}
        </div>
      )}
      {connections.length > 0 && (
        <div className="uni-connections">
          <span>Связанные объекты</span>
          {connections.map((dep) => {
            const other = snapshot.entities.find(
              (entity) => entity.id === (dep.fromId === selectedId ? dep.toId : dep.fromId),
            );
            return other ? (
              <button key={dep.id} onClick={() => select(other, false)}>
                <ArrowRight size={13} />
                {other.title}
              </button>
            ) : null;
          })}
        </div>
      )}
    </section>
  );
}
