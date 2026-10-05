/** Keep identity positions through page appends. Resize and changed geometry reflow
 * the map; changed descriptions or ordering do not move already visible targets. */
export function stabilizeFieldMap(state, layout, viewportWidth) {
  const key = JSON.stringify([viewportWidth, layout.compact]);
  if (state.key !== key || layout.items.some(item => {
    const prior = state.placements.get(item.id);
    return prior && (prior.width !== item.width || prior.height !== item.height);
  })) {
    state.key = key; state.placements.clear(); state.bottom = 0; state.batch = null;
  }
  const origins = new Map(layout.items.map(item => [item.id, { x: item.x, y: item.y }]));
  const shifts = new Map(), groupShifts = new Map();
  const primary = item => item.kind === 'community' || item.kind === 'person' || item.kind === 'app' && !item.communityId;
  const occupied = [...state.placements].filter(([id]) => !id.startsWith('more:') && !layout.items.some(item => item.id === id && item.kind === 'app' && item.communityId)).map(([, value]) => value);
  const overlap = (a, b) => a.x < b.x + b.width + 12 && a.x + a.width + 12 > b.x && a.y < b.y + b.height + 12 && a.y + a.height + 12 > b.y;
  for (const item of layout.items.filter(item => item.kind === 'community' || item.kind === 'app' && !item.communityId)) {
    const prior = state.placements.get(item.id);
    const position = prior ?? { x: item.x, y: item.y, width: item.width, height: item.height };
    if (!prior && occupied.some(other => overlap(position, other))) position.y = Math.max(...occupied.map(other => other.y + other.height), 0) + 40;
    const shift = { x: position.x - item.x, y: position.y - item.y };
    shifts.set(item.id, shift);
    if (item.kind === 'community') groupShifts.set(item.entity.value.communityId, shift);
    occupied.push(position);
  }
  for (const item of layout.items.filter(item => item.kind === 'person')) {
    const prior = state.placements.get(item.id);
    const connection = layout.connections.find(edge => edge.kind === 'person' && Math.abs(edge.from.x - item.x - item.width / 2) < .01 && Math.abs(edge.from.y - item.y - 31) < .01);
    const groupShift = connection ? groupShifts.get(connection.communityId) : null;
    const position = prior ?? { x: item.x + (groupShift?.x ?? 0), y: item.y + (groupShift?.y ?? 0), width: item.width, height: item.height };
    if (!prior && !connection && occupied.some(other => overlap(position, other))) position.y = Math.max(...occupied.map(other => other.y + other.height), 0) + 28;
    shifts.set(item.id, { x: position.x - item.x, y: position.y - item.y }); occupied.push(position);
  }
  for (const item of layout.items) {
    const shift = shifts.get(item.id) ?? groupShifts.get(item.communityId ?? item.entity?.value.communityId) ?? { x: 0, y: 0 };
    const prior = primary(item) ? state.placements.get(item.id) : null;
    item.x = prior?.x ?? item.x + shift.x; item.y = prior?.y ?? item.y + shift.y;
    state.placements.set(item.id, { x: item.x, y: item.y, width: item.width, height: item.height });
  }
  for (const edge of layout.connections) {
    const group = groupShifts.get(edge.communityId) ?? { x: 0, y: 0 };
    let from = group;
    if (edge.kind === 'person') {
      const person = layout.items.find(item => item.kind === 'person' && Math.abs((origins.get(item.id)?.x ?? NaN) + item.width / 2 - edge.from.x) < .01 && Math.abs((origins.get(item.id)?.y ?? NaN) + 31 - edge.from.y) < .01);
      from = shifts.get(person?.id) ?? { x: 0, y: 0 };
    }
    edge.from = { x: edge.from.x + from.x, y: edge.from.y + from.y };
    edge.to = { x: edge.to.x + group.x, y: edge.to.y + group.y };
  }
  state.bottom = Math.max(0, ...layout.items.filter(primary).map(item => item.y + item.height));
  layout.height = Math.max(layout.height, state.bottom + 90);
  layout.width = Math.max(layout.width, ...layout.items.map(item => item.x + item.width + 12));
  return layout;
}
