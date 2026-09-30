/** Coordinates belong to an identity in one search scope, never to its filtered index. */
export function createFieldLayoutState() {
  return { key: '', placements: new Map(), batch: null, bottom: 0 };
}

export function stableFieldLayout(state, entities, metrics) {
  const key = JSON.stringify(metrics);
  if (state.key !== key) { state.key = key; state.placements.clear(); state.batch = null; state.bottom = metrics.padding; }
  return entities.map(entity => {
    let position = state.placements.get(entity.id);
    if (!position) {
      const size = metrics[entity.type];
      if (!state.batch || state.batch.type !== entity.type) state.batch = {
        type: entity.type, top: state.placements.size ? state.bottom + metrics.gap : metrics.padding, count: 0,
      };
      const index = state.batch.count++;
      position = { x: size.left + index % size.columns * size.pitch,
        y: state.batch.top + Math.floor(index / size.columns) * (size.height + metrics.gap),
        width: size.width, height: size.height };
      state.placements.set(entity.id, position); state.bottom = Math.max(state.bottom, position.y + position.height);
    }
    return { ...position, id: entity.id };
  });
}
