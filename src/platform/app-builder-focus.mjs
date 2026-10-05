/**
 * Focus belongs to the user. A replacement may transfer it only from its own
 * removed/disabled control; activity elsewhere invalidates an older handoff.
 */
export function createAssistantFocusHandoff({ root, getActiveElement, isNeutral }) {
  let owner = null, revision = 0;
  const unavailable = node => !node?.isConnected || node.disabled === true || node.matches?.(':disabled') === true;
  const observe = element => {
    if (element && root.contains(element)) {
      if (owner !== element) revision++;
      owner = element;
    } else if (!(isNeutral(element) && owner && unavailable(owner))) {
      owner = null; revision++;
    }
  };
  return {
    observe,
    releaseOutside(target) { if (!target || !root.contains(target)) { owner = null; revision++; } },
    capture(area = root) {
      const active = getActiveElement();
      if (active && area.contains(active)) return { anchor: active, area, revision, displaced: false };
      if (isNeutral(active) && owner && unavailable(owner) && (area === root || area.contains(owner))) {
        return { anchor: owner, area, revision, displaced: true };
      }
      return null;
    },
    restore(claim, fallback) {
      if (!claim || claim.revision !== revision || !root.isConnected || !fallback?.isConnected || !root.contains(fallback)) return false;
      const active = getActiveElement();
      if (isNeutral(active)) {
        if (!claim.displaced && !unavailable(claim.anchor)) return false;
      } else if (!active || !claim.area.contains(active)) return false;
      const target = claim.anchor.isConnected && root.contains(claim.anchor) && !unavailable(claim.anchor) ? claim.anchor : fallback;
      if (unavailable(target) || typeof target.focus !== 'function') return false;
      target.focus({ preventScroll: true }); observe(target); return true;
    },
  };
}
