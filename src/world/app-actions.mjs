/** App requests can only target the current frame's captured entry, never an arbitrary URL. */
export function isAppExternalRequest(event, { frameWindow, origin, current, activated }) {
  const data = event.data;
  return current === true && activated === true && !!frameWindow && event.source === frameWindow
    && typeof origin === 'string' && event.origin === origin && data !== null && typeof data === 'object'
    && !Array.isArray(data) && Object.keys(data).sort().join(',') === 'action,schema'
    && data.schema === 'soty.app-action.v1' && data.action === 'open-separately';
}
