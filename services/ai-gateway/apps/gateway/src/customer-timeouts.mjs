// Operator settings only; callers cannot override the gateway's waiting bound.
export function customerTimeoutsFromEnv(env) {
  const deadline = env.CONNECTOR_CUSTOMER_DEADLINE_MS;
  const primary = env.CONNECTOR_CUSTOMER_PRIMARY_MS;
  if (deadline === undefined && primary === undefined) return {};
  const parse = (value, fallback) => {
    if (value === undefined) return fallback;
    if (typeof value !== 'string' || !/^[1-9][0-9]{0,9}$/.test(value)) throw new Error('Invalid customer timeout');
    const ms = Number(value);
    if (!Number.isSafeInteger(ms) || ms > 2_147_483_647) throw new Error('Invalid customer timeout');
    return ms;
  };
  const deadlineMs = parse(deadline, 55_000);
  const primaryMs = parse(primary, 24_000);
  // The existing connector reserves 2 seconds before returning to the caller.
  if (deadlineMs <= 2000 || primaryMs >= deadlineMs - 2000) throw new Error('Invalid customer timeout pair');
  return { deadlineMs, primaryMs };
}
