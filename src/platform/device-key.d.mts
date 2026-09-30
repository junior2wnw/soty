export interface DeviceIdentity { hostDeviceId: string; connectorId: string }
export function deviceKey(target: DeviceIdentity): string;
export function resolveDeviceKey(value: string, devices: readonly DeviceIdentity[]): string;
