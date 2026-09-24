import { stableJson } from "trustlink-kernel";
import { webSha256Base64Url } from "trustlink-kernel/platform/web";
import { cleanNick } from "./codec";
import { deviceKey, idbGet, idbClaim } from "./storage";
import { DeviceRecord } from "./types";

export async function loadDevice(): Promise<DeviceRecord | null> {
  const record = await idbGet<DeviceRecord>(deviceKey);
  if (record === undefined) return null;
  if (!record || typeof record.id !== "string" || !record.id.startsWith("dev_") || !(record.privateKey instanceof CryptoKey) || !record.publicJwk) {
    throw new Error("existing-device-needs-recovery");
  }
  return record;
}

export async function createDevice(nick: string): Promise<DeviceRecord> {
  const existing = await loadDevice();
  if (existing) return existing;
  const keys = await crypto.subtle.generateKey(
    { name: "ECDSA", namedCurve: "P-256" },
    true,
    ["sign", "verify"]
  ) as CryptoKeyPair;
  const publicJwk = await crypto.subtle.exportKey("jwk", keys.publicKey);
  const id = `dev_${(await webSha256Base64Url(stableJson(publicJwk))).slice(0, 32)}`;
  const record: DeviceRecord = {
    id,
    nick: cleanNick(nick),
    publicJwk,
    privateKey: keys.privateKey,
    createdAt: new Date().toISOString()
  };
  return idbClaim(deviceKey, record);
}
