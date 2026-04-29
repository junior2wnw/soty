import { stableJson } from "trustlink-kernel";
import { webSha256Base64Url } from "trustlink-kernel/platform/web";
import { cleanNick } from "./codec";
import { deviceKey, idbGet, idbSet } from "./storage";
import { DeviceRecord } from "./types";

export async function loadDevice(): Promise<DeviceRecord | null> {
  return (await idbGet<DeviceRecord>(deviceKey)) ?? null;
}

export async function createDevice(nick: string): Promise<DeviceRecord> {
  const keys = await crypto.subtle.generateKey(
    { name: "ECDSA", namedCurve: "P-256" },
    false,
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
  await idbSet(deviceKey, record);
  return record;
}
