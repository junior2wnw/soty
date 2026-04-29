import { cleanTrustLabel, readUtf8, utf8 } from "trustlink-kernel";
import { openBytesWithRoomSecret, sealBytesWithRoomSecret } from "trustlink-kernel/platform/web";
import { TunnelRecord } from "./types";

export function encode(value: string): Uint8Array {
  return utf8(value);
}

export function decode(value: Uint8Array): string {
  return readUtf8(value);
}

export function cleanNick(value: string): string {
  return cleanTrustLabel(value, { fallback: ".", maxLength: 32 });
}

export function encryptForTunnel(
  tunnel: TunnelRecord,
  bytes: Uint8Array
): Promise<{ nonce: string; ciphertext: string }> {
  return sealBytesWithRoomSecret(tunnel.key, bytes);
}

export function decryptFromTunnel(tunnel: TunnelRecord, nonce: string, ciphertext: string): Promise<Uint8Array> {
  return openBytesWithRoomSecret(tunnel.key, nonce, ciphertext);
}
