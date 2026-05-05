export interface DeviceRecord {
  readonly id: string;
  readonly nick: string;
  readonly publicJwk: JsonWebKey;
  readonly privateKey: CryptoKey;
  readonly createdAt: string;
}

export interface TunnelRecord {
  readonly id: string;
  readonly key: string;
  readonly label: string;
  readonly color?: string;
  readonly counterparty?: boolean;
  readonly archived?: boolean;
  readonly agent?: boolean;
  readonly score?: number;
  readonly lastActionAt?: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly unread: boolean;
}

export interface InvitePayload {
  readonly v: 1;
  readonly kind: "soty.trustlink.invite";
  readonly roomId: string;
  readonly roomKey: string;
  readonly from: {
    readonly deviceId: string;
    readonly nick: string;
    readonly publicJwk: JsonWebKey;
  };
  readonly createdAt: string;
}

export interface SignedInvite {
  readonly payload: InvitePayload;
  readonly signature?: string;
}

export interface JoinInvite {
  readonly roomId: string;
  readonly fromNick: string;
}

export interface JoinAcceptPayload {
  readonly roomKeyCiphertext: string;
  readonly nonce: string;
  readonly hostPublicJwk: JsonWebKey;
  readonly hostNick: string;
}
