export { StreamchimeClient } from "./client.js";
export type { StreamchimeClientOptions } from "./client.js";

export { TikTokRoom, mapEventName } from "./room.js";
export type { TikTokRoomOptions, TikTokRoomEventMap, TikTokRoomEventName } from "./room.js";

export { Gateway, TypedEmitter, StreamchimeProblemError, nextBackoffDelay } from "./gateway.js";
export type {
  GatewayOptions,
  GatewayEventMap,
  GatewayReadyPayload,
  GatewaySubscribedPayload,
  GatewayProblemPayload,
  GatewayResumeGapPayload,
  GatewayDisconnectPayload,
} from "./gateway.js";

export { lookupRoom, gifts, StreamchimeApiError } from "./rest.js";
export type { RoomLookup, Gift, GiftsResult } from "./rest.js";

export * from "./types.js";
