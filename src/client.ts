// StreamchimeClient: the gateway connection plus the REST helpers, per Shared names' SDK row.
import { Gateway, TypedEmitter, type GatewayEventMap, type GatewaySubscribedPayload } from "./gateway.js";
import { TikTokRoom } from "./room.js";
import { lookupRoom, gifts as fetchGifts, type RoomLookup, type GiftsResult } from "./rest.js";

const DEFAULT_GATEWAY_URL = "wss://ws.streamchime.com/v1";
const DEFAULT_API_URL = "https://api.streamchime.com";

export interface StreamchimeClientOptions {
  /** The app's own bearer key, sc_sk_..., from the developer console. */
  apiKey: string;
  gatewayUrl?: string;
  apiUrl?: string;
}

/** The gateway connection for one developer app: identify, subscribe, event delivery, and the
 * REST lookups. Reconnects on any close except 4001 (the key was wrong or revoked) with backoff
 * 1, 2, 4, 8, 16 s capped at 60, resuming from the last seq seen on each subscribed channel. */
export class StreamchimeClient extends TypedEmitter<GatewayEventMap> {
  private readonly gateway: Gateway;
  private readonly apiKey: string;
  private readonly apiUrl: string;

  constructor(options: StreamchimeClientOptions) {
    super();
    if (!options.apiKey) {
      throw new Error("StreamchimeClient requires an apiKey.");
    }
    this.apiKey = options.apiKey;
    this.apiUrl = options.apiUrl ?? DEFAULT_API_URL;
    this.gateway = new Gateway({
      apiKey: options.apiKey,
      gatewayUrl: options.gatewayUrl ?? DEFAULT_GATEWAY_URL,
    });

    this.gateway.on("ready", (payload) => this.emit("ready", payload));
    this.gateway.on("event", (payload) => this.emit("event", payload));
    this.gateway.on("problem", (payload) => this.emit("problem", payload));
    this.gateway.on("resume_gap", (payload) => this.emit("resume_gap", payload));
    this.gateway.on("disconnect", (payload) => this.emit("disconnect", payload));
    this.gateway.on("error", (payload) => this.emit("error", payload));
    // Not part of the documented public contract; see GatewayEventMap["subscribed"]'s own
    // comment in gateway.ts. Forwarded synchronously the same way as every event above so
    // TikTokRoom (which only holds a StreamchimeClient, not the Gateway itself) can listen to it.
    this.gateway.on("subscribed", (payload) => this.emit("subscribed", payload));
  }

  /** Resolves once the first ready frame arrives. Reconnects after that are not awaited here;
   * observe them through the "ready" and "disconnect" events. */
  connect(): Promise<void> {
    return this.gateway.connect();
  }

  /** Subscribes this app to a TikTok handle's room. Resolves with the subscribed frame's fields,
   * or rejects with a StreamchimeProblemError (invalid_handle, room_cap_exceeded, rate_limited). */
  subscribeTikTok(handle: string): Promise<GatewaySubscribedPayload> {
    return this.gateway.subscribeTikTok(handle);
  }

  /** Unsubscribes from a channel by id. Rejects with a StreamchimeProblemError (unknown_channel)
   * if it is not one of this app's current subscriptions. */
  unsubscribe(channelId: string): Promise<void> {
    return this.gateway.unsubscribe(channelId);
  }

  /** Closes the socket and stops reconnecting. */
  close(): void {
    this.gateway.close();
  }

  /** A TikTokRoom bound to this same client and gateway connection; connect() on the room
   * subscribes the handle and starts emitting its mapped domain events. */
  room(handle: string): TikTokRoom {
    return new TikTokRoom(handle, this);
  }

  /** GET /v1/tiktok/rooms/{handle}: whether TikTok knows the handle and whether it is live, best
   * effort, no login needed. Streamchime is not affiliated with TikTok. */
  lookupRoom(handle: string): Promise<RoomLookup> {
    return lookupRoom(this.apiUrl, this.apiKey, handle);
  }

  /** GET /v1/tiktok/gifts: the last fetched TikTok gift catalogue. */
  gifts(): Promise<GiftsResult> {
    return fetchGifts(this.apiUrl, this.apiKey);
  }
}
