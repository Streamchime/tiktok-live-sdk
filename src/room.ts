// TikTokRoom: a convenience wrapper over StreamchimeClient for a single TikTok handle, per
// Shared names' SDK row. Maps every delivered envelope to one of its own named events by the
// envelope's type (and, for raw, its subtype).
import { TypedEmitter } from "./gateway.js";
import type { GatewaySubscribedPayload } from "./gateway.js";
import { StreamchimeClient, type StreamchimeClientOptions } from "./client.js";
import type { Event as StreamchimeEvent } from "./types.js";

export type TikTokRoomEventName =
  | "chat"
  | "gift"
  | "like"
  | "follow"
  | "share"
  | "join"
  | "subscribe"
  | "viewerCount"
  | "streamStart"
  | "streamEnd"
  | "pk"
  | "raw";

export type TikTokRoomEventMap = Record<TikTokRoomEventName, StreamchimeEvent>;

export type TikTokRoomOptions = StreamchimeClientOptions;

/** Maps one envelope to the TikTokRoom event name it is delivered as. The schema's fourteen
 * EventType values (schema/event.v1.schema.json) do not include "join" or "pk" on their own:
 * TikTok's own room-join and PK-battle moments arrive as type "raw" with a subtype, so this reads
 * subtype for those two. See FLAG in the completion note: the exact subtype spelling TikTok's own
 * room sends for a join or a PK battle is not yet confirmed against a live room (Task 15's harness
 * and the 72 hour hold are what will observe it); "join" and "pk" are matched case-insensitively
 * against the token this build guesses, and anything else typed raw, including an unmatched
 * subtype, still reaches a listener as "raw" rather than being dropped. */
export function mapEventName(envelope: Pick<StreamchimeEvent, "type" | "subtype" | "payload">): TikTokRoomEventName {
  switch (envelope.type) {
    case "message":
      return "chat";
    case "gift":
      return "gift";
    case "like":
      return "like";
    case "follow":
      return "follow";
    case "share":
      return "share";
    case "subscription":
      return "subscribe";
    case "viewer_count":
      return "viewerCount";
    case "stream_status": {
      const live = (envelope.payload as { live?: boolean } | undefined)?.live;
      return live === false ? "streamEnd" : "streamStart";
    }
    case "raw": {
      const subtype = (envelope.subtype ?? "").toLowerCase();
      if (subtype === "join" || subtype === "member") {
        return "join";
      }
      if (subtype === "pk" || subtype === "pk_battle") {
        return "pk";
      }
      return "raw";
    }
    default:
      // tip, cheer, raid, gift_subscription and redemption are not TikTok event types today
      // (schema/event.v1.schema.json's EventType is shared across every platform); falling back
      // to "raw" rather than throwing means a future schema addition still reaches a listener.
      return "raw";
  }
}

/** A single TikTok handle's room: connect() subscribes it and the room starts emitting chat,
 * gift, like, follow, share, join, subscribe, viewerCount, streamStart, streamEnd, pk and raw.
 * Construct with `new TikTokRoom(handle, { apiKey })` for its own client, or get one bound to an
 * existing client with `client.room(handle)`. */
export class TikTokRoom extends TypedEmitter<TikTokRoomEventMap> {
  readonly handle: string;
  channelId: string | null = null;
  status: string | null = null;

  private readonly client: StreamchimeClient;
  private readonly ownsClient: boolean;

  private readonly onEvent = (envelope: StreamchimeEvent): void => {
    if (envelope.channel_id !== this.channelId) {
      return;
    }
    this.emit(mapEventName(envelope), envelope);
  };

  // Sets channelId synchronously off the gateway's own "subscribed" signal rather than off the
  // subscribeTikTok() promise resolving: the real gateway attaches this room to its relay before
  // it even enqueues the subscribed frame (t3-app-rooms worktree, HandleAppSubscribeAsync), so an
  // event for this channel can be handled in the same synchronous batch as, or right behind,
  // "subscribed" itself, a tick before an awaited promise's continuation would run. Matched on
  // handle rather than channel id, since that is the one thing both this room and the subscribed
  // frame already agree on before the channel id is known.
  private readonly onSubscribed = (payload: GatewaySubscribedPayload): void => {
    if (payload.handle !== this.handle) {
      return;
    }
    this.channelId = payload.channelId;
    this.status = payload.status;
  };

  constructor(handle: string, clientOrOptions: StreamchimeClient | TikTokRoomOptions) {
    super();
    this.handle = handle;
    if (clientOrOptions instanceof StreamchimeClient) {
      this.client = clientOrOptions;
      this.ownsClient = false;
    } else {
      this.client = new StreamchimeClient(clientOrOptions);
      this.ownsClient = true;
    }
    this.client.on("event", this.onEvent);
    this.client.on("subscribed", this.onSubscribed);
  }

  /** Connects the underlying client if it is not already connected, subscribes this handle, and
   * resolves with the subscribed frame's fields. onSubscribed above has already set channelId
   * and status by the time this resolves; the assignment here is only for a caller that awaits
   * connect() itself rather than listening for events. */
  async connect(): Promise<GatewaySubscribedPayload> {
    await this.client.connect();
    const result = await this.client.subscribeTikTok(this.handle);
    this.channelId = result.channelId;
    this.status = result.status;
    return result;
  }

  /** Unsubscribes this room without closing the underlying client. */
  async unsubscribe(): Promise<void> {
    if (!this.channelId) {
      return;
    }
    const channelId = this.channelId;
    this.channelId = null;
    await this.client.unsubscribe(channelId);
  }

  /** Stops listening for this room's events. Closes the underlying client too, but only when this
   * TikTokRoom created it itself (the `new TikTokRoom(handle, { apiKey })` form); a room made with
   * `client.room(handle)` leaves the shared client open for the caller to close. */
  close(): void {
    this.client.off("event", this.onEvent);
    this.client.off("subscribed", this.onSubscribed);
    if (this.ownsClient) {
      this.client.close();
    }
  }
}
