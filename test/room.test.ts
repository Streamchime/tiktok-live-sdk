import { describe, it, expect, afterEach } from "vitest";
import { WebSocketServer, type WebSocket as WsSocket } from "ws";
import { StreamchimeClient } from "../src/client.js";
import { mapEventName } from "../src/room.js";
import type { Event as StreamchimeEvent } from "../src/types.js";

function baseEnvelope(overrides: Partial<StreamchimeEvent>): StreamchimeEvent {
  return {
    v: 1,
    id: "evt_1",
    seq: 1,
    platform: "tiktok",
    channel_id: "ch_sample",
    type: "message",
    subtype: null,
    occurred_at: "2026-09-24T00:00:00.000Z",
    received_at: "2026-09-24T00:00:00.000Z",
    actor: null,
    payload: {},
    platform_ids: {},
    test: false,
    ...overrides,
  } as StreamchimeEvent;
}

describe("mapEventName", () => {
  it("maps the canonical event types to TikTokRoom's own event names", () => {
    expect(mapEventName(baseEnvelope({ type: "message" }))).toBe("chat");
    expect(mapEventName(baseEnvelope({ type: "gift" }))).toBe("gift");
    expect(mapEventName(baseEnvelope({ type: "like" }))).toBe("like");
    expect(mapEventName(baseEnvelope({ type: "follow" }))).toBe("follow");
    expect(mapEventName(baseEnvelope({ type: "share" }))).toBe("share");
    expect(mapEventName(baseEnvelope({ type: "subscription" }))).toBe("subscribe");
    expect(mapEventName(baseEnvelope({ type: "viewer_count" }))).toBe("viewerCount");
  });

  it("maps stream_status by payload.live to streamStart or streamEnd", () => {
    expect(mapEventName(baseEnvelope({ type: "stream_status", payload: { live: true } }))).toBe("streamStart");
    expect(mapEventName(baseEnvelope({ type: "stream_status", payload: { live: false } }))).toBe("streamEnd");
  });

  it("maps raw join to join, and falls back to raw for an unmapped subtype", () => {
    expect(mapEventName(baseEnvelope({ type: "raw", subtype: "join" }))).toBe("join");
    expect(mapEventName(baseEnvelope({ type: "raw", subtype: "member_enter" }))).toBe("raw");
    expect(mapEventName(baseEnvelope({ type: "raw", subtype: null }))).toBe("raw");
  });

  // Streamchime.Rooms/Mapping/TikTokMapper.cs's MapBattle and MapArmies are the source of these
  // three exact subtypes (streamchime-api main, read-only): pk_start and pk_end from MapBattle,
  // pk_score from MapArmies. All three map to the single "pk" event; event.subtype carries which
  // phase (see README).
  it("maps every PK battle subtype to the single pk event", () => {
    expect(mapEventName(baseEnvelope({ type: "raw", subtype: "pk_start" }))).toBe("pk");
    expect(mapEventName(baseEnvelope({ type: "raw", subtype: "pk_score" }))).toBe("pk");
    expect(mapEventName(baseEnvelope({ type: "raw", subtype: "pk_end" }))).toBe("pk");
  });

  it("falls back to raw for a schema type TikTokRoom names no event for", () => {
    expect(mapEventName(baseEnvelope({ type: "tip" }))).toBe("raw");
    expect(mapEventName(baseEnvelope({ type: "redemption" }))).toBe("raw");
  });
});

// A fake gateway proving TikTokRoom itself: connect() subscribes and an event for its channel
// arrives mapped to the right named event, per the wire contract already exercised in
// gateway.test.ts. No token, handle or captured message text from the real gateway appears here.
function startFakeGateway(onConnection: (socket: WsSocket) => void): Promise<{ url: string; close: () => Promise<void> }> {
  return new Promise((resolve) => {
    const wss = new WebSocketServer({ port: 0 });
    wss.on("listening", () => {
      const address = wss.address();
      const url = typeof address === "object" && address ? `ws://127.0.0.1:${address.port}` : "";
      resolve({ url, close: () => new Promise((r) => wss.close(() => r())) });
    });
    wss.on("connection", onConnection);
  });
}

function nextMessage(socket: WsSocket): Promise<any> {
  return new Promise((resolve) => socket.once("message", (data) => resolve(JSON.parse(data.toString()))));
}

let cleanup: Array<() => Promise<void> | void> = [];
afterEach(async () => {
  for (const fn of cleanup) await fn();
  cleanup = [];
});

describe("TikTokRoom end to end", () => {
  it("connects, subscribes and emits a mapped domain event", async () => {
    const server = await startFakeGateway(async (socket) => {
      await nextMessage(socket); // identify
      socket.send(JSON.stringify({
        op: "ready", session_id: "s1", seq: 0, epoch: "e1", resumed: false, gap: null,
        channels: [], account: { plan: "free", badge: false }, settings: null,
        caps: { rooms: 3, sockets: 5 }, rooms: [],
      }));
      const subscribe = await nextMessage(socket);
      expect(subscribe).toEqual({ op: "subscribe", platform: "tiktok", handle: "samplehandle" });
      socket.send(JSON.stringify({ op: "subscribed", channel_id: "ch_sample", handle: "samplehandle", status: "connecting", seq: 0 }));
      socket.send(JSON.stringify({
        op: "event", seq: 7,
        event: baseEnvelope({ seq: 7, channel_id: "ch_sample", type: "gift", payload: { gift_id: "1", gift_name: "Rose", count: 1, streak_final: true, money: { amount: 1, estimate: true } } }),
      }));
    });
    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    // client.close() first: the fake WebSocketServer's own close() waits for the underlying
    // HTTP server's connections to end, so closing the server before the client that is still
    // connected to it would deadlock this cleanup.
    cleanup.push(() => client.close());
    cleanup.push(server.close);

    const room = client.room("samplehandle");
    const giftPromise = new Promise<StreamchimeEvent>((resolve) => room.once("gift", resolve));

    const subscribed = await room.connect();
    expect(subscribed).toEqual({ channelId: "ch_sample", handle: "samplehandle", status: "connecting", seq: 0 });

    const gift = await giftPromise;
    expect(gift.channel_id).toBe("ch_sample");
    expect((gift.payload as { gift_name: string }).gift_name).toBe("Rose");
  });
});
