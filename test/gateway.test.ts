import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { WebSocketServer, type WebSocket as WsSocket } from "ws";
import { StreamchimeClient } from "../src/client.js";
import { Gateway, StreamchimeProblemError, nextBackoffDelay } from "../src/gateway.js";

// A fake gateway server standing in for wss://ws.streamchime.com/v1, per the wire contract read
// from streamchime-api's Gateway/GatewayFrames.cs, GatewayClose.cs, GatewayEndpoint.cs and
// GatewayReady.cs, and from the t3-app-rooms worktree's GatewayEndpoint.cs and RoomResume.cs
// (Task 6b, app subscribe/unsubscribe/resume). No token, handle or captured message text from the
// real gateway appears anywhere here; every value below is invented for the test.
interface FakeGatewayHandle {
  url: string;
  connections: WsSocket[];
  nextMessage(socket: WsSocket): Promise<any>;
  close(): Promise<void>;
}

function startFakeGateway(
  onConnection: (socket: WsSocket, gateway: FakeGatewayHandle) => void,
): Promise<FakeGatewayHandle> {
  return new Promise((resolve) => {
    const wss = new WebSocketServer({ port: 0 });
    const connections: WsSocket[] = [];
    const handle: FakeGatewayHandle = {
      url: "",
      connections,
      nextMessage(socket: WsSocket) {
        return new Promise((resolveMessage) => {
          socket.once("message", (data) => resolveMessage(JSON.parse(data.toString())));
        });
      },
      close() {
        return new Promise((resolveClose) => wss.close(() => resolveClose()));
      },
    };
    wss.on("listening", () => {
      const address = wss.address();
      if (typeof address === "object" && address) {
        handle.url = `ws://127.0.0.1:${address.port}`;
      }
      resolve(handle);
    });
    wss.on("connection", (socket) => {
      connections.push(socket);
      onConnection(socket, handle);
    });
  });
}

function send(socket: WsSocket, frame: Record<string, unknown>): void {
  socket.send(JSON.stringify(frame));
}

function sampleReady(overrides: Record<string, unknown> = {}) {
  return {
    op: "ready",
    session_id: "sess_test",
    seq: 0,
    epoch: "e1e1e1e1",
    resumed: false,
    gap: null,
    channels: [],
    account: { plan: "free", badge: false },
    settings: null,
    caps: { rooms: 3, sockets: 5 },
    rooms: [],
    ...overrides,
  };
}

function sampleEnvelope(overrides: Record<string, unknown> = {}) {
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
    actor: { platform_user_id: null, username: null, display_name: "sample viewer", avatar_url: null, badges: [], anonymous: false },
    payload: { text: "sample message" },
    platform_ids: {},
    test: false,
    ...overrides,
  };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function once<T = any>(emitter: any, event: string): Promise<T> {
  return new Promise((resolve) => emitter.once(event, resolve));
}

let servers: FakeGatewayHandle[] = [];
let clients: StreamchimeClient[] = [];
let gateways: Gateway[] = [];

afterEach(async () => {
  for (const client of clients) client.close();
  for (const gateway of gateways) gateway.close();
  for (const server of servers) await server.close();
  clients = [];
  gateways = [];
  servers = [];
});

describe("identify and ready", () => {
  it("sends an app identify and resolves connect() on ready", async () => {
    const server = await startFakeGateway(async (socket, gw) => {
      const identify = await gw.nextMessage(socket);
      expect(identify).toEqual({ op: "identify", type: "app", token: "sc_sk_test_key" });
      send(socket, sampleReady());
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);

    const readyPromise = once(client, "ready");
    await client.connect();
    const ready = await readyPromise;
    expect(ready.caps).toEqual({ rooms: 3, sockets: 5 });
    expect(ready.sessionId).toBe("sess_test");
  });
});

describe("subscribe", () => {
  it("resolves subscribeTikTok with the subscribed frame's fields", async () => {
    const server = await startFakeGateway(async (socket, gw) => {
      await gw.nextMessage(socket); // identify
      send(socket, sampleReady());
      const subscribe = await gw.nextMessage(socket);
      expect(subscribe).toEqual({ op: "subscribe", platform: "tiktok", handle: "samplehandle" });
      send(socket, { op: "subscribed", channel_id: "ch_sample", handle: "samplehandle", status: "connecting", seq: 0 });
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);
    await client.connect();

    const result = await client.subscribeTikTok("samplehandle");
    expect(result).toEqual({ channelId: "ch_sample", handle: "samplehandle", status: "connecting", seq: 0 });
  });

  it("rejects the pending subscribe promise on a problem frame and also emits problem", async () => {
    const server = await startFakeGateway(async (socket, gw) => {
      await gw.nextMessage(socket);
      send(socket, sampleReady());
      await gw.nextMessage(socket); // subscribe
      send(socket, { op: "problem", title: "room_cap_exceeded", detail: "This app has reached the room cap for its tier.", channel_id: null });
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);
    await client.connect();

    const problemPromise = once(client, "problem");
    await expect(client.subscribeTikTok("toobig")).rejects.toBeInstanceOf(StreamchimeProblemError);
    const problem = await problemPromise;
    expect(problem).toEqual({ title: "room_cap_exceeded", detail: "This app has reached the room cap for its tier.", channelId: null });
  });

  it("resolves unsubscribe and rejects it on unknown_channel", async () => {
    const server = await startFakeGateway(async (socket, gw) => {
      await gw.nextMessage(socket);
      send(socket, sampleReady());
      const unsubscribe = await gw.nextMessage(socket);
      expect(unsubscribe).toEqual({ op: "unsubscribe", channel_id: "ch_gone" });
      send(socket, { op: "problem", title: "unknown_channel", detail: "This channel is not one of this app's current subscriptions.", channel_id: "ch_gone" });
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);
    await client.connect();

    await expect(client.unsubscribe("ch_gone")).rejects.toBeInstanceOf(StreamchimeProblemError);
  });

  it("serializes two subscribe calls fired without awaiting the first, correlating each to its own answer", async () => {
    const receivedOrder: string[] = [];
    const server = await startFakeGateway(async (socket, gw) => {
      await gw.nextMessage(socket); // identify
      send(socket, sampleReady());

      const first = await gw.nextMessage(socket);
      receivedOrder.push(first.handle);
      expect(first).toEqual({ op: "subscribe", platform: "tiktok", handle: "first" });
      send(socket, { op: "subscribed", channel_id: "ch_first", handle: "first", status: "connecting", seq: 0 });

      // Only sent by the client once the first call above has been answered: this is the
      // property under test, not something this handler forces.
      const second = await gw.nextMessage(socket);
      receivedOrder.push(second.handle);
      expect(second).toEqual({ op: "subscribe", platform: "tiktok", handle: "second" });
      send(socket, { op: "problem", title: "room_cap_exceeded", detail: "This app has reached the room cap for its tier.", channel_id: null });
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);
    await client.connect();

    // Fired back to back; neither is awaited before the next call starts (review
    // task-14-review.md M3).
    const firstCall = client.subscribeTikTok("first");
    const secondCall = client.subscribeTikTok("second");

    await expect(firstCall).resolves.toEqual({ channelId: "ch_first", handle: "first", status: "connecting", seq: 0 });
    await expect(secondCall).rejects.toBeInstanceOf(StreamchimeProblemError);
    expect(receivedOrder).toEqual(["first", "second"]);
  });
});

describe("send while disconnected", () => {
  it("rejects subscribeTikTok and unsubscribe immediately instead of hanging (review task-14-review.md S1)", async () => {
    const gateway = new Gateway({ apiKey: "sc_sk_test_key", gatewayUrl: "ws://127.0.0.1:1" });
    gateways.push(gateway);
    // connect() is never called: the gateway has no live or in-flight socket at all.
    await expect(gateway.subscribeTikTok("samplehandle")).rejects.toThrow(/not connected/i);
    await expect(gateway.unsubscribe("ch_sample")).rejects.toThrow(/not connected/i);
  });
});

describe("close while disconnected", () => {
  it("settles a pending connect() and fires disconnect when closed during the backoff gap (review task-14-review.md S2)", async () => {
    const server = await startFakeGateway(async (socket, gw) => {
      await gw.nextMessage(socket); // identify
      socket.close(1012, "service_restart"); // no ready sent, so connect() is still pending
    });
    servers.push(server);

    const gateway = new Gateway({
      apiKey: "sc_sk_test_key",
      gatewayUrl: server.url,
      backoffBaseMs: 5000,
      backoffMaxMs: 5000,
    });
    gateways.push(gateway);

    const disconnectEvents: Array<{ code: number; reason: string }> = [];
    gateway.on("disconnect", (payload) => disconnectEvents.push(payload));

    const connectPromise = gateway.connect();
    // Let the server's own close and the reconnect scheduling that follows it land first, so
    // close() below runs well inside the backoff gap (no live or in-flight socket).
    await new Promise((resolve) => setTimeout(resolve, 100));

    gateway.close();

    await expect(connectPromise).rejects.toThrow();
    expect(disconnectEvents.some((d) => d.code === 1000 && d.reason === "closed by client")).toBe(true);
    expect(server.connections.length).toBe(1); // no reconnect attempt after close()
  });
});

describe("event delivery and cursor tracking", () => {
  it("emits the envelope on event and sends the tracked cursor on the next identify", async () => {
    // Awaited instead of a fixed sleep past the real default backoff (review task-14-review.md
    // M4): resolves once the second connection's identify has actually been asserted, however
    // long that reconnect really takes, rather than guessing a sleep long enough to outlast it.
    let resolveSecondIdentifyChecked!: () => void;
    const secondIdentifyChecked = new Promise<void>((resolve) => {
      resolveSecondIdentifyChecked = resolve;
    });

    const server = await startFakeGateway(async (socket, gw) => {
      if (gw.connections.length === 1) {
        const identify = await gw.nextMessage(socket);
        expect(identify).toEqual({ op: "identify", type: "app", token: "sc_sk_test_key" });
        send(socket, sampleReady());
        send(socket, { op: "event", seq: 42, event: sampleEnvelope({ seq: 42 }) });
        // Close for something other than 4001 so the client reconnects and resends identify,
        // this time with the cursor the event above should have left behind.
        setTimeout(() => socket.close(1012, "service_restart"), 50);
        return;
      }
      const secondIdentify = await gw.nextMessage(socket);
      expect(secondIdentify).toEqual({ op: "identify", type: "app", token: "sc_sk_test_key", cursors: { ch_sample: 42 } });
      send(socket, sampleReady());
      resolveSecondIdentifyChecked();
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);

    const eventPromise = once(client, "event");
    await client.connect();
    const envelope = await eventPromise;
    expect(envelope.channel_id).toBe("ch_sample");
    expect(envelope.payload).toEqual({ text: "sample message" });

    await secondIdentifyChecked;
    expect(server.connections.length).toBe(2);
  });
});

describe("cursor cleanup (review task-14-review.md B2)", () => {
  it("drops a channel's cursor once it is unsubscribed, so a reconnect does not resend it", async () => {
    let resolveSecondIdentifyChecked!: () => void;
    const secondIdentifyChecked = new Promise<void>((resolve) => {
      resolveSecondIdentifyChecked = resolve;
    });

    const server = await startFakeGateway(async (socket, gw) => {
      if (gw.connections.length === 1) {
        await gw.nextMessage(socket); // identify
        send(socket, sampleReady());
        const subscribe = await gw.nextMessage(socket);
        expect(subscribe).toEqual({ op: "subscribe", platform: "tiktok", handle: "samplehandle" });
        send(socket, { op: "subscribed", channel_id: "ch_sample", handle: "samplehandle", status: "connecting", seq: 0 });
        send(socket, { op: "event", seq: 42, event: sampleEnvelope({ seq: 42 }) });
        const unsubscribe = await gw.nextMessage(socket);
        expect(unsubscribe).toEqual({ op: "unsubscribe", channel_id: "ch_sample" });
        send(socket, { op: "unsubscribed", channel_id: "ch_sample" });
        setTimeout(() => socket.close(1012, "service_restart"), 50);
        return;
      }
      const secondIdentify = await gw.nextMessage(socket);
      // No "cursors" key at all: the only channel this socket ever saw an event for was
      // unsubscribed before the reconnect, so the tracked cursor map is empty again.
      expect(secondIdentify).toEqual({ op: "identify", type: "app", token: "sc_sk_test_key" });
      send(socket, sampleReady());
      resolveSecondIdentifyChecked();
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);
    await client.connect();

    const eventPromise = once(client, "event");
    await client.subscribeTikTok("samplehandle");
    await eventPromise; // the cursor for ch_sample is now tracked

    await client.unsubscribe("ch_sample");

    await secondIdentifyChecked;
  });

  it("drops a channel's cursor on a resume_gap for that channel", async () => {
    let resolveSecondIdentifyChecked!: () => void;
    const secondIdentifyChecked = new Promise<void>((resolve) => {
      resolveSecondIdentifyChecked = resolve;
    });

    const server = await startFakeGateway(async (socket, gw) => {
      if (gw.connections.length === 1) {
        await gw.nextMessage(socket);
        send(socket, sampleReady());
        send(socket, { op: "event", seq: 42, event: sampleEnvelope({ seq: 42 }) });
        send(socket, { op: "resume_gap", channel_id: "ch_sample", oldest_seq: 100 });
        setTimeout(() => socket.close(1012, "service_restart"), 50);
        return;
      }
      const secondIdentify = await gw.nextMessage(socket);
      expect(secondIdentify).toEqual({ op: "identify", type: "app", token: "sc_sk_test_key" });
      send(socket, sampleReady());
      resolveSecondIdentifyChecked();
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);

    const gapPromise = once(client, "resume_gap");
    await client.connect();
    await gapPromise;

    await secondIdentifyChecked;
  });

  it("drops a channel's cursor on an unknown_channel problem for that channel, even with no pending op", async () => {
    let resolveSecondIdentifyChecked!: () => void;
    const secondIdentifyChecked = new Promise<void>((resolve) => {
      resolveSecondIdentifyChecked = resolve;
    });

    const server = await startFakeGateway(async (socket, gw) => {
      if (gw.connections.length === 1) {
        await gw.nextMessage(socket);
        send(socket, sampleReady());
        send(socket, { op: "event", seq: 42, event: sampleEnvelope({ seq: 42 }) });
        // Not an answer to any subscribe/unsubscribe call this client made; matches
        // RoomResume.ReplayAsync (t3-app-rooms worktree) naming a stale cursor on an identify or
        // resume the client itself did not initiate as an unsubscribe.
        send(socket, { op: "problem", title: "unknown_channel", detail: "This channel is not one of this app's current subscriptions.", channel_id: "ch_sample" });
        setTimeout(() => socket.close(1012, "service_restart"), 50);
        return;
      }
      const secondIdentify = await gw.nextMessage(socket);
      expect(secondIdentify).toEqual({ op: "identify", type: "app", token: "sc_sk_test_key" });
      send(socket, sampleReady());
      resolveSecondIdentifyChecked();
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);

    const eventPromise = once(client, "event");
    const problemPromise = once(client, "problem");
    await client.connect();
    await eventPromise;
    await problemPromise;

    await secondIdentifyChecked;
  });
});

describe("resume_gap", () => {
  it("surfaces a resume_gap frame as an event", async () => {
    const server = await startFakeGateway(async (socket, gw) => {
      await gw.nextMessage(socket);
      send(socket, sampleReady());
      send(socket, { op: "resume_gap", channel_id: "ch_sample", oldest_seq: 500 });
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_test_key", gatewayUrl: server.url });
    clients.push(client);

    const gapPromise = once(client, "resume_gap");
    await client.connect();
    const gap = await gapPromise;
    expect(gap).toEqual({ channelId: "ch_sample", oldestSeq: 500 });
  });
});

describe("4001 stops reconnection", () => {
  it("does not reconnect after an unauthorized close and rejects connect()", async () => {
    const server = await startFakeGateway(async (socket, gw) => {
      await gw.nextMessage(socket);
      socket.close(4001, "unauthorized");
    });
    servers.push(server);

    const client = new StreamchimeClient({ apiKey: "sc_sk_bad_key", gatewayUrl: server.url });
    clients.push(client);

    await expect(client.connect()).rejects.toThrow(/4001|unauthorized/i);

    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(server.connections.length).toBe(1);
  });
});

describe("backoff", () => {
  it("doubles from the base delay and caps at the max", () => {
    expect(nextBackoffDelay(0, 1000, 60000)).toBe(1000);
    expect(nextBackoffDelay(1, 1000, 60000)).toBe(2000);
    expect(nextBackoffDelay(2, 1000, 60000)).toBe(4000);
    expect(nextBackoffDelay(3, 1000, 60000)).toBe(8000);
    expect(nextBackoffDelay(4, 1000, 60000)).toBe(16000);
    expect(nextBackoffDelay(10, 1000, 60000)).toBe(60000);
  });

  it("reconnects with growing delay after repeated failures and resets after a successful ready", async () => {
    let attempts = 0;
    const attemptTimestamps: number[] = [];
    const server = await startFakeGateway(async (socket, gw) => {
      attempts += 1;
      attemptTimestamps.push(Date.now());
      await gw.nextMessage(socket); // identify
      if (attempts < 3) {
        socket.close(1012, "service_restart");
        return;
      }
      send(socket, sampleReady());
    });
    servers.push(server);

    const gateway = new Gateway({
      apiKey: "sc_sk_test_key",
      gatewayUrl: server.url,
      backoffBaseMs: 20,
      backoffMaxMs: 200,
      pingIntervalMs: 60000,
    });
    gateways.push(gateway);

    await gateway.connect();
    expect(attempts).toBe(3);
    // Attempt 2 should follow attempt 1 by roughly the base delay (20ms), attempt 3 by roughly
    // double that (40ms); real-clock timing on localhost, so this only checks ordering and rough
    // growth, not exact milliseconds.
    const firstGap = attemptTimestamps[1]! - attemptTimestamps[0]!;
    const secondGap = attemptTimestamps[2]! - attemptTimestamps[1]!;
    expect(firstGap).toBeGreaterThanOrEqual(15);
    expect(secondGap).toBeGreaterThan(firstGap - 5);
  });
});

describe("ping", () => {
  it("sends a ping frame on the configured interval", async () => {
    const server = await startFakeGateway(async (socket, gw) => {
      await gw.nextMessage(socket); // identify
      send(socket, sampleReady());
      const ping = await gw.nextMessage(socket);
      expect(ping.op).toBe("ping");
      expect(typeof ping.t).toBe("number");
    });
    servers.push(server);

    const gateway = new Gateway({ apiKey: "sc_sk_test_key", gatewayUrl: server.url, pingIntervalMs: 30 });
    gateways.push(gateway);
    await gateway.connect();
    await new Promise((resolve) => setTimeout(resolve, 100));
  });
});
