// The low level /v1 gateway client: identify, ready, ping, subscribe/unsubscribe and their
// answers, resume_gap and problem, reconnect with backoff. Built against the wire contract read
// from streamchime-api's Gateway/GatewayFrames.cs, Gateway/GatewayClose.cs,
// Gateway/GatewayEndpoint.cs and Gateway/GatewayReady.cs (read-only, main), and from the
// t3-app-rooms worktree's Gateway/GatewayEndpoint.cs and Gateway/RoomResume.cs (Task 6b, under
// review) for the app subscribe/unsubscribe/resume frames. No token, handle, box address or
// captured message text from the real gateway appears here.
import { EventEmitter } from "node:events";
import WebSocket from "ws";
import type { Event as StreamchimeEvent } from "./types.js";

/** A minimal typed wrapper over Node's EventEmitter: one payload type per named event, no
 * variadic argument lists to keep straight. Shared by Gateway, StreamchimeClient and TikTokRoom
 * so each can declare its own event map without repeating this. */
export class TypedEmitter<Events extends object> {
  private readonly emitter = new EventEmitter();

  on<K extends keyof Events & string>(event: K, listener: (payload: Events[K]) => void): this {
    this.emitter.on(event, listener as (...args: unknown[]) => void);
    return this;
  }

  once<K extends keyof Events & string>(event: K, listener: (payload: Events[K]) => void): this {
    this.emitter.once(event, listener as (...args: unknown[]) => void);
    return this;
  }

  off<K extends keyof Events & string>(event: K, listener: (payload: Events[K]) => void): this {
    this.emitter.off(event, listener as (...args: unknown[]) => void);
    return this;
  }

  protected emit<K extends keyof Events & string>(event: K, payload: Events[K]): boolean {
    return this.emitter.emit(event, payload);
  }

  protected removeAllListeners(): void {
    this.emitter.removeAllListeners();
  }
}

export interface GatewayReadyPayload {
  sessionId: string;
  seq: number;
  epoch: string;
  resumed: boolean;
  channels: Array<{ channelId: string; platform: string; handle: string; live: boolean }>;
  /** Null for every principal but an app; present for an app with its per-tier ceilings. */
  caps: { rooms: number; sockets: number } | null;
  /** Every subscription this app already had before this socket connected (empty on a brand new app). */
  rooms: Array<{ channelId: string; handle: string; status: string; seq: number }>;
}

export interface GatewaySubscribedPayload {
  channelId: string;
  handle: string;
  status: string;
  seq: number;
}

export interface GatewayProblemPayload {
  title: string;
  detail: string;
  /** Null when the problem is not about any one channel (for example rate_limited, room_cap_exceeded). */
  channelId: string | null;
}

export interface GatewayResumeGapPayload {
  channelId: string;
  oldestSeq: number;
}

export interface GatewayDisconnectPayload {
  code: number;
  reason: string;
}

export interface GatewayEventMap {
  ready: GatewayReadyPayload;
  event: StreamchimeEvent;
  problem: GatewayProblemPayload;
  resume_gap: GatewayResumeGapPayload;
  disconnect: GatewayDisconnectPayload;
  error: Error;
  /** Not part of the SDK's documented public contract (README lists ready, event, problem,
   * resume_gap, disconnect and error only): an implementation detail TikTokRoom listens to so it
   * learns its channel id in the same synchronous pass that handled the subscribed frame, ahead
   * of any event frame for that channel arriving right behind it. Needed because the real
   * gateway's own AppRoomRelay.AttachAsync (t3-app-rooms worktree, HandleAppSubscribeAsync) runs
   * before the subscribed frame is even enqueued, so a room's first events can be on the wire
   * essentially back to back with its own subscribed confirmation; waiting for the
   * subscribeTikTok() promise to resolve (an awaited microtask) is a tick too late to catch an
   * event frame handled synchronously right after it in the same message batch. */
  subscribed: GatewaySubscribedPayload;
}

/** Thrown when a problem frame answers a pending subscribe or unsubscribe request. */
export class StreamchimeProblemError extends Error {
  readonly title: string;
  readonly detail: string;
  readonly channelId: string | null;

  constructor(payload: GatewayProblemPayload) {
    super(`${payload.title}: ${payload.detail}`);
    this.name = "StreamchimeProblemError";
    this.title = payload.title;
    this.detail = payload.detail;
    this.channelId = payload.channelId;
  }
}

export interface GatewayOptions {
  /** The app's own bearer key, sc_sk_..., sent as the identify frame's token. */
  apiKey: string;
  gatewayUrl: string;
  /** Default 25000 (spec: "a ping every 25 s"). */
  pingIntervalMs?: number;
  /** Default 1000 (spec: "backoff 1, 2, 4, 8, 16 s capped at 60"). */
  backoffBaseMs?: number;
  /** Default 60000. */
  backoffMaxMs?: number;
}

/** The exact backoff formula the spec's sequence describes (1, 2, 4, 8, 16 s, capped at 60):
 * base * 2^attempt, capped at max. A pure function so the sequence and the cap are each testable
 * without waiting out real delays. */
export function nextBackoffDelay(attempt: number, baseMs: number, maxMs: number): number {
  return Math.min(baseMs * 2 ** attempt, maxMs);
}

interface PendingSubscribe {
  kind: "subscribe";
  resolve: (payload: GatewaySubscribedPayload) => void;
  reject: (error: Error) => void;
}

interface PendingUnsubscribe {
  kind: "unsubscribe";
  channelId: string;
  resolve: () => void;
  reject: (error: Error) => void;
}

type PendingOp = PendingSubscribe | PendingUnsubscribe;

const DEFAULT_PING_INTERVAL_MS = 25000;
const DEFAULT_BACKOFF_BASE_MS = 1000;
const DEFAULT_BACKOFF_MAX_MS = 60000;

/** The gateway close code the identify refused with (unauthorized or revoked key). Reconnecting
 * would only fail identically, so this is the one close code that stops reconnection (spec
 * section 7, Gateway/GatewayClose.cs's GatewayClose.Unauthorized). */
const UNAUTHORIZED_CLOSE_CODE = 4001;

/** The problem titles that answer a subscribe request (Shared names, "Client ops (app sessions
 * only)" and the t3-app-rooms worktree's HandleAppSubscribeAsync): each one is sent instead of a
 * subscribed frame, in place of it, so it rejects the same pending promise a subscribed frame
 * would have resolved. fleet_at_capacity is deliberately not here: the worktree code sends it
 * only after subscribed already went out, so by the time it can arrive the pending subscribe was
 * already resolved and fleet_at_capacity is only ever a plain problem event. */
const SUBSCRIBE_REJECTION_TITLES = new Set(["invalid_handle", "room_cap_exceeded", "rate_limited"]);

export class Gateway extends TypedEmitter<GatewayEventMap> {
  private readonly apiKey: string;
  private readonly gatewayUrl: string;
  private readonly pingIntervalMs: number;
  private readonly backoffBaseMs: number;
  private readonly backoffMaxMs: number;

  private ws: WebSocket | null = null;
  private closing = false;
  private connected = false;
  private reconnectAttempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private pendingOp: PendingOp | null = null;
  private opQueue: Promise<void> = Promise.resolve();
  private readonly cursors = new Map<string, number>();
  private connectResolvers: Array<() => void> = [];
  private connectRejecters: Array<(error: Error) => void> = [];

  constructor(options: GatewayOptions) {
    super();
    this.apiKey = options.apiKey;
    this.gatewayUrl = options.gatewayUrl;
    this.pingIntervalMs = options.pingIntervalMs ?? DEFAULT_PING_INTERVAL_MS;
    this.backoffBaseMs = options.backoffBaseMs ?? DEFAULT_BACKOFF_BASE_MS;
    this.backoffMaxMs = options.backoffMaxMs ?? DEFAULT_BACKOFF_MAX_MS;
    // Node's EventEmitter throws on an "error" event with no listener; a gateway that surfaces
    // socket errors as its own "error" event should never crash a caller who only listens for
    // "ready" and "event". A caller that does want them still gets every one through its own
    // .on("error", ...).
    this.on("error", () => {});
  }

  /** Resolves once the first ready frame arrives. Later reconnects are not awaited here; observe
   * them through the "ready" and "disconnect" events instead. */
  connect(): Promise<void> {
    if (this.connected) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve, reject) => {
      this.connectResolvers.push(resolve);
      this.connectRejecters.push(reject);
      this.closing = false;
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
        this.open();
      } else if (!this.ws) {
        this.open();
      }
      // Otherwise a connection attempt is already in flight; the queued resolver above answers it.
    });
  }

  async subscribeTikTok(handle: string): Promise<GatewaySubscribedPayload> {
    return this.enqueueOp(
      () =>
        new Promise<GatewaySubscribedPayload>((resolve, reject) => {
          this.pendingOp = { kind: "subscribe", resolve, reject };
          this.send({ op: "subscribe", platform: "tiktok", handle });
        }),
    );
  }

  async unsubscribe(channelId: string): Promise<void> {
    return this.enqueueOp(
      () =>
        new Promise<void>((resolve, reject) => {
          this.pendingOp = { kind: "unsubscribe", channelId, resolve, reject };
          this.send({ op: "unsubscribe", channel_id: channelId });
        }),
    );
  }

  close(): void {
    this.closing = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.stopPing();
    if (this.ws) {
      this.ws.close(1000);
    }
  }

  /** Subscribe and unsubscribe share one FIFO: the gateway (like the server's own receive loop)
   * only ever has one of either kind outstanding at a time, which is what lets a subscribed,
   * unsubscribed or problem frame with no channel id of its own be matched to the request that is
   * waiting for it, without a request id on the wire. */
  private enqueueOp<T>(run: () => Promise<T>): Promise<T> {
    const result = this.opQueue.then(run, run);
    this.opQueue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private open(): void {
    const ws = new WebSocket(this.gatewayUrl);
    this.ws = ws;
    ws.on("open", () => this.handleOpen());
    ws.on("message", (data) => this.handleMessage(data));
    ws.on("close", (code, reason) => this.handleClose(code, reason.toString()));
    ws.on("error", (error) => this.emit("error", error));
  }

  private handleOpen(): void {
    this.sendIdentify();
    this.startPing();
  }

  private sendIdentify(): void {
    const frame: Record<string, unknown> = { op: "identify", type: "app", token: this.apiKey };
    if (this.cursors.size > 0) {
      frame.cursors = Object.fromEntries(this.cursors);
    }
    this.send(frame);
  }

  private startPing(): void {
    this.stopPing();
    this.pingTimer = setInterval(() => {
      this.send({ op: "ping", t: Date.now() });
    }, this.pingIntervalMs);
    this.pingTimer.unref?.();
  }

  private stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  private send(frame: Record<string, unknown>): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      return;
    }
    this.ws.send(JSON.stringify(frame));
  }

  private handleMessage(data: WebSocket.RawData): void {
    let frame: Record<string, unknown>;
    try {
      frame = JSON.parse(data.toString());
    } catch {
      return; // Malformed frame from a hostile or broken proxy; never throws.
    }

    switch (frame.op) {
      case "ready":
        this.handleReady(frame);
        break;
      case "event":
        this.handleEvent(frame);
        break;
      case "subscribed":
        this.handleSubscribed(frame);
        break;
      case "unsubscribed":
        this.handleUnsubscribed();
        break;
      case "resume_gap":
        this.emit("resume_gap", {
          channelId: frame.channel_id as string,
          oldestSeq: frame.oldest_seq as number,
        });
        break;
      case "problem":
        this.handleProblem(frame);
        break;
      case "error":
        this.emit("error", new Error(`gateway error ${frame.code as string}: ${frame.message as string}`));
        break;
      default:
        // pong, settings and channels are first-party only; any op this build does not know yet
        // is ignored rather than treated as fatal, the same forward tolerance the schema itself
        // asks for ("additive versioning: unknown keys are allowed").
        break;
    }
  }

  private handleReady(frame: Record<string, unknown>): void {
    this.connected = true;
    this.reconnectAttempt = 0;
    const rawChannels = (frame.channels as Array<Record<string, unknown>> | undefined) ?? [];
    const rawCaps = frame.caps as { rooms: number; sockets: number } | null | undefined;
    const rawRooms = (frame.rooms as Array<Record<string, unknown>> | null | undefined) ?? [];

    const payload: GatewayReadyPayload = {
      sessionId: frame.session_id as string,
      seq: frame.seq as number,
      epoch: frame.epoch as string,
      resumed: Boolean(frame.resumed),
      channels: rawChannels.map((c) => ({
        channelId: c.channel_id as string,
        platform: c.platform as string,
        handle: c.handle as string,
        live: Boolean(c.live),
      })),
      caps: rawCaps ? { rooms: rawCaps.rooms, sockets: rawCaps.sockets } : null,
      rooms: rawRooms.map((r) => ({
        channelId: r.channel_id as string,
        handle: r.handle as string,
        status: r.status as string,
        seq: r.seq as number,
      })),
    };

    this.emit("ready", payload);
    this.resolveConnect();
  }

  private resolveConnect(): void {
    const resolvers = this.connectResolvers;
    this.connectResolvers = [];
    this.connectRejecters = [];
    for (const resolve of resolvers) resolve();
  }

  private handleEvent(frame: Record<string, unknown>): void {
    const envelope = frame.event as StreamchimeEvent | undefined;
    if (!envelope || typeof envelope.channel_id !== "string" || typeof envelope.seq !== "number") {
      return;
    }
    this.cursors.set(envelope.channel_id, envelope.seq);
    this.emit("event", envelope);
  }

  private handleSubscribed(frame: Record<string, unknown>): void {
    const payload: GatewaySubscribedPayload = {
      channelId: frame.channel_id as string,
      handle: frame.handle as string,
      status: frame.status as string,
      seq: frame.seq as number,
    };
    // Emitted synchronously, ahead of resolving the pending promise below: a listener on
    // "subscribed" (TikTokRoom) observes it in this same synchronous pass, before control
    // returns to the event loop and before any event frame batched right behind this one is
    // handled. The promise resolution below is still how subscribeTikTok()'s own caller learns
    // the result; both fire from the one subscribed frame.
    this.emit("subscribed", payload);
    if (this.pendingOp?.kind === "subscribe") {
      this.pendingOp.resolve(payload);
      this.pendingOp = null;
    }
  }

  private handleUnsubscribed(): void {
    if (this.pendingOp?.kind === "unsubscribe") {
      this.pendingOp.resolve();
      this.pendingOp = null;
    }
  }

  private handleProblem(frame: Record<string, unknown>): void {
    const payload: GatewayProblemPayload = {
      title: frame.title as string,
      detail: frame.detail as string,
      channelId: (frame.channel_id as string | null | undefined) ?? null,
    };
    this.emit("problem", payload);

    if (!this.pendingOp) {
      return;
    }
    if (this.pendingOp.kind === "subscribe" && SUBSCRIBE_REJECTION_TITLES.has(payload.title)) {
      this.pendingOp.reject(new StreamchimeProblemError(payload));
      this.pendingOp = null;
      return;
    }
    if (this.pendingOp.kind === "unsubscribe" && payload.title === "unknown_channel") {
      this.pendingOp.reject(new StreamchimeProblemError(payload));
      this.pendingOp = null;
    }
  }

  private handleClose(code: number, reason: string): void {
    this.connected = false;
    this.stopPing();
    if (this.ws) {
      this.ws.removeAllListeners();
      this.ws = null;
    }
    if (this.pendingOp) {
      this.pendingOp.reject(new Error(`gateway closed (${code}) before answering`));
      this.pendingOp = null;
    }
    this.emit("disconnect", { code, reason });

    if (this.closing) {
      return;
    }

    if (code === UNAUTHORIZED_CLOSE_CODE) {
      const error = new Error("gateway refused the identify: 4001 unauthorized");
      const rejecters = this.connectRejecters;
      this.connectResolvers = [];
      this.connectRejecters = [];
      for (const reject of rejecters) reject(error);
      return;
    }

    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    const delay = nextBackoffDelay(this.reconnectAttempt, this.backoffBaseMs, this.backoffMaxMs);
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.open();
    }, delay);
    this.reconnectTimer.unref?.();
  }
}
