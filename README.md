# Streamchime SDK for TikTok LIVE

Streamchime is not affiliated with TikTok, YouTube, Twitch or Kick.

Best effort, no uptime promise.

A Node.js client for Streamchime's developer gateway: connect a TikTok LIVE room, receive chat,
gifts, likes, follows, shares, viewer counts and stream status as one typed event, and look up a
room or the current gift catalogue over REST. Streamchime's cloud is what actually reads TikTok
LIVE and normalizes it into this schema; this package only talks to Streamchime's own gateway and
REST API.

## Install

```
npm install @streamchime/tiktok-live-sdk
```

Node 20 or newer. Get an app key from the Developers console at
[streamchime.com/app/developers](https://app.streamchime.com/developers) (Hobby tier is free, up
to 3 rooms). Creating an app accepts the
[Developer Acceptable Use policy](https://streamchime.com/developers/acceptable-use).

## Quick start

```js
import { StreamchimeClient } from "@streamchime/tiktok-live-sdk";

const client = new StreamchimeClient({
  apiKey: process.env.STREAMCHIME_APP_KEY,
});
const room = client.room("streamerhandle");

room.on("chat", (event) => {
  console.log(`${event.actor?.display_name ?? "viewer"}: ${event.payload.text}`);
});

await room.connect();
```

## StreamchimeClient

```js
const client = new StreamchimeClient({
  apiKey: "sc_sk_...",                                  // required
  gatewayUrl: "wss://ws.streamchime.com/v1",             // default
  apiUrl: "https://api.streamchime.com",                 // default
});
```

- `await client.connect()`: opens the gateway socket, identifies, and resolves once the first
  ready frame arrives. After that, the client reconnects on its own (see Reconnection below).
- `await client.subscribeTikTok(handle)`: subscribes this app to a TikTok handle's room. Resolves
  with `{ channelId, handle, status, seq }`, or rejects with a `StreamchimeProblemError`
  (`invalid_handle`, `room_cap_exceeded` or `rate_limited`).
- `await client.unsubscribe(channelId)`: unsubscribes from a channel. Rejects with a
  `StreamchimeProblemError` (`unknown_channel`) if it is not one of this app's current
  subscriptions.
- `client.room(handle)`: returns a `TikTokRoom` bound to this client (see below).
- `await client.lookupRoom(handle)`: `GET /v1/tiktok/rooms/{handle}`, whether TikTok knows the
  handle and whether it is live right now.
- `await client.gifts()`: `GET /v1/tiktok/gifts`, the last fetched TikTok gift catalogue.
- `client.close()`: closes the socket and stops reconnecting.

`StreamchimeClient` emits `ready`, `event` (the envelope), `problem`, `resume_gap`, `disconnect`
(with the close code) and `error`.

## TikTokRoom

A convenience wrapper for a single TikTok handle. Use `client.room(handle)` to share a client's
connection across several rooms and REST calls, or construct one on its own:

```js
import { TikTokRoom } from "@streamchime/tiktok-live-sdk";

const room = new TikTokRoom("streamerhandle", { apiKey: process.env.STREAMCHIME_APP_KEY });
await room.connect();
```

`await room.connect()` connects the underlying client if needed, subscribes the handle, and
resolves with `{ channelId, handle, status, seq }`. `await room.unsubscribe()` unsubscribes
without closing the client. `room.close()` stops listening, and also closes the client, but only
when the room created its own (the `new TikTokRoom(handle, { apiKey })` form).

Each event a room receives is mapped, by its type and, for a raw event, its subtype, to one of:

| Event | Fires on |
|---|---|
| `chat` | a chat message |
| `gift` | a gift |
| `like` | a batch of likes |
| `follow` | a new follow |
| `share` | a share |
| `join` | a viewer entering the room |
| `subscribe` | a subscription |
| `viewerCount` | a viewer count update |
| `streamStart` | the room going live |
| `streamEnd` | the room ending |
| `pk` | a PK battle moment (three phases; see below) |
| `raw` | anything not mapped to one of the above |

Every listener receives the full event envelope (`event.type`, `event.payload`,
`event.actor`, and so on; see `schema/event.v1.schema.json` and the generated types in
`src/types.ts`).

`pk` fires for three different moments of a PK battle, told apart by `event.subtype`, the phase:
`"pk_start"`, `"pk_score"` (a running score update mid-battle) or `"pk_end"`. There is one `pk`
event, not three, so `room.on("pk", (event) => { ... })` sees every phase; check `event.subtype`
inside the listener for the phase.

## REST helpers

```js
const lookup = await client.lookupRoom("streamerhandle");
// { handle, displayName, avatarUrl, live, roomId, title, viewerCount, checkedAt }

const catalogue = await client.gifts();
// { fetchedAt, gifts: [{ giftId, name, diamonds, imageUrl }] }
```

Both throw `StreamchimeApiError` (`status`, `title`, `detail`, and `retryAfter` in seconds when the
server sent one) on a non-2xx response: `invalid_handle` (400), `not_found` (404),
`rooms_unavailable` (503, retry after `retryAfter` seconds), `no_catalogue` (404, no gift
catalogue fetched yet).

## Reconnection

The client reconnects on any gateway close except `4001` (the key is wrong or revoked) and `1009`
(the identify frame itself was too large for the gateway, a standard WebSocket close the gateway
sends when a client frame is over its size bound): retrying either would only fail the same way,
so both instead surface as an `error` event and reject a `connect()` still waiting on its first
ready. Every other close reconnects with backoff starting at 1 second and doubling up to a 60
second cap: 1, 2, 4, 8, 16, 32, 60 seconds. Every subscribed channel's last delivered position is
sent back on the next identify, so a reconnect resumes from where it left off; if the gap is too
old for the server to replay, a `resume_gap` event names the channel and the oldest position it
can still replay from. The client sends a ping every 25 seconds to keep the connection alive.

## Caps by tier

| Tier | Rooms | Sockets | Subscribes per minute | REST calls per minute |
|---|---|---|---|---|
| Hobby (free) | 3 | 5 | 60 | 120 |
| Pro | 100 | 5 | 60 | 600 |
| Scale | 1,000 | 5 | 120 | 600 |

`client.on("ready", (ready) => ready.caps)` reports the connected app's own rooms and sockets
ceiling; `await client.lookupRoom(...)` and `await client.gifts()` spend the REST budget above.

## License

MIT. See [LICENSE](./LICENSE).
