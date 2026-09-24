import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import { lookupRoom, gifts, StreamchimeApiError } from "../src/rest.js";

// The REST contract read from streamchime-api's Controllers/V1/V1Dtos.cs, V1TikTokController.cs
// and V1AppController.cs (Task 7b): snake_case bodies, application/problem+json errors shaped
// {type, title, status, detail}. Nothing here is a real token or handle.
const API_URL = "https://api.example.invalid";
const API_KEY = "sc_sk_test_key";

function jsonResponse(status: number, body: unknown, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn();
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("lookupRoom", () => {
  it("sends a bearer token and maps the snake_case response to camelCase", async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse(200, {
        handle: "samplehandle",
        display_name: "Sample Streamer",
        avatar_url: "https://example.invalid/a.png",
        live: true,
        room_id: 123456,
        title: "sample room title",
        viewer_count: 42,
        checked_at: "2026-09-24T00:00:00.000Z",
      }),
    );

    const result = await lookupRoom(API_URL, API_KEY, "samplehandle");

    expect(fetchMock).toHaveBeenCalledWith(
      `${API_URL}/v1/tiktok/rooms/samplehandle`,
      expect.objectContaining({ headers: expect.objectContaining({ Authorization: `Bearer ${API_KEY}` }) }),
    );
    expect(result).toEqual({
      handle: "samplehandle",
      displayName: "Sample Streamer",
      avatarUrl: "https://example.invalid/a.png",
      live: true,
      roomId: 123456,
      title: "sample room title",
      viewerCount: 42,
      checkedAt: "2026-09-24T00:00:00.000Z",
    });
  });

  it("throws StreamchimeApiError with the problem's title and detail on 400", async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse(400, { type: "about:blank", title: "invalid_handle", status: 400, detail: "handle must be 2 to 24 characters." }),
    );

    await expect(lookupRoom(API_URL, API_KEY, "!!")).rejects.toMatchObject({
      status: 400,
      title: "invalid_handle",
      detail: "handle must be 2 to 24 characters.",
    });
  });

  it("throws on 404 not_found and on 503 rooms_unavailable with retryAfter", async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(404, { type: "about:blank", title: "not_found", status: 404, detail: "TikTok knows no such account." }));
    await expect(lookupRoom(API_URL, API_KEY, "nosuchhandle")).rejects.toBeInstanceOf(StreamchimeApiError);

    fetchMock.mockResolvedValueOnce(
      jsonResponse(503, { type: "about:blank", title: "rooms_unavailable", status: 503, detail: "The room service could not answer right now. Try again shortly." }, { "retry-after": "30" }),
    );
    const error = await lookupRoom(API_URL, API_KEY, "samplehandle").catch((e) => e);
    expect(error).toBeInstanceOf(StreamchimeApiError);
    expect(error.status).toBe(503);
    expect(error.title).toBe("rooms_unavailable");
    expect(error.retryAfter).toBe(30);
  });
});

describe("gifts", () => {
  it("maps the gift catalogue's snake_case fields to camelCase", async () => {
    fetchMock.mockResolvedValueOnce(
      jsonResponse(200, {
        fetched_at: "2026-09-24T00:00:00.000Z",
        gifts: [{ gift_id: 5655, name: "Rose", diamonds: 1, image_url: "https://example.invalid/rose.png" }],
      }),
    );

    const result = await gifts(API_URL, API_KEY);
    expect(result).toEqual({
      fetchedAt: "2026-09-24T00:00:00.000Z",
      gifts: [{ giftId: 5655, name: "Rose", diamonds: 1, imageUrl: "https://example.invalid/rose.png" }],
    });
  });

  it("throws StreamchimeApiError on 404 no_catalogue", async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(404, { type: "about:blank", title: "no_catalogue", status: 404, detail: "No gift catalogue has been fetched yet." }));
    await expect(gifts(API_URL, API_KEY)).rejects.toMatchObject({ status: 404, title: "no_catalogue" });
  });
});
