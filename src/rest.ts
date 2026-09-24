// The REST helpers: GET /v1/tiktok/rooms/{handle} and GET /v1/tiktok/gifts, per Shared names'
// REST table and streamchime-api's Controllers/V1/V1Dtos.cs, V1TikTokController.cs and
// Controllers/V1/V1Json.cs (read-only). Every response is snake_case; every error is
// application/problem+json shaped {type: "about:blank", title, status, detail}.

export interface RoomLookup {
  handle: string;
  displayName: string | null;
  avatarUrl: string | null;
  live: boolean;
  roomId: number | null;
  title: string | null;
  viewerCount: number | null;
  checkedAt: string;
}

export interface Gift {
  giftId: number;
  name: string;
  diamonds: number;
  imageUrl: string | null;
}

export interface GiftsResult {
  fetchedAt: string;
  gifts: Gift[];
}

/** A /v1/ problem response (application/problem+json). retryAfter is the Retry-After header in
 * seconds when the server sent one (only rooms_unavailable does today), null otherwise. */
export class StreamchimeApiError extends Error {
  readonly status: number;
  readonly title: string;
  readonly detail: string;
  readonly retryAfter: number | null;

  constructor(status: number, title: string, detail: string, retryAfter: number | null = null) {
    super(`${title}: ${detail}`);
    this.name = "StreamchimeApiError";
    this.status = status;
    this.title = title;
    this.detail = detail;
    this.retryAfter = retryAfter;
  }
}

interface ProblemBody {
  title?: string;
  detail?: string;
}

async function getJson(url: string, apiKey: string): Promise<Record<string, unknown>> {
  const response = await fetch(url, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });

  const body = (await response.json().catch(() => null)) as (Record<string, unknown> & ProblemBody) | null;

  if (!response.ok) {
    const retryAfterHeader = response.headers.get("retry-after");
    const retryAfter = retryAfterHeader ? Number(retryAfterHeader) : null;
    throw new StreamchimeApiError(
      response.status,
      body?.title ?? "request_failed",
      body?.detail ?? response.statusText,
      Number.isFinite(retryAfter) ? retryAfter : null,
    );
  }

  return body ?? {};
}

export async function lookupRoom(apiUrl: string, apiKey: string, handle: string): Promise<RoomLookup> {
  const url = `${apiUrl}/v1/tiktok/rooms/${encodeURIComponent(handle)}`;
  const body = await getJson(url, apiKey);
  return {
    handle: body.handle as string,
    displayName: (body.display_name as string | null | undefined) ?? null,
    avatarUrl: (body.avatar_url as string | null | undefined) ?? null,
    live: Boolean(body.live),
    roomId: (body.room_id as number | null | undefined) ?? null,
    title: (body.title as string | null | undefined) ?? null,
    viewerCount: (body.viewer_count as number | null | undefined) ?? null,
    checkedAt: body.checked_at as string,
  };
}

export async function gifts(apiUrl: string, apiKey: string): Promise<GiftsResult> {
  const url = `${apiUrl}/v1/tiktok/gifts`;
  const body = await getJson(url, apiKey);
  const rawGifts = (body.gifts as Array<Record<string, unknown>> | undefined) ?? [];
  return {
    fetchedAt: body.fetched_at as string,
    gifts: rawGifts.map((g) => ({
      giftId: g.gift_id as number,
      name: g.name as string,
      diamonds: g.diamonds as number,
      imageUrl: (g.image_url as string | null | undefined) ?? null,
    })),
  };
}
