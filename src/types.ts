/* eslint-disable */
/**
 * This file was generated from schema/event.v1.schema.json by scripts/types.mjs
 * (json-schema-to-typescript). Do not edit by hand; edit the schema copy and regenerate
 * with `npm run build:types`.
 */

/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "Platform".
 */
export type Platform = "twitch" | "kick" | "youtube" | "tiktok" | "bagibagi";
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "EventType".
 */
export type EventType =
  | "message"
  | "gift"
  | "tip"
  | "cheer"
  | "subscription"
  | "gift_subscription"
  | "follow"
  | "raid"
  | "like"
  | "share"
  | "viewer_count"
  | "stream_status"
  | "redemption"
  | "raw";

/**
 * Streamchime event envelope v1 (MIT). snake_case wire format. Additive versioning: unknown keys are allowed.
 */
export interface Event {
  v: number;
  id: string;
  seq: number;
  platform: Platform;
  channel_id: string;
  type: EventType;
  subtype?: null | string;
  occurred_at: string;
  received_at: string;
  actor?: null | Actor;
  /**
   * One of the payload definitions, selected by type (and subtype).
   */
  payload: {};
  platform_ids: {
    [k: string]: string;
  };
  test: boolean;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "Actor".
 */
export interface Actor {
  platform_user_id?: null | string;
  username?: null | string;
  display_name?: null | string;
  avatar_url?: null | string;
  badges: string[];
  anonymous: boolean;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "Money".
 */
export interface Money {
  amount: number;
  currency?: null | string;
  unit?: null | string;
  usd_estimate?: null | number;
  estimate: boolean;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "GiftPayload".
 */
export interface GiftPayload {
  gift_id: string;
  gift_name: string;
  count: number;
  streak_final: boolean;
  money: Money;
  message?: null | string;
  image_url?: null | string;
  pinned_seconds?: number | null;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "MessagePayload".
 */
export interface MessagePayload {
  text: string;
  reply_to_message_id?: null | string;
  bits?: number | null;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "TipPayload".
 */
export interface TipPayload {
  money: Money;
  message?: null | string;
  tier?: number | null;
  sticker_id?: null | string;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "CheerPayload".
 */
export interface CheerPayload {
  message: string;
  money: Money;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "SubscriptionPayload".
 */
export interface SubscriptionPayload {
  tier: string;
  is_gift: boolean;
  tier_name?: null | string;
  months?: number | null;
  cumulative_months?: number | null;
  message?: null | string;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "GiftSubscriptionPayload".
 */
export interface GiftSubscriptionPayload {
  count: number;
  tier: string;
  tier_name?: null | string;
  recipients?: string[] | null;
  cumulative_total?: number | null;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "FollowPayload".
 */
export interface FollowPayload {
  is_repeat?: boolean | null;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "RaidPayload".
 */
export interface RaidPayload {
  viewer_count: number;
  from: RaidFrom;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "RaidFrom".
 */
export interface RaidFrom {
  platform_user_id: string;
  display_name: string;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "LikePayload".
 */
export interface LikePayload {
  count: number;
  total: number;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "SharePayload".
 */
export interface SharePayload {}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "ViewerCountPayload".
 */
export interface ViewerCountPayload {
  count: number;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "StreamStatusPayload".
 */
export interface StreamStatusPayload {
  live: boolean;
  title?: null | string;
  category?: null | string;
  started_at?: null | string;
  ended_at?: null | string;
}
/**
 * This interface was referenced by `Event`'s JSON-Schema
 * via the `definition` "RedemptionPayload".
 */
export interface RedemptionPayload {
  reward_id: string;
  reward_name: string;
  status: string;
  cost?: number | null;
  input?: null | string;
}
