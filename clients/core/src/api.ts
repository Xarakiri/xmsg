// SPDX-License-Identifier: Apache-2.0
// HTTP calls to the auth, users, channels and messages services. IDs are decimal strings, as on the wire.

import { fromBinary } from "@bufbuild/protobuf";
import { EventPageSchema, type EventPage } from "./gen/xmsg/v1/sync_pb.js";
import type { ChannelKind, ListedChannel, LoadedReply, NotifyLevel } from "./state.ts";

export interface Tokens {
  access_token: string;
  refresh_token: string;
  expires_in: number;
  user_id: string;
  session_id: string;
}

export interface Endpoints {
  /** Base URL of the auth service, e.g. "/auth" behind a proxy or "http://localhost:8081". */
  auth: string;
  channels: string;
  /** Base URL of the messages service's HTTP reads. */
  messages: string;
  /** Base URL of the readstate service. */
  readstate: string;
  users: string;
  /** Web Push subscriptions (optional: push may be off). */
  push?: string;
}

/** What anyone sharing a workspace with the user sees. */
export interface UserProfile {
  user_id: string;
  name: string;
  avatar_url: string | null;
}

export interface Workspace {
  workspace_id: string;
  name: string;
  slug: string;
  role: "owner" | "admin" | "member";
}

export interface WorkspaceMember extends UserProfile {
  role: Workspace["role"];
}

/** One search result: a message, or a reply (then `msgSeq` is its reply_seq in the thread of `rootMsgSeq`). */
export interface SearchHit {
  channelId: string;
  msgSeq: number;
  messageId: string;
  authorId: string;
  reply: boolean;
  rootMsgSeq: number;
  body: string;
}

export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export type CommandOutcome = { status: "accepted"; event_seq: string } | { status: "pending"; request_id: string };

async function call<T>(url: string, init: RequestInit & { token?: string; body?: string }): Promise<T> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (init.token) headers.authorization = `Bearer ${init.token}`;
  const response = await fetch(url, { ...init, headers });
  const text = await response.text();
  const body = text ? JSON.parse(text) : {};
  if (!response.ok) throw new ApiError(response.status, body.error ?? response.statusText);
  return body as T;
}

const json = (value: unknown) => JSON.stringify(value);

export class Api {
  private readonly endpoints: Endpoints;
  constructor(endpoints: Endpoints) {
    this.endpoints = endpoints;
  }

  signup(email: string, password: string, name: string): Promise<{ user_id: string }> {
    return call(`${this.endpoints.auth}/v1/signup`, { method: "POST", body: json({ email, password, name }) });
  }

  login(email: string, password: string, platform = "web"): Promise<Tokens> {
    return call(`${this.endpoints.auth}/v1/login`, { method: "POST", body: json({ email, password, platform }) });
  }

  refresh(refreshToken: string): Promise<Tokens> {
    return call(`${this.endpoints.auth}/v1/token/refresh`, { method: "POST", body: json({ refresh_token: refreshToken }) });
  }

  async logout(refreshToken: string): Promise<void> {
    await call(`${this.endpoints.auth}/v1/logout`, { method: "POST", body: json({ refresh_token: refreshToken }) });
  }

  createChannel(
    token: string,
    kind: "public" | "private" | "broadcast",
    name: string,
    memberIds: string[] = [],
  ): Promise<{ channel_id: string; status: string }> {
    return call(`${this.endpoints.channels}/v1/channels`, {
      method: "POST",
      token,
      body: json({ type: kind, name, member_ids: memberIds }),
    });
  }

  openDm(token: string, userIds: string[]): Promise<{ channel_id: string; status: string }> {
    return call(`${this.endpoints.channels}/v1/dms`, { method: "POST", token, body: json({ user_ids: userIds }) });
  }

  /** Join yourself (no userId) or invite someone. */
  join(token: string, channelId: string, userId?: string): Promise<CommandOutcome> {
    return call(`${this.endpoints.channels}/v1/channels/${channelId}/members`, {
      method: "POST",
      token,
      body: json(userId ? { user_id: userId } : {}),
    });
  }

  /** My channels with their heads (sync, cheap step). */
  async myChannels(token: string): Promise<ListedChannel[]> {
    const reply = await call<{ channels: { channel_id: string; type: string; name: string; event_seq: string; msg_seq: string }[] }>(
      `${this.endpoints.channels}/v1/me/channels`,
      { method: "GET", token },
    );
    return reply.channels.map((c) => ({
      id: c.channel_id,
      kind: (["public", "private", "dm", "group_dm", "broadcast"].includes(c.type) ? c.type : "unknown") as ChannelKind,
      name: c.name,
      eventSeq: Number(c.event_seq),
      msgSeq: Number(c.msg_seq),
    }));
  }

  /** My personal channel settings (ADR-0016). */
  async settings(token: string): Promise<{ channelId: string; muted: boolean; notifyLevel: NotifyLevel }[]> {
    const reply = await call<{ settings: { channel_id: string; muted: boolean; notify_level: NotifyLevel }[] }>(
      `${this.endpoints.channels}/v1/me/settings`,
      { method: "GET", token },
    );
    return reply.settings.map((s) => ({ channelId: s.channel_id, muted: s.muted, notifyLevel: s.notify_level }));
  }

  /** Sets them (absolute values; stored by the settings-writer a moment later). */
  async putSettings(token: string, channelId: string, muted: boolean, notifyLevel: NotifyLevel): Promise<void> {
    await call(`${this.endpoints.channels}/v1/channels/${channelId}/settings`, {
      method: "PUT",
      token,
      body: json({ muted, notify_level: notifyLevel }),
    });
  }

  /** My read positions (`readstate`, ADR-0012). */
  async readState(token: string): Promise<{ channelId: string; msgSeq: number }[]> {
    const reply = await call<{ positions: { channel_id: string; last_read_msg_seq: string }[] }>(
      `${this.endpoints.readstate}/v1/me/read-state`,
      { method: "GET", token },
    );
    return reply.positions.map((p) => ({ channelId: p.channel_id, msgSeq: Number(p.last_read_msg_seq) }));
  }

  /** Unread mentions of me (and `@channel`) per channel, after my read positions. */
  async mentions(token: string): Promise<{ channelId: string; msgSeqs: number[] }[]> {
    const reply = await call<{ mentions: { channel_id: string; msg_seqs: string[] }[] }>(
      `${this.endpoints.readstate}/v1/me/mentions`,
      { method: "GET", token },
    );
    return reply.mentions.map((m) => ({ channelId: m.channel_id, msgSeqs: m.msg_seqs.map(Number) }));
  }

  /** Read up to `msgSeq` (applied by the user's readstate owner shortly after). */
  async markRead(token: string, channelId: string, msgSeq: number): Promise<void> {
    await call(`${this.endpoints.readstate}/v1/channels/${channelId}/read`, {
      method: "POST",
      token,
      body: json({ msg_seq: String(msgSeq) }),
    });
  }

  /** A channel's events after `after` (sync, lazy step). 404: not a member (any more). */
  async events(token: string, channelId: string, after: number, limit = 200): Promise<EventPage> {
    const url = `${this.endpoints.messages}/v1/channels/${channelId}/events?after=${after}&limit=${limit}`;
    const response = await fetch(url, { headers: { authorization: `Bearer ${token}` } });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new ApiError(response.status, (body as { error?: string }).error ?? response.statusText);
    }
    return fromBinary(EventPageSchema, new Uint8Array(await response.arrayBuffer()));
  }

  /**
   * Edit your own message. Accepted (202) means sequenced shortly; the change arrives as a
   * `messageEdited` event like everyone else's. Reuse `editId` when retrying the same edit.
   */
  async editMessage(token: string, channelId: string, msgSeq: number, body: string, editId: string): Promise<void> {
    await call(`${this.endpoints.messages}/v1/channels/${channelId}/messages/${msgSeq}`, {
      method: "PATCH",
      token,
      body: json({ body, edit_id: editId }),
    });
  }

  /**
   * Add or remove a reaction (ADR-0015). `counter` must increase per (message, emoji) for
   * this user; a timestamp will do. The result arrives as a `reactionsChanged` event.
   */
  async react(token: string, channelId: string, msgSeq: number, emoji: string, add: boolean, counter = Date.now()): Promise<void> {
    await call(
      `${this.endpoints.messages}/v1/channels/${channelId}/messages/${msgSeq}/reactions/${encodeURIComponent(emoji)}?counter=${counter}`,
      { method: add ? "PUT" : "DELETE", token },
    );
  }

  /** Messages and replies matching `q`, only in my channels (plan §5.12). */
  async search(token: string, q: string, limit = 20): Promise<SearchHit[]> {
    const reply = await call<{ results: { channel_id: string; msg_seq: string; message_id: string; author_id: string; kind: string; root_msg_seq: string; body: string }[] }>(
      `${this.endpoints.messages}/v1/search?q=${encodeURIComponent(q)}&limit=${limit}`,
      { method: "GET", token },
    );
    return reply.results.map((r) => ({
      channelId: r.channel_id,
      msgSeq: Number(r.msg_seq),
      messageId: r.message_id,
      authorId: r.author_id,
      reply: r.kind === "reply",
      rootMsgSeq: Number(r.root_msg_seq),
      body: r.body,
    }));
  }

  /** The server's VAPID public key, for `pushManager.subscribe` (plan §5.11). */
  async pushKey(): Promise<string> {
    const reply = await call<{ key: string }>(`${this.endpoints.push ?? "/push"}/v1/push/vapid-public-key`, { method: "GET" });
    return reply.key;
  }

  /** Registers this browser for notifications (the subscription's JSON as the browser gives it). */
  async subscribePush(token: string, subscription: { endpoint: string; keys: { p256dh: string; auth: string } }): Promise<void> {
    await call(`${this.endpoints.push ?? "/push"}/v1/push/subscriptions`, { method: "POST", token, body: json(subscription) });
  }

  async unsubscribePush(token: string, endpoint: string): Promise<void> {
    await call(`${this.endpoints.push ?? "/push"}/v1/push/subscriptions`, { method: "DELETE", token, body: json({ endpoint }) });
  }

  /** A thread's replies, oldest first (ADR-0005). */
  async thread(token: string, channelId: string, rootMsgSeq: number, after = 0): Promise<{ replies: LoadedReply[]; hasMore: boolean }> {
    const reply = await call<{ replies: { reply_seq: string; message_id: string; author_id: string; body: string }[]; has_more: boolean }>(
      `${this.endpoints.messages}/v1/channels/${channelId}/threads/${rootMsgSeq}?after=${after}`,
      { method: "GET", token },
    );
    return {
      replies: reply.replies.map((r) => ({ replySeq: Number(r.reply_seq), messageId: r.message_id, authorId: r.author_id, body: r.body })),
      hasMore: reply.has_more,
    };
  }

  /** Delete your own message, or anyone's as a channel admin (checked by the sequencer). */
  async deleteMessage(token: string, channelId: string, msgSeq: number): Promise<void> {
    await call(`${this.endpoints.messages}/v1/channels/${channelId}/messages/${msgSeq}`, { method: "DELETE", token });
  }

  me(token: string): Promise<UserProfile & { email: string }> {
    return call(`${this.endpoints.users}/v1/me`, { method: "GET", token });
  }

  /** Absent fields stay; avatar_url null clears the avatar. */
  updateMe(token: string, change: { name?: string; avatar_url?: string | null }): Promise<UserProfile> {
    return call(`${this.endpoints.users}/v1/me`, { method: "PATCH", token, body: json(change) });
  }

  /** Up to 200 profiles; users sharing no workspace with you are simply missing. */
  async users(token: string, ids: string[]): Promise<UserProfile[]> {
    const reply = await call<{ users: UserProfile[] }>(
      `${this.endpoints.users}/v1/users?ids=${ids.map(encodeURIComponent).join(",")}`,
      { method: "GET", token },
    );
    return reply.users;
  }

  async myWorkspaces(token: string): Promise<Workspace[]> {
    return (await call<{ workspaces: Workspace[] }>(`${this.endpoints.users}/v1/me/workspaces`, { method: "GET", token })).workspaces;
  }

  createWorkspace(token: string, name: string, slug: string): Promise<Workspace> {
    return call(`${this.endpoints.users}/v1/workspaces`, { method: "POST", token, body: json({ name, slug }) });
  }

  /** One page of members by user ID; pass `next` back as `after` for the following page. */
  workspaceMembers(token: string, workspaceId: string, after?: string): Promise<{ members: WorkspaceMember[]; next: string | null }> {
    const query = after ? `?after=${encodeURIComponent(after)}` : "";
    return call(`${this.endpoints.users}/v1/workspaces/${workspaceId}/members${query}`, { method: "GET", token });
  }

  createInvite(token: string, workspaceId: string, ttlHours?: number): Promise<{ code: string; workspace_id: string; expires_at: string }> {
    return call(`${this.endpoints.users}/v1/workspaces/${workspaceId}/invites`, {
      method: "POST",
      token,
      body: json(ttlHours ? { ttl_hours: ttlHours } : {}),
    });
  }

  acceptInvite(token: string, code: string): Promise<{ workspace_id: string; status: "joined" | "already_member" }> {
    return call(`${this.endpoints.users}/v1/invites/${encodeURIComponent(code)}/accept`, { method: "POST", token });
  }

  /** Leave (your own ID) or remove someone (admins). */
  remove(token: string, channelId: string, userId: string): Promise<CommandOutcome> {
    return call(`${this.endpoints.channels}/v1/channels/${channelId}/members/${userId}`, { method: "DELETE", token });
  }
}

export interface TokenStore {
  load(): string | null;
  save(refreshToken: string | null): void;
}

/** The slice of the Web Locks API a Session uses (`navigator.locks` in browsers). */
export interface Locks {
  request<T>(name: string, callback: () => Promise<T>): Promise<T>;
}

/**
 * Keeps a fresh access token; the refresh token rotates on every refresh (auth service).
 * Tabs share the stored refresh token, so refreshes take a cross-tab lock: a tab presenting
 * a token another tab has just rotated would look like token theft to the server.
 */
export class Session {
  private access: { token: string; expiresAt: number } | undefined;
  private inflight: Promise<string> | undefined;

  private readonly api: Api;
  private readonly store: TokenStore;
  private readonly locks: Locks | null;
  /** `locks` defaults to `navigator.locks` where there is one; null refreshes unlocked. */
  constructor(api: Api, store: TokenStore, locks: Locks | null = globalThis.navigator?.locks ?? null) {
    this.api = api;
    this.store = store;
    this.locks = locks;
  }

  userId = "";

  /** Starts from a login, or resumes from a stored refresh token. */
  adopt(tokens: Tokens): void {
    this.userId = tokens.user_id;
    this.access = { token: tokens.access_token, expiresAt: Date.now() + tokens.expires_in * 1000 };
    this.store.save(tokens.refresh_token);
  }

  get hasRefreshToken(): boolean {
    return this.store.load() !== null;
  }

  /** A valid access token, refreshing a minute before expiry. Concurrent callers share one refresh. */
  accessToken(): Promise<string> {
    if (this.access && this.access.expiresAt - Date.now() > 60_000) return Promise.resolve(this.access.token);
    this.inflight ??= this.refreshNow().finally(() => (this.inflight = undefined));
    return this.inflight;
  }

  async logout(): Promise<void> {
    const refresh = this.store.load();
    this.store.save(null);
    this.access = undefined;
    if (refresh) await this.api.logout(refresh).catch(() => undefined);
  }

  private refreshNow(): Promise<string> {
    return this.locks ? this.locks.request("xmsg-refresh", () => this.refreshLocked()) : this.refreshLocked();
  }

  private async refreshLocked(): Promise<string> {
    // Read only now, holding the lock: a tab that refreshed while we waited has stored its
    // successor, and refreshing with that continues the chain.
    const refresh = this.store.load();
    if (!refresh) throw new ApiError(401, "not logged in");
    try {
      const tokens = await this.api.refresh(refresh);
      this.adopt(tokens);
      return tokens.access_token;
    } catch (e) {
      // Don't drop a token another tab stored meanwhile (e.g. a fresh login).
      if (e instanceof ApiError && e.status === 401 && this.store.load() === refresh) this.store.save(null);
      throw e;
    }
  }
}
