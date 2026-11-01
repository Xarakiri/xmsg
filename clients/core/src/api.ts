// SPDX-License-Identifier: Apache-2.0
// HTTP calls to the auth, users, channels and messages services. IDs are decimal strings, as on the wire.

import { fromBinary } from "@bufbuild/protobuf";
import { EventPageSchema, type EventPage } from "./gen/xmsg/v1/sync_pb.js";
import type { Attachment, ChannelKind, ListedChannel, LoadedReply, NotifyLevel } from "./state.ts";

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
  /** Files (ADR-0024); "/media" behind the proxy by default. */
  media?: string;
  /** Calls (ADR-0025); "/calls" behind the proxy by default. */
  calls?: string;
}

/** Largest upload the media service takes (its default). */
export const MAX_FILE_BYTES = 25 * 1024 * 1024;

/** The slice of `fetch` the Api uses; `@tauri-apps/plugin-http` matches this shape. */
export type FetchLike = (input: string, init?: RequestInit) => Promise<Response>;

let activeFetch: FetchLike = fetch;
/** Overrides the fetch every Api instance uses (the desktop build passes Tauri's native fetch). */
export function setFetch(fetchImpl: FetchLike): void {
  activeFetch = fetchImpl;
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
  /** False until the owner finished first-run setup (ADR-0026). Absent on older servers. */
  setup_completed?: boolean;
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

/**
 * What single sign-on left in the URL fragment when it sent the browser back: the session's
 * tokens, or why it failed. Fragments never reach a server; read once, then cleared.
 */
export function fromSsoFragment(hash: string): { tokens?: Tokens; error?: string } {
  const params = new URLSearchParams(hash.replace(/^#/, ""));
  const error = params.get("sso_error");
  if (error) return { error };
  const access = params.get("access_token");
  const refresh = params.get("refresh_token");
  const userId = params.get("user_id");
  if (!access || !refresh || !userId) return {};
  return {
    tokens: {
      access_token: access,
      refresh_token: refresh,
      user_id: userId,
      session_id: params.get("session_id") ?? "",
      expires_in: Number(params.get("expires_in") ?? "0"),
    },
  };
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
  const response = await activeFetch(url, { ...init, headers });
  const text = await response.text();
  const body = text ? JSON.parse(text) : {};
  if (!response.ok) throw new ApiError(response.status, body.error ?? response.statusText);
  return body as T;
}

const json = (value: unknown) => JSON.stringify(value);

export class Api {
  private readonly endpoints: Endpoints;

  /**
   * `base` is prepended to root-relative endpoint paths (a desktop app's saved server);
   * `fetch` overrides the global one for all Api calls (Tauri's native fetch on desktop).
   */
  constructor(endpoints: Endpoints, opts: { base?: string; fetch?: FetchLike } = {}) {
    const base = opts.base ?? "";
    this.endpoints = base
      ? Object.fromEntries(Object.entries(endpoints).map(([k, v]) => [k, v?.startsWith("/") ? `${base}${v}` : v])) as Endpoints
      : endpoints;
    if (opts.fetch) setFetch(opts.fetch);
  }

  signup(email: string, password: string, name: string, invite?: string): Promise<{ user_id: string }> {
    return call(`${this.endpoints.auth}/v1/signup`, { method: "POST", body: json({ email, password, name, ...(invite ? { invite } : {}) }) });
  }

  /** Who may register: the first account always, or anyone when sign-up is open (ADR-0026). */
  signupPolicy(): Promise<{ first_account: boolean; invite_only: boolean }> {
    return call(`${this.endpoints.auth}/v1/signup-policy`, { method: "GET" });
  }

  login(email: string, password: string, platform = "web"): Promise<Tokens> {
    return call(`${this.endpoints.auth}/v1/login`, { method: "POST", body: json({ email, password, platform }) });
  }

  /** Break-glass sign-in for a workspace admin when single sign-on is down: each code works once. */
  loginWithRecoveryCode(email: string, code: string, platform = "web"): Promise<Tokens> {
    return call(`${this.endpoints.auth}/v1/login/recovery`, { method: "POST", body: json({ email, code, platform }) });
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
    const response = await activeFetch(url, { headers: { authorization: `Bearer ${token}` } });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new ApiError(response.status, (body as { error?: string }).error ?? response.statusText);
    }
    return fromBinary(EventPageSchema, new Uint8Array(await response.arrayBuffer()));
  }

  /** A channel's events before `before` (older pages when scrolling up), oldest first. */
  async eventsBefore(token: string, channelId: string, before: number, limit = 200): Promise<EventPage> {
    const url = `${this.endpoints.messages}/v1/channels/${channelId}/events?before=${before}&limit=${limit}`;
    const response = await activeFetch(url, { headers: { authorization: `Bearer ${token}` } });
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

  /** Identity providers to offer on the sign-in page (empty without single sign-on). */
  async ssoProviders(): Promise<{ id: string; name: string }[]> {
    const reply = await call<{ providers: { id: string; name: string }[] }>(`${this.endpoints.auth}/v1/sso/providers`, { method: "GET" });
    return reply.providers;
  }

  /** Where the browser goes to sign in with a provider (a full-page navigation). */
  ssoStartUrl(providerId: string, returnTo = "/"): string {
    return `${this.endpoints.auth}/v1/sso/${encodeURIComponent(providerId)}/start?return_to=${encodeURIComponent(returnTo)}`;
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

  /** Uploads a file to a channel (members only); send its `fileId` with a message to attach it. */
  async uploadFile(token: string, channelId: string, file: Blob, name: string): Promise<Attachment> {
    const url = `${this.endpoints.media ?? "/media"}/v1/channels/${channelId}/files?name=${encodeURIComponent(name)}`;
    const response = await activeFetch(url, {
      method: "POST",
      headers: { authorization: `Bearer ${token}`, "content-type": file.type || "application/octet-stream" },
      body: file,
    });
    const reply = await response.json().catch(() => ({}));
    if (!response.ok) throw new ApiError(response.status, reply.error ?? response.statusText);
    return {
      fileId: reply.file_id,
      name: reply.name,
      contentType: reply.content_type,
      size: reply.size,
      ...(reply.width != null && { width: reply.width, height: reply.height }),
      hasThumbnail: reply.thumbnail_type != null,
    };
  }

  /**
   * A file's bytes (or its thumbnail) for showing or saving. Fetched with the token, since
   * an <img src> can't carry one: show it through URL.createObjectURL.
   */
  async fileBlob(token: string, fileId: string, thumbnail = false): Promise<Blob> {
    const url = `${this.endpoints.media ?? "/media"}/v1/files/${fileId}${thumbnail ? "/thumbnail" : ""}`;
    const response = await activeFetch(url, { headers: { authorization: `Bearer ${token}` } });
    if (!response.ok) throw new ApiError(response.status, response.statusText);
    return response.blob();
  }

  /**
   * Joins the channel's call, or starts one (ADR-0025): members only. The answer has what
   * LiveKit needs (`url`, `token`) and the call it is.
   */
  async joinCall(token: string, channelId: string): Promise<{ callId: string; huddle: boolean; url: string; token: string }> {
    const reply = await call<{ call_id: string; huddle: boolean; url: string; token: string }>(
      `${this.endpoints.calls ?? "/calls"}/v1/channels/${channelId}/call`,
      { method: "POST", token },
    );
    return { callId: reply.call_id, huddle: reply.huddle, url: reply.url, token: reply.token };
  }

  /** A thread's replies, oldest first (ADR-0005). */
  async thread(token: string, channelId: string, rootMsgSeq: number, after = 0): Promise<{ replies: LoadedReply[]; hasMore: boolean }> {
    type FileJson = { file_id: string; name: string; content_type: string; size: number; width?: number; height?: number; has_thumbnail: boolean };
    const reply = await call<{
      replies: { reply_seq: string; message_id: string; author_id: string; body: string; attachments?: FileJson[] }[];
      has_more: boolean;
    }>(
      `${this.endpoints.messages}/v1/channels/${channelId}/threads/${rootMsgSeq}?after=${after}`,
      { method: "GET", token },
    );
    return {
      replies: reply.replies.map((r) => ({
        replySeq: Number(r.reply_seq),
        messageId: r.message_id,
        authorId: r.author_id,
        body: r.body,
        ...(r.attachments?.length && {
          attachments: r.attachments.map((a) => ({
            fileId: a.file_id,
            name: a.name,
            contentType: a.content_type,
            size: a.size,
            ...(a.width != null && { width: a.width, height: a.height }),
            hasThumbnail: a.has_thumbnail,
          })),
        }),
      })),
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

  /** Owner/admin: rename the workspace, mark first-run setup done, set the sign-up policy (ADR-0026). */
  updateWorkspace(token: string, workspaceId: string, change: { name?: string; setup_completed?: true; signup?: "invite_only" | "open" }): Promise<Workspace> {
    return call(`${this.endpoints.users}/v1/workspaces/${workspaceId}`, { method: "PATCH", token, body: json(change) });
  }

  /** Owner/admin (not an admin resetting an owner): a single-use password-reset code, shown once. */
  passwordResetCode(token: string, workspaceId: string, userId: string): Promise<{ code: string; expires_at: string }> {
    return call(`${this.endpoints.users}/v1/workspaces/${workspaceId}/members/${userId}/password-reset`, { method: "POST", token });
  }

  /** Applying a reset code: one indistinguishable 400 for unknown email, wrong code or expiry. */
  async resetPassword(email: string, code: string, password: string): Promise<void> {
    await call(`${this.endpoints.auth}/v1/password-reset`, { method: "POST", body: json({ email, code, password }) });
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

  /** The workspace's default channels (ADR-0026); any member. */
  async defaultChannels(token: string, workspaceId: string): Promise<{ channelId: string; name: string }[]> {
    const reply = await call<{ channels: { channel_id: string; name: string }[] }>(
      `${this.endpoints.channels}/v1/workspaces/${workspaceId}/default-channels`,
      { method: "GET", token },
    );
    return reply.channels.map((c) => ({ channelId: c.channel_id, name: c.name }));
  }

  /** Owner/admin: make a public channel a default. True when existing members are being added. */
  async addDefaultChannel(token: string, workspaceId: string, channelId: string, addExistingMembers: boolean): Promise<boolean> {
    const response = await activeFetch(`${this.endpoints.channels}/v1/workspaces/${workspaceId}/default-channels`, {
      method: "POST",
      headers: { "content-type": "application/json", authorization: `Bearer ${token}` },
      body: json({ channel_id: channelId, add_existing_members: addExistingMembers }),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new ApiError(response.status, (body as { error?: string }).error ?? response.statusText);
    }
    return response.status === 202;
  }

  /** Owner/admin: a default channel stops being one; members keep it (ADR-0026). */
  async removeDefaultChannel(token: string, workspaceId: string, channelId: string): Promise<void> {
    await call(`${this.endpoints.channels}/v1/workspaces/${workspaceId}/default-channels/${channelId}`, { method: "DELETE", token });
  }

  /** First sign-in: join the workspace's default channels (idempotent, ADR-0026). */
  async joinDefaultChannels(token: string, workspaceId: string): Promise<string[]> {
    const reply = await call<{ joined: string[] }>(`${this.endpoints.channels}/v1/me/default-channels/join`, {
      method: "POST",
      token,
      body: json({ workspace_id: workspaceId }),
    });
    return reply.joined;
  }

  /** Owner: change a member's workspace role, as far as the API allows (403 otherwise). */
  async updateWorkspaceMemberRole(token: string, workspaceId: string, userId: string, role: "admin" | "member"): Promise<void> {
    await call(`${this.endpoints.users}/v1/workspaces/${workspaceId}/members/${userId}`, { method: "PATCH", token, body: json({ role }) });
  }

  /** Owner/admin: remove a member from the workspace (they keep no channels). */
  async removeWorkspaceMember(token: string, workspaceId: string, userId: string): Promise<void> {
    await call(`${this.endpoints.users}/v1/workspaces/${workspaceId}/members/${userId}`, { method: "DELETE", token });
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
