// SPDX-License-Identifier: Apache-2.0
// HTTP calls to the auth, channels and messages services. IDs are decimal strings, as on the wire.

import { fromBinary } from "@bufbuild/protobuf";
import { EventPageSchema, type EventPage } from "./gen/xmsg/v1/sync_pb.js";
import type { ChannelKind, ListedChannel } from "./state.ts";

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
    const reply = await call<{ channels: { channel_id: string; type: string; name: string; event_seq: string }[] }>(
      `${this.endpoints.channels}/v1/me/channels`,
      { method: "GET", token },
    );
    return reply.channels.map((c) => ({
      id: c.channel_id,
      kind: (["public", "private", "dm", "group_dm", "broadcast"].includes(c.type) ? c.type : "unknown") as ChannelKind,
      name: c.name,
      eventSeq: Number(c.event_seq),
    }));
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

  /** Leave (your own ID) or remove someone (admins). */
  remove(token: string, channelId: string, userId: string): Promise<CommandOutcome> {
    return call(`${this.endpoints.channels}/v1/channels/${channelId}/members/${userId}`, { method: "DELETE", token });
  }
}

export interface TokenStore {
  load(): string | null;
  save(refreshToken: string | null): void;
}

/** Keeps a fresh access token; the refresh token rotates on every refresh (auth service). */
export class Session {
  private access: { token: string; expiresAt: number } | undefined;
  private inflight: Promise<string> | undefined;

  private readonly api: Api;
  private readonly store: TokenStore;
  constructor(api: Api, store: TokenStore) {
    this.api = api;
    this.store = store;
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

  private async refreshNow(): Promise<string> {
    const refresh = this.store.load();
    if (!refresh) throw new ApiError(401, "not logged in");
    try {
      const tokens = await this.api.refresh(refresh);
      this.adopt(tokens);
      return tokens.access_token;
    } catch (e) {
      if (e instanceof ApiError && e.status === 401) this.store.save(null);
      throw e;
    }
  }
}
