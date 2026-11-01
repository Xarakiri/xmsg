// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { Api, ApiError } from "../src/api.ts";

interface Recorded {
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string;
}

function fakeFetch(handler: (req: Recorded) => { status?: number; json?: unknown; text?: string }) {
  const calls: Recorded[] = [];
  const impl = async (url: string, init?: RequestInit) => {
    const recorded: Recorded = {
      url,
      method: init?.method ?? "GET",
      headers: (init?.headers ?? {}) as Record<string, string>,
      body: typeof init?.body === "string" ? init.body : "",
    };
    calls.push(recorded);
    const reply = handler(recorded);
    const noBody = (reply.status ?? 200) === 204;
    const text = noBody ? null : reply.text ?? JSON.stringify(reply.json ?? {});
    return new Response(text, { status: reply.status ?? 200, headers: { "content-type": "application/json" } });
  };
  return { calls, impl };
}

const endpoints = { auth: "/auth", channels: "/channels", messages: "/messages", readstate: "/readstate", users: "/users" };

test("myWorkspaces carries setup_completed", async () => {
  const { impl } = fakeFetch(() => ({ json: { workspaces: [{ workspace_id: "1", name: "acme", slug: "acme", role: "owner", setup_completed: false }] } }));
  const api = new Api(endpoints, { fetch: impl });
  const workspaces = await api.myWorkspaces("tok");
  assert.equal(workspaces[0]!.setup_completed, false);
});

test("updateWorkspace PATCHes name and setup_completed", async () => {
  const { calls, impl } = fakeFetch(() => ({ json: { workspace_id: "1", name: "acme", slug: "acme", role: "owner", setup_completed: true } }));
  const api = new Api(endpoints, { fetch: impl });
  await api.updateWorkspace("tok", "1", { name: "acme", setup_completed: true });
  assert.equal(calls[0]!.url, "/users/v1/workspaces/1");
  assert.equal(calls[0]!.method, "PATCH");
  assert.deepEqual(JSON.parse(calls[0]!.body), { name: "acme", setup_completed: true });
});

test("default channel endpoints", async () => {
  const { calls, impl } = fakeFetch((req) => {
    if (req.method === "GET") return { json: { channels: [{ channel_id: "9", name: "general" }] } };
    return { status: 204 };
  });
  const api = new Api(endpoints, { fetch: impl });
  const channels = await api.defaultChannels("tok", "1");
  assert.deepEqual(channels, [{ channelId: "9", name: "general" }]);
  assert.equal(calls[0]!.url, "/channels/v1/workspaces/1/default-channels");

  const adding = await api.addDefaultChannel("tok", "1", "9", true);
  assert.equal(adding, false, "204 means nothing to add");
  assert.deepEqual(JSON.parse(calls[1]!.body), { channel_id: "9", add_existing_members: true });

  await api.removeDefaultChannel("tok", "1", "9");
  assert.equal(calls[2]!.method, "DELETE");
  assert.equal(calls[2]!.url, "/channels/v1/workspaces/1/default-channels/9");

  const { impl: impl202 } = fakeFetch(() => ({ status: 202 }));
  const api202 = new Api(endpoints, { fetch: impl202 });
  assert.equal(await api202.addDefaultChannel("tok", "1", "9", true), true);
});

test("addDefaultChannel surfaces 400 for a private channel", async () => {
  const { impl } = fakeFetch(() => ({ status: 400, json: { error: "channel is not public" } }));
  const api = new Api(endpoints, { fetch: impl });
  await assert.rejects(() => api.addDefaultChannel("tok", "1", "9", false), (e: unknown) => e instanceof ApiError && e.status === 400);
});

test("joinDefaultChannels posts the workspace and returns joined ids", async () => {
  const { calls, impl } = fakeFetch(() => ({ json: { joined: ["9", "10"] } }));
  const api = new Api(endpoints, { fetch: impl });
  const joined = await api.joinDefaultChannels("tok", "1");
  assert.deepEqual(joined, ["9", "10"]);
  assert.equal(calls[0]!.url, "/channels/v1/me/default-channels/join");
  assert.equal(calls[0]!.method, "POST");
  assert.deepEqual(JSON.parse(calls[0]!.body), { workspace_id: "1" });
});

test("workspace member role change and removal", async () => {
  const { calls, impl } = fakeFetch(() => ({ status: 204 }));
  const api = new Api(endpoints, { fetch: impl });
  await api.updateWorkspaceMemberRole("tok", "1", "42", "admin");
  assert.equal(calls[0]!.method, "PATCH");
  assert.equal(calls[0]!.url, "/users/v1/workspaces/1/members/42");
  assert.deepEqual(JSON.parse(calls[0]!.body), { role: "admin" });
  await api.removeWorkspaceMember("tok", "1", "42");
  assert.equal(calls[1]!.method, "DELETE");
  assert.equal(calls[1]!.url, "/users/v1/workspaces/1/members/42");
});

test("signup-policy, invite code and password reset", async () => {
  const { calls, impl } = fakeFetch((req) => {
    if (req.url.endsWith("/signup-policy")) return { json: { first_account: false, invite_only: true } };
    if (req.url.endsWith("/password-reset") && !req.url.includes("/members/")) return { status: 204 };
    return { json: { code: "reset-code-xyz", expires_at: "2026-10-31T13:00:00Z" } };
  });
  const api = new Api(endpoints, { fetch: impl });

  assert.deepEqual(await api.signupPolicy(), { first_account: false, invite_only: true });
  assert.equal(calls[0]!.url, "/auth/v1/signup-policy");

  await api.resetPassword("a@b.dev", "some-code", "new-password-123");
  assert.equal(calls[1]!.url, "/auth/v1/password-reset");
  assert.deepEqual(JSON.parse(calls[1]!.body), { email: "a@b.dev", code: "some-code", password: "new-password-123" });

  const reply = await api.passwordResetCode("tok", "1", "42");
  assert.equal(reply.code, "reset-code-xyz");
  assert.equal(calls[2]!.url, "/users/v1/workspaces/1/members/42/password-reset");

  const { impl: impl400 } = fakeFetch(() => ({ status: 400, json: { error: "invalid code" } }));
  const api400 = new Api(endpoints, { fetch: impl400 });
  await assert.rejects(() => api400.resetPassword("a@b.dev", "x", "y"), (e: unknown) => e instanceof ApiError && e.status === 400);
});

test("signup carries the invite code", async () => {
  const { calls, impl } = fakeFetch(() => ({ json: { user_id: "5" } }));
  const api = new Api(endpoints, { fetch: impl });
  await api.signup("a@b.dev", "password-123", "Aiko", "invite-7f3d");
  assert.deepEqual(JSON.parse(calls[0]!.body), { email: "a@b.dev", password: "password-123", name: "Aiko", invite: "invite-7f3d" });
  await api.signup("c@d.dev", "password-123", "Bo");
  assert.deepEqual(JSON.parse(calls[1]!.body), { email: "c@d.dev", password: "password-123", name: "Bo" }, "no invite key without a code");
});
