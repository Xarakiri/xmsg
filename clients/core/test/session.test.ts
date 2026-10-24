// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { Api, ApiError, type Locks, Session, type TokenStore, type Tokens } from "../src/api.ts";

/** The auth service's rotation without the grace: presenting a rotated token revokes the session. */
class FakeApi extends Api {
  live = new Set(["r0"]);
  presented: string[] = [];
  private next = 1;
  constructor() {
    super({ auth: "", channels: "", messages: "", users: "", readstate: "/readstate" });
  }
  override async refresh(refreshToken: string): Promise<Tokens> {
    this.presented.push(refreshToken);
    await new Promise((r) => setTimeout(r, 5));
    if (!this.live.delete(refreshToken)) {
      this.live.clear();
      throw new ApiError(401, "invalid credentials");
    }
    const n = this.next++;
    this.live.add(`r${n}`);
    return { access_token: `a${n}`, refresh_token: `r${n}`, expires_in: 900, user_id: "1", session_id: "7" };
  }
}

/** localStorage shared by every tab. */
class MemoryStore implements TokenStore {
  value: string | null;
  constructor(value: string | null) {
    this.value = value;
  }
  load() {
    return this.value;
  }
  save(refreshToken: string | null) {
    this.value = refreshToken;
  }
}

/** One exclusive lock per name, granted in request order, as navigator.locks does across tabs. */
class FakeLocks implements Locks {
  private tails = new Map<string, Promise<unknown>>();
  request<T>(name: string, callback: () => Promise<T>): Promise<T> {
    const run = (this.tails.get(name) ?? Promise.resolve()).then(callback);
    this.tails.set(name, run.catch(() => undefined));
    return run;
  }
}

test("concurrent callers in one tab share a single refresh", async () => {
  const api = new FakeApi();
  const store = new MemoryStore("r0");
  const session = new Session(api, store, null);
  const tokens = await Promise.all([session.accessToken(), session.accessToken(), session.accessToken()]);
  assert.deepEqual(tokens, ["a1", "a1", "a1"]);
  assert.deepEqual(api.presented, ["r0"]);
  assert.equal(store.value, "r1");
  assert.equal(await session.accessToken(), "a1", "cached until near expiry");
});

test("two tabs refreshing at once take turns and continue the chain", async () => {
  const api = new FakeApi();
  const store = new MemoryStore("r0");
  const locks = new FakeLocks();
  const [a, b] = [new Session(api, store, locks), new Session(api, store, locks)];
  const tokens = await Promise.all([a.accessToken(), b.accessToken()]);
  assert.deepEqual(tokens, ["a1", "a2"]);
  assert.deepEqual(api.presented, ["r0", "r1"], "the second tab re-read the rotated token");
  assert.equal(store.value, "r2");
});

test("without the lock, two tabs collide and lose the session", async () => {
  const api = new FakeApi();
  const store = new MemoryStore("r0");
  const [a, b] = [new Session(api, store, null), new Session(api, store, null)];
  const results = await Promise.allSettled([a.accessToken(), b.accessToken()]);
  assert.deepEqual(results.map((r) => r.status), ["fulfilled", "rejected"]);
  assert.deepEqual(api.presented, ["r0", "r0"]);
});

test("a 401 clears the stored token, but not one another tab stored meanwhile", async () => {
  const api = new FakeApi();
  api.live.clear();
  const store = new MemoryStore("r0");
  await assert.rejects(new Session(api, store, null).accessToken(), (e) => e instanceof ApiError && e.status === 401);
  assert.equal(store.value, null);

  const racing = new MemoryStore("r0");
  const original = api.refresh.bind(api);
  api.refresh = async (token) => {
    racing.save("from-login"); // another tab logs in while this refresh is in flight
    return original(token);
  };
  await assert.rejects(new Session(api, racing, null).accessToken());
  assert.equal(racing.value, "from-login");
});

test("no stored token means not logged in, without calling the server", async () => {
  const api = new FakeApi();
  await assert.rejects(new Session(api, new MemoryStore(null), new FakeLocks()).accessToken(), (e) => e instanceof ApiError && e.status === 401);
  assert.deepEqual(api.presented, []);
});

test("single sign-on hands the session over in the URL fragment, or why it failed", async () => {
  const { fromSsoFragment } = await import("../src/api.ts");
  const ok = fromSsoFragment("#access_token=a.b.c&refresh_token=r1&user_id=7&session_id=9&expires_in=900");
  assert.deepEqual(ok.tokens, { access_token: "a.b.c", refresh_token: "r1", user_id: "7", session_id: "9", expires_in: 900 });
  assert.deepEqual(fromSsoFragment("#sso_error=this%20sign-in%20link%20has%20expired"), { error: "this sign-in link has expired" });
  assert.deepEqual(fromSsoFragment("#channel=7"), {}, "other fragments are left alone");
  assert.deepEqual(fromSsoFragment(""), {});
});
