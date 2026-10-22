// SPDX-License-Identifier: Apache-2.0
// Display names for user IDs: asked for in batches (the users service takes up to 200
// per call), cached for the session, and shown as a short ID until known. Users who
// share no workspace with us are never returned; they stay short IDs.

import type { UserProfile } from "./api.ts";

export const MAX_USER_BATCH = 200;

export const shortId = (id: string) => `…${id.slice(-6)}`;

export class Directory {
  /** null: asked for, but not visible to us. */
  private readonly known = new Map<string, UserProfile | null>();
  private readonly queued = new Set<string>();
  private readonly inflight = new Set<string>();
  private readonly listeners = new Set<() => void>();
  private scheduled = false;
  private changes = 0;

  private readonly fetchUsers: (ids: string[]) => Promise<UserProfile[]>;
  constructor(fetchUsers: (ids: string[]) => Promise<UserProfile[]>) {
    this.fetchUsers = fetchUsers;
  }

  /** Bumps whenever a profile arrives (for React's useSyncExternalStore). */
  get version(): number {
    return this.changes;
  }

  profile(id: string): UserProfile | undefined {
    return this.known.get(id) ?? undefined;
  }

  /** The display name, or a short ID while unknown (and asks for it). */
  label(id: string): string {
    const known = this.known.get(id);
    if (known === undefined) this.want([id]);
    return known?.name || shortId(id);
  }

  /** Queues unknown IDs; everything queued in one tick goes out together. */
  want(ids: Iterable<string>): void {
    for (const id of ids) {
      if (!this.known.has(id) && !this.inflight.has(id)) this.queued.add(id);
    }
    if (this.queued.size === 0 || this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      void this.flush();
    });
  }

  /** A profile we learned elsewhere, e.g. our own after renaming. */
  remember(profile: UserProfile): void {
    this.known.set(profile.user_id, profile);
    this.changed();
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private async flush(): Promise<void> {
    const ids = [...this.queued];
    this.queued.clear();
    for (const id of ids) this.inflight.add(id);
    await Promise.all(
      Array.from({ length: Math.ceil(ids.length / MAX_USER_BATCH) }, async (_, i) => {
        const batch = ids.slice(i * MAX_USER_BATCH, (i + 1) * MAX_USER_BATCH);
        try {
          const found = new Map((await this.fetchUsers(batch)).map((p) => [p.user_id, p]));
          for (const id of batch) this.known.set(id, found.get(id) ?? null);
          this.changed();
        } catch {
          // Left unknown, so the next label() asks again.
        } finally {
          for (const id of batch) this.inflight.delete(id);
        }
      }),
    );
  }

  private changed(): void {
    this.changes++;
    for (const listener of this.listeners) listener();
  }
}
