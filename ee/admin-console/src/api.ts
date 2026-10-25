// SPDX-License-Identifier: LicenseRef-xmsg-EE
// Calls to the xmsg APIs on this origin, signed in with the web app's session: the console
// shares its refresh token (localStorage, same origin) and writes back the rotated one.

const REFRESH_KEY = "xmsg.refresh";

export class ApiError extends Error {
  readonly status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

export interface Licence {
  licensed: boolean;
  licence_id?: string;
  customer?: string;
  issued_at?: string;
  expires_at?: string;
  status?: "active" | "grace" | "expired";
  features?: string[];
  seats?: number;
  seats_used: number;
}

export interface Usage {
  members: number;
  members_by_role: Record<string, number>;
  joined_last_30_days: number;
  channels_by_type: Record<string, number>;
  archived_channels: number;
  active_sessions: number;
  sso_linked_members: number;
  sso_enforced: boolean;
}

export interface Workspace {
  workspace_id: string;
  name: string;
  slug: string;
  role: string;
}

export interface Member {
  user_id: string;
  name: string;
  avatar_url: string | null;
  role: string;
}

export interface SsoPolicy {
  enforced: boolean;
  available: boolean;
  editable: boolean;
}

function storage<T>(f: () => T): T | undefined {
  try {
    return f();
  } catch {
    return undefined; // storage blocked: behave as signed out
  }
}

let access: { token: string; userId: string; until: number } | null = null;
let refreshing: Promise<boolean> | null = null;

/** Exchanges the shared refresh token for an access token (one exchange at a time). */
function refresh(): Promise<boolean> {
  refreshing ??= (async () => {
    try {
      const token = storage(() => localStorage.getItem(REFRESH_KEY));
      if (!token) return false;
      const response = await fetch("/auth/v1/token/refresh", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ refresh_token: token }),
      });
      if (!response.ok) {
        if (response.status === 401) storage(() => localStorage.removeItem(REFRESH_KEY));
        return false;
      }
      const tokens = (await response.json()) as { access_token: string; refresh_token: string; expires_in: number; user_id: string };
      storage(() => localStorage.setItem(REFRESH_KEY, tokens.refresh_token));
      // Renew a minute early.
      access = { token: tokens.access_token, userId: tokens.user_id, until: Date.now() + (tokens.expires_in - 60) * 1000 };
      return true;
    } finally {
      refreshing = null;
    }
  })();
  return refreshing;
}

/** The signed-in user's ID, or null when the browser has no xmsg session. */
export async function signIn(): Promise<string | null> {
  if (access && access.until > Date.now()) return access.userId;
  return (await refresh()) ? access!.userId : null;
}

export async function request<T>(method: string, path: string, body?: unknown): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    if (!access || access.until <= Date.now()) {
      if (!(await refresh())) throw new ApiError(401, "Your session has ended: sign in to xmsg again");
    }
    const response = await fetch(path, {
      method,
      headers: { authorization: `Bearer ${access!.token}`, ...(body === undefined ? {} : { "content-type": "application/json" }) },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (response.status === 401 && attempt === 0) {
      access = null; // revoked or rotated elsewhere: one fresh try
      continue;
    }
    if (response.status === 204) return undefined as T;
    const json = await response.json().catch(() => ({}));
    if (!response.ok) throw new ApiError(response.status, (json as { error?: string }).error ?? `HTTP ${response.status}`);
    return json as T;
  }
}

export const api = {
  licence: () => request<Licence>("GET", "/admin/api/v1/licence"),
  usage: (ws: string) => request<Usage>("GET", `/admin/api/v1/workspaces/${ws}/usage`),
  workspaces: async () => (await request<{ workspaces: Workspace[] }>("GET", "/users/v1/me/workspaces")).workspaces,
  members: (ws: string, after?: string) =>
    request<{ members: Member[]; next: string | null }>("GET", `/users/v1/workspaces/${ws}/members?limit=200${after ? `&after=${after}` : ""}`),
  setRole: (ws: string, user: string, role: string) => request<void>("PATCH", `/users/v1/workspaces/${ws}/members/${user}`, { role }),
  remove: (ws: string, user: string) => request<void>("DELETE", `/users/v1/workspaces/${ws}/members/${user}`),
  ssoPolicy: (ws: string) => request<SsoPolicy>("GET", `/auth/v1/workspaces/${ws}/sso`),
  setSso: (ws: string, enforced: boolean) =>
    request<{ enforced: boolean; recovery_codes?: string[] }>("PUT", `/auth/v1/workspaces/${ws}/sso`, { enforced }),
  newRecoveryCodes: async (ws: string) =>
    (await request<{ recovery_codes: string[] }>("POST", `/auth/v1/workspaces/${ws}/sso/recovery-codes`)).recovery_codes,
};
