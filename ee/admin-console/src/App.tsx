// SPDX-License-Identifier: LicenseRef-xmsg-EE
import { useCallback, useEffect, useState } from "react";
import { ApiError, api, signIn, type Licence, type Member, type SsoPolicy, type Usage, type Workspace } from "./api.ts";

type Tab = "overview" | "members" | "security";

function message(e: unknown): string {
  if (e instanceof ApiError) return e.status >= 500 ? "The service is unavailable, try again" : e.message;
  return "Can't reach the server";
}

function Failed({ error }: { error: string }) {
  return error ? <p className="error" role="alert">{error}</p> : null;
}

export function App() {
  const [me, setMe] = useState<string | null | undefined>(undefined);
  const [workspaces, setWorkspaces] = useState<Workspace[] | null>(null);
  const [current, setCurrent] = useState<string>("");
  const [tab, setTab] = useState<Tab>("overview");
  const [error, setError] = useState("");

  useEffect(() => {
    signIn()
      .then(async (id) => {
        setMe(id);
        if (!id) return;
        const mine = (await api.workspaces()).filter((w) => w.role === "owner" || w.role === "admin");
        setWorkspaces(mine);
        setCurrent(mine[0]?.workspace_id ?? "");
      })
      .catch((e) => {
        setMe(null);
        setError(message(e));
      });
  }, []);

  if (me === undefined) return <div className="center muted">Loading…</div>;
  if (me === null)
    return (
      <div className="center">
        <div className="card narrow">
          <h1>xmsg admin</h1>
          <p>Sign in to xmsg first; the console uses the same sign-in.</p>
          <Failed error={error} />
          <a className="button primary" href="/">
            Sign in
          </a>
        </div>
      </div>
    );
  if (workspaces && workspaces.length === 0)
    return (
      <div className="center">
        <div className="card narrow">
          <h1>xmsg admin</h1>
          <p>The console is for workspace owners and admins, and you aren't one in any workspace.</p>
          <a href="/">Back to xmsg</a>
        </div>
      </div>
    );
  const workspace = workspaces?.find((w) => w.workspace_id === current);

  return (
    <div className="shell">
      <header className="top">
        <span className="brand">xmsg admin</span>
        {workspaces && workspaces.length > 1 ? (
          <select aria-label="Workspace" value={current} onChange={(e) => setCurrent(e.target.value)}>
            {workspaces.map((w) => (
              <option key={w.workspace_id} value={w.workspace_id}>
                {w.name}
              </option>
            ))}
          </select>
        ) : (
          <span className="strong">{workspace?.name}</span>
        )}
        <nav className="tabs" role="tablist">
          {(["overview", "members", "security"] as const).map((t) => (
            <button key={t} role="tab" aria-selected={tab === t} onClick={() => setTab(t)}>
              {t[0].toUpperCase() + t.slice(1)}
            </button>
          ))}
        </nav>
        <a className="back" href="/">
          Back to xmsg
        </a>
      </header>
      <main className="content">
        <Failed error={error} />
        {workspace && tab === "overview" && <Overview key={current} workspace={workspace} />}
        {workspace && tab === "members" && <Members key={current} workspace={workspace} me={me} />}
        {workspace && tab === "security" && <Security key={current} workspace={workspace} />}
      </main>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="stat">
      <div className="value">{value}</div>
      <div className="muted small">{label}</div>
    </div>
  );
}

const day = (iso?: string) => (iso ? new Date(iso).toLocaleDateString() : "");

function LicenceCard({ licence }: { licence: Licence }) {
  if (!licence.licensed)
    return (
      <section className="card">
        <h2>Licence</h2>
        <p>No Enterprise licence is installed: Enterprise features are off. Messaging works as usual.</p>
        <p className="muted small">{licence.seats_used} people are in at least one workspace.</p>
      </section>
    );
  const status = { active: "Active", grace: "Expired: in the 30-day grace period", expired: "Expired" }[licence.status ?? "active"];
  const over = licence.seats !== undefined && licence.seats_used > licence.seats;
  return (
    <section className="card">
      <div className="row between">
        <h2>Licence</h2>
        <span className={`badge ${licence.status}`}>{status}</span>
      </div>
      <dl className="facts">
        <dt>Customer</dt>
        <dd>{licence.customer}</dd>
        <dt>Valid until</dt>
        <dd>{day(licence.expires_at)}</dd>
        <dt>Seats</dt>
        <dd className={over ? "warn" : ""}>
          {licence.seats_used} of {licence.seats} used{over ? " (over the licensed count)" : ""}
        </dd>
        <dt>Features</dt>
        <dd>{licence.features?.join(", ") || "none"}</dd>
      </dl>
      {licence.status !== "active" && (
        <p className="muted small">
          Sign-in through SSO and LDAP keeps working and recovery codes always work; changing their settings needs a renewed licence.
        </p>
      )}
    </section>
  );
}

function Overview({ workspace }: { workspace: Workspace }) {
  const [licence, setLicence] = useState<Licence | null>(null);
  const [usage, setUsage] = useState<Usage | null>(null);
  const [usageError, setUsageError] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    api.licence().then(setLicence, (e) => setError(message(e)));
    api.usage(workspace.workspace_id).then(setUsage, (e) => setUsageError(message(e)));
  }, [workspace.workspace_id]);

  return (
    <div className="stack">
      <Failed error={error} />
      {!licence && !error && <p className="muted">Loading…</p>}
      {licence && <LicenceCard licence={licence} />}
      <section className="card">
        <h2>Usage</h2>
        {usageError && <p className="muted">{usageError}</p>}
        {usage && (
          <>
            <div className="stats">
              <Stat label="members" value={usage.members} />
              <Stat label="joined in the last 30 days" value={usage.joined_last_30_days} />
              <Stat label="active sessions" value={usage.active_sessions} />
              <Stat label="sign in through SSO or LDAP" value={usage.sso_linked_members} />
            </div>
            <dl className="facts">
              <dt>Roles</dt>
              <dd>
                {Object.entries(usage.members_by_role)
                  .map(([role, n]) => `${n} ${role}${n === 1 ? "" : "s"}`)
                  .join(", ")}
              </dd>
              <dt>Channels</dt>
              <dd>
                {Object.entries(usage.channels_by_type)
                  .map(([kind, n]) => `${n} ${kind.replace("_", " ")}`)
                  .join(", ") || "none"}
                {usage.archived_channels > 0 && `, ${usage.archived_channels} archived`}
              </dd>
              <dt>Single sign-on</dt>
              <dd>{usage.sso_enforced ? "Required" : "Optional"}</dd>
            </dl>
          </>
        )}
      </section>
    </div>
  );
}

function Members({ workspace, me }: { workspace: Workspace; me: string }) {
  const [members, setMembers] = useState<Member[]>([]);
  const [next, setNext] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState<string>("");
  const ws = workspace.workspace_id;
  const owner = workspace.role === "owner";

  const load = useCallback(
    async (after?: string) => {
      try {
        const page = await api.members(ws, after);
        setMembers((m) => (after ? [...m, ...page.members] : page.members));
        setNext(page.next);
      } catch (e) {
        setError(message(e));
      }
    },
    [ws],
  );
  useEffect(() => {
    void load();
  }, [load]);

  const act = async (user: string, f: () => Promise<void>, done: (m: Member[]) => Member[]) => {
    setBusy(user);
    setError("");
    try {
      await f();
      setMembers(done);
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy("");
    }
  };
  // The users service's rules: the owner removes anyone else, admins remove members.
  const removable = (m: Member) => m.role !== "owner" && m.user_id !== me && (owner || m.role === "member");

  return (
    <section className="card">
      <div className="row between">
        <h2>Members</h2>
        <span className="muted small">{owner ? "As the owner you can change roles." : "Only the owner changes roles."}</span>
      </div>
      <Failed error={error} />
      <table className="members">
        <thead>
          <tr>
            <th>Name</th>
            <th>Role</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {members.map((m) => (
            <tr key={m.user_id}>
              <td>
                {m.name}
                {m.user_id === me && <span className="muted"> (you)</span>}
              </td>
              <td>
                {owner && m.role !== "owner" ? (
                  <select
                    aria-label={`Role of ${m.name}`}
                    value={m.role}
                    disabled={busy === m.user_id}
                    onChange={(e) => {
                      const role = e.target.value;
                      void act(m.user_id, () => api.setRole(ws, m.user_id, role), (all) => all.map((x) => (x.user_id === m.user_id ? { ...x, role } : x)));
                    }}
                  >
                    <option value="admin">admin</option>
                    <option value="member">member</option>
                  </select>
                ) : (
                  m.role
                )}
              </td>
              <td className="actions">
                {removable(m) && (
                  <button
                    className="danger"
                    disabled={busy === m.user_id}
                    onClick={() => {
                      if (!confirm(`Remove ${m.name} from ${workspace.name}? They lose access to its channels at once.`)) return;
                      void act(m.user_id, () => api.remove(ws, m.user_id), (all) => all.filter((x) => x.user_id !== m.user_id));
                    }}
                  >
                    Remove
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {next && <button onClick={() => void load(next)}>Load more</button>}
    </section>
  );
}

function Codes({ codes }: { codes: string[] }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="codes" role="status">
      <p className="strong">Your recovery codes. They're shown only now: store them somewhere safe.</p>
      <p className="muted small">Each signs you in once when single sign-on is down. New codes replace these.</p>
      <ol className="mono">
        {codes.map((c) => (
          <li key={c}>{c}</li>
        ))}
      </ol>
      <button
        onClick={() =>
          navigator.clipboard.writeText(codes.join("\n")).then(
            () => setCopied(true),
            () => setCopied(false),
          )
        }
      >
        {copied ? "Copied" : "Copy all"}
      </button>
    </div>
  );
}

function Security({ workspace }: { workspace: Workspace }) {
  const [policy, setPolicy] = useState<SsoPolicy | null>(null);
  const [codes, setCodes] = useState<string[]>([]);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const ws = workspace.workspace_id;

  useEffect(() => {
    api.ssoPolicy(ws).then(setPolicy, (e) => setError(message(e)));
  }, [ws]);

  const run = async (f: () => Promise<void>) => {
    setBusy(true);
    setError("");
    try {
      await f();
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };
  const toggle = () =>
    run(async () => {
      const enforce = !policy!.enforced;
      if (!enforce && !confirm("Allow passwords again in this workspace?")) return;
      const reply = await api.setSso(ws, enforce);
      setPolicy({ ...policy!, enforced: reply.enforced });
      if (reply.recovery_codes) setCodes(reply.recovery_codes);
    });

  return (
    <section className="card">
      <h2>Single sign-on</h2>
      <Failed error={error} />
      {policy && (
        <>
          <p>
            {policy.enforced
              ? "Required: members sign in through the identity provider or directory, not with passwords."
              : "Optional: members may also sign in with a password."}
          </p>
          {!policy.available && <p className="muted">No identity provider is set up, so single sign-on can't be required.</p>}
          {policy.available && !policy.editable && (
            <p className="muted">The licence doesn't allow changing this. Sign-in keeps working as it is.</p>
          )}
          <div className="row">
            {policy.available && policy.editable && (
              <button className={policy.enforced ? "" : "primary"} disabled={busy} onClick={() => void toggle()}>
                {policy.enforced ? "Allow passwords" : "Require single sign-on"}
              </button>
            )}
            <button disabled={busy} onClick={() => void run(async () => setCodes(await api.newRecoveryCodes(ws)))}>
              New recovery codes
            </button>
          </div>
          {!policy.enforced && policy.available && policy.editable && (
            <p className="muted small">To require it, sign in through single sign-on once yourself; you'll get recovery codes for when it's down.</p>
          )}
        </>
      )}
      {codes.length > 0 && <Codes codes={codes} />}
    </section>
  );
}
