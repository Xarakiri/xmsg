// SPDX-License-Identifier: Apache-2.0
import { useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from "react";
import {
  ApiError,
  MAX_FILE_BYTES,
  canPost,
  fromSsoFragment,
  mentionCount,
  threadKey,
  unread,
  type Attachment,
  type Channel,
  type ChatState,
  type ConnectionStatus,
  type Directory,
  type Message,
  type Presence,
  type SearchHit,
} from "@xmsg/client-core";
import { Attachments, PendingFiles, formatSize, type PendingFile } from "./files.tsx";
import { api, session } from "./session.ts";
import { disablePush, enablePush, pushState, type PushState } from "./push.ts";
import { loadState } from "./storage.ts";
import { useChat } from "./useChat.ts";
import { useNames } from "./useNames.ts";

// Back from single sign-on: the session's tokens (or the reason it failed) are in the
// fragment. Taken once, and removed from the address bar and history at once.
const sso = fromSsoFragment(location.hash);
if (sso.tokens || sso.error) history.replaceState(null, "", location.pathname + location.search);
if (sso.tokens) session.adopt(sso.tokens);

export function App() {
  const [me, setMe] = useState<string | null>(null);
  const [checking, setChecking] = useState(session.hasRefreshToken);

  // Resume a previous login from the stored refresh token.
  useEffect(() => {
    if (!checking) return;
    session
      .accessToken()
      .then(() => setMe(session.userId))
      .catch(() => undefined)
      .finally(() => setChecking(false));
  }, [checking]);

  if (checking) return <div className="center muted">Loading…</div>;
  if (!me) return <SignIn onSignedIn={setMe} />;
  return (
    <Chat
      key={me}
      me={me}
      onSignOut={async () => {
        await session.logout();
        setMe(null);
      }}
    />
  );
}

function errorText(e: unknown): string {
  if (e instanceof ApiError) return e.status >= 500 ? "Service unavailable, try again" : e.message;
  return "Can't reach the server";
}

function SignIn({ onSignedIn }: { onSignedIn: (userId: string) => void }) {
  const [mode, setMode] = useState<"in" | "up" | "code">("in");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [code, setCode] = useState("");
  const [name, setName] = useState("");
  const [error, setError] = useState(sso.error ?? "");
  const [busy, setBusy] = useState(false);
  const [providers, setProviders] = useState<{ id: string; name: string }[]>([]);
  useEffect(() => {
    api.ssoProviders().then(setProviders).catch(() => setProviders([]));
  }, []);

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError("");
    try {
      if (mode === "up") await api.signup(email, password, name);
      session.adopt(mode === "code" ? await api.loginWithRecoveryCode(email, code) : await api.login(email, password));
      onSignedIn(session.userId);
    } catch (err) {
      setError(errorText(err));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="center">
      <form className="card auth" onSubmit={submit}>
        <h1>xmsg</h1>
        <div className="tabs" role="tablist">
          <button type="button" role="tab" aria-selected={mode === "in"} onClick={() => setMode("in")}>
            Sign in
          </button>
          <button type="button" role="tab" aria-selected={mode === "up"} onClick={() => setMode("up")}>
            Create account
          </button>
        </div>
        {mode === "up" && (
          <label>
            Name
            <input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} autoComplete="name" />
          </label>
        )}
        <label>
          Email
          <input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required autoComplete="email" />
        </label>
        {mode === "code" ? (
          <label>
            Recovery code
            <input value={code} onChange={(e) => setCode(e.target.value)} required autoComplete="one-time-code" placeholder="xxxx-xxxx-xxxx-xxxx" />
          </label>
        ) : (
          <label>
            Password
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={mode === "up" ? 10 : undefined}
              autoComplete={mode === "up" ? "new-password" : "current-password"}
            />
          </label>
        )}
        {error && <p className="error">{error}</p>}
        <button className="primary" disabled={busy}>
          {mode === "up" ? "Create account" : "Sign in"}
        </button>
        {providers.length > 0 && (
          <div className="sso">
            <div className="muted small">or</div>
            {providers.map((p) => (
              <a key={p.id} className="button" href={api.ssoStartUrl(p.id, location.pathname)}>
                Continue with {p.name}
              </a>
            ))}
            <button type="button" className="link small" onClick={() => setMode(mode === "code" ? "in" : "code")}>
              {mode === "code" ? "Sign in with a password" : "Use a recovery code"}
            </button>
          </div>
        )}
      </form>
    </div>
  );
}

function channelTitle(c: Channel, me: string, names: Directory): string {
  if (c.name) return c.kind === "public" || c.kind === "private" || c.kind === "broadcast" ? `# ${c.name}` : c.name;
  const others = c.members.filter((m) => m !== me);
  if (c.kind === "dm" || c.kind === "group_dm") return others.length ? others.map((m) => names.label(m)).join(", ") : "Direct message";
  return `Channel …${c.id.slice(-6)}`;
}

/** A member's name, with their role when it isn't plain member: "Ada (admin)". */
function memberLabel(member: string, me: string, names: Directory, roles?: Channel["roles"]): string {
  const name = member === me ? "You" : names.label(member);
  const role = roles?.[member];
  return role && role !== "member" ? `${name} (${role})` : name;
}

/** "Ada (admin), Bob, You and 4 more" */
function memberList(members: string[], me: string, names: Directory, roles?: Channel["roles"], shown = 5): string {
  const labels = members.slice(0, shown).map((m) => memberLabel(m, me, names, roles));
  const more = members.length - labels.length;
  return more > 0 ? `${labels.join(", ")} and ${more} more` : labels.join(", ");
}

/** Opens the local database first, so the chat renders with what it had (plan §9.2). */
function Chat({ me, onSignOut }: { me: string; onSignOut: () => void }) {
  const [initial, setInitial] = useState<ChatState | null>(null);
  useEffect(() => {
    void loadState(me).then(setInitial);
  }, [me]);
  if (!initial) return <div className="center muted">Loading…</div>;
  return <ChatView me={me} initial={initial} onSignOut={onSignOut} />;
}

function ChatView({ me, initial, onSignOut }: { me: string; initial: ChatState; onSignOut: () => void }) {
  const { state, status, syncError, send, upload, nameChannel, markRead, setMuted, retry, typingIn, notifyTyping, react, openThread, watchPresence, presenceOf, view } = useChat(me, initial);
  // Search (plan §5.12): results replace the conversation until one is picked or closed.
  const [query, setQuery] = useState("");
  const [hits, setHits] = useState<SearchHit[] | null>(null);
  const [searchNote, setSearchNote] = useState("");
  const runSearch = async () => {
    const q = query.trim();
    if (!q) return setHits(null);
    setSearchNote("");
    try {
      setHits(await api.search(await session.accessToken(), q));
    } catch (err) {
      setHits([]);
      setSearchNote(errorText(err));
    }
  };
  // The thread open next to the conversation: its root message's msg_seq.
  const [threadOf, setThreadOf] = useState<number | null>(null);
  const names = useNames(me);
  // A notification click opens its channel: `?channel=` in a new window, a message otherwise.
  const [selected, setSelected] = useState<string | null>(() => new URLSearchParams(location.search).get("channel"));
  useEffect(() => {
    const onMessage = (e: MessageEvent) => {
      if (e.data?.type === "open-channel" && typeof e.data.channelId === "string") setSelected(e.data.channelId);
    };
    navigator.serviceWorker?.addEventListener("message", onMessage);
    return () => navigator.serviceWorker?.removeEventListener("message", onMessage);
  }, []);
  const [panel, setPanel] = useState<"none" | "channel" | "dm">("none");

  const channels = useMemo(() => Object.values(state.channels).filter((c) => !c.left), [state.channels]);
  const rooms = channels.filter((c) => c.kind !== "dm" && c.kind !== "group_dm");
  const dms = channels.filter((c) => c.kind === "dm" || c.kind === "group_dm");
  const current = selected ? state.channels[selected] : undefined;
  // Presence for the people on screen: DM partners and the open channel's members.
  const watched = useMemo(() => {
    const ids = new Set<string>();
    for (const c of channels) if (c.kind === "dm") for (const m of c.members) if (m !== me) ids.add(m);
    for (const m of current?.members ?? []) if (m !== me) ids.add(m);
    return [...ids].sort().slice(0, 200);
  }, [channels, current, me]);
  useEffect(() => watchPresence(watched), [watched.join(",")]); // eslint-disable-line react-hooks/exhaustive-deps

  // The channel on screen (none while the tab is hidden): large channels send full events
  // only for it, and a channel that moved on meanwhile catches up when opened (plan §6.7).
  useEffect(() => {
    const update = () => view(document.visibilityState === "visible" ? (current?.id ?? null) : null);
    update();
    document.addEventListener("visibilitychange", update);
    return () => document.removeEventListener("visibilitychange", update);
  }, [current?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  // The open channel is read while the tab is visible (debounced, so a burst of
  // messages costs one update; also when the tab comes back).
  const pendingUnread = current ? unread(current) : 0;
  useEffect(() => {
    if (!current || pendingUnread === 0) return;
    const id = setTimeout(() => {
      if (document.visibilityState === "visible") markRead(current.id);
    }, 400);
    const onVisible = () => document.visibilityState === "visible" && markRead(current.id);
    document.addEventListener("visibilitychange", onVisible);
    return () => {
      clearTimeout(id);
      document.removeEventListener("visibilitychange", onVisible);
    };
  }, [current?.id, pendingUnread]);

  return (
    <div className="app">
      <aside className="sidebar">
        <header className="me">
          <StatusDot status={status} />
          <div>
            <div className="strong">
              <button className="link strong" title="Change your display name" onClick={() => rename(me, names)}>
                {names.label(me)}
              </button>
              {syncError && <span className="muted small"> · couldn't load history</span>}
            </div>
            <button className="link mono" title="Copy your user ID" onClick={() => navigator.clipboard?.writeText(me)}>
              {me}
            </button>
          </div>
        </header>
        <form
          className="search"
          role="search"
          onSubmit={(e) => {
            e.preventDefault();
            void runSearch();
          }}
        >
          <input type="search" placeholder="Search messages" value={query} onChange={(e) => setQuery(e.target.value)} aria-label="Search messages" />
        </form>
        <Section title="Channels" onAdd={() => setPanel("channel")}>
          {rooms.map((c) => (
            <Item key={c.id} active={c.id === selected} onClick={() => setSelected(c.id)} label={channelTitle(c, me, names)} unread={unread(c)} mentions={mentionCount(c)} muted={c.muted} />
          ))}
        </Section>
        <Section title="Direct messages" onAdd={() => setPanel("dm")}>
          {dms.map((c) => {
            // A 1:1 DM shows the other person's presence.
            const other = c.kind === "dm" ? c.members.find((m) => m !== me) : undefined;
            return (
              <Item
                key={c.id}
                active={c.id === selected}
                onClick={() => setSelected(c.id)}
                label={channelTitle(c, me, names)}
                unread={unread(c)}
                mentions={mentionCount(c)}
                muted={c.muted}
                presence={other ? presenceOf(other) : undefined}
              />
            );
          })}
        </Section>
        <footer>
          <NotificationsToggle />
          <button className="link" onClick={onSignOut}>
            Sign out
          </button>
        </footer>
      </aside>
      <main className="main">
        {panel !== "none" && (
          <CreatePanel
            kind={panel}
            onClose={() => setPanel("none")}
            onCreated={(id, name) => {
              if (name) nameChannel(id, name);
              setSelected(id);
              setPanel("none");
            }}
          />
        )}
        {hits !== null ? (
          <SearchResults
            hits={hits}
            note={searchNote}
            channelName={(id) => (state.channels[id] ? channelTitle(state.channels[id]!, me, names) : "a channel")}
            names={names}
            me={me}
            onPick={(hit) => {
              setHits(null);
              setSelected(hit.channelId);
              setThreadOf(hit.reply ? hit.rootMsgSeq : null);
              const root = hit.reply ? (state.messages[hit.channelId] ?? []).find((m) => m.msgSeq === hit.rootMsgSeq) : undefined;
              if (root?.messageId) openThread(hit.channelId, { msgSeq: root.msgSeq, messageId: root.messageId });
            }}
            onClose={() => setHits(null)}
          />
        ) : current ? (
          <Conversation
            channel={current}
            me={me}
            names={names}
            messages={state.messages[current.id] ?? []}
            onSend={(b, files) => send(current.id, b, undefined, files)}
            onUpload={(file) => upload(current.id, file)}
            onToggleMute={() => setMuted(current.id, !current.muted)}
            onRetry={retry}
            typing={typingIn(current.id)}
            onTyping={() => notifyTyping(current.id)}
            onReact={(msgSeq, emoji, add) => react(current.id, msgSeq, emoji, add)}
            presenceOf={presenceOf}
            onOpenThread={(m) => {
              setThreadOf(m.msgSeq);
              openThread(current.id, { msgSeq: m.msgSeq, messageId: messageIdOf(m) });
            }}
          />
        ) : (
          <div className="center muted">{channels.length ? "Pick a conversation" : "Create a channel or start a DM"}</div>
        )}
        {current && threadOf !== null && (() => {
          const root = (state.messages[current.id] ?? []).find((m) => m.msgSeq === threadOf);
          if (!root) return null;
          const ref = { msgSeq: root.msgSeq, messageId: messageIdOf(root) };
          return (
            <ThreadPanel
              root={root}
              replies={state.messages[threadKey(current.id, root.msgSeq)] ?? []}
              me={me}
              names={names}
              onSend={(b) => send(current.id, b, ref)}
              onRetry={retry}
              onClose={() => setThreadOf(null)}
            />
          );
        })()}
      </main>
    </div>
  );
}

async function rename(me: string, names: Directory) {
  const name = prompt("Your display name", names.profile(me)?.name ?? "")?.trim();
  if (!name) return;
  try {
    names.remember(await api.updateMe(await session.accessToken(), { name }));
  } catch (err) {
    alert(errorText(err));
  }
}

function StatusDot({ status }: { status: ConnectionStatus }) {
  const label = { connecting: "Connecting…", online: "Connected", offline: "Reconnecting…", unauthorized: "Signed out" }[status];
  return <span className={`dot ${status}`} title={label} aria-label={label} />;
}

function Section({ title, onAdd, children }: { title: string; onAdd: () => void; children: React.ReactNode }) {
  return (
    <section>
      <h2>
        {title}
        <button className="icon" onClick={onAdd} aria-label={title === "Channels" ? "New channel" : "New direct message"}>
          +
        </button>
      </h2>
      <ul>{children}</ul>
    </section>
  );
}

function Item({
  label,
  presence,
  mentions,
  active,
  unread,
  muted,
  onClick,
}: {
  label: string;
  active: boolean;
  unread: number;
  mentions: number;
  muted: boolean;
  presence?: Presence;
  onClick: () => void;
}) {
  // Muted channels never shout: no bold, no badge (as in Slack). Mentions still show.
  const shown = muted ? 0 : unread;
  const classes = ["item", active && "active", shown > 0 && !active && "unread", muted && "muted"].filter(Boolean).join(" ");
  return (
    <li>
      <button className={classes} onClick={onClick} aria-label={`${label}${muted ? ", muted" : ""}${shown > 0 ? `, ${shown} unread` : ""}`}>
        {presence && <PresenceDot status={presence} />}
        <span className="label">{label}</span>
        {mentions > 0 && !active && <span className="badge mention">@{mentions > 99 ? "99+" : mentions}</span>}
        {shown > 0 && !mentions && !active && <span className="badge">{shown > 99 ? "99+" : shown}</span>}
      </button>
    </li>
  );
}

const ids = (text: string) => text.split(/[\s,]+/).filter(Boolean);

function CreatePanel({ kind, onClose, onCreated }: { kind: "channel" | "dm"; onClose: () => void; onCreated: (id: string, name?: string) => void }) {
  const [name, setName] = useState("");
  const [type, setType] = useState<"public" | "private" | "broadcast">("public");
  const [members, setMembers] = useState("");
  const [error, setError] = useState("");

  const submit = async (e: FormEvent) => {
    e.preventDefault();
    setError("");
    try {
      const token = await session.accessToken();
      const reply = kind === "channel" ? await api.createChannel(token, type, name.trim(), ids(members)) : await api.openDm(token, ids(members));
      onCreated(reply.channel_id, kind === "channel" ? name.trim() : undefined);
    } catch (err) {
      setError(errorText(err));
    }
  };

  return (
    <form className="card panel" onSubmit={submit}>
      <h3>{kind === "channel" ? "New channel" : "New direct message"}</h3>
      {kind === "channel" && (
        <div className="row">
          <label className="grow">
            Name
            <input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} autoFocus />
          </label>
          <label>
            Type
            <select value={type} onChange={(e) => setType(e.target.value as typeof type)}>
              <option value="public">Public</option>
              <option value="private">Private</option>
              <option value="broadcast">Broadcast</option>
            </select>
          </label>
        </div>
      )}
      <label>
        {kind === "channel" ? "Members (user IDs, optional)" : "User IDs (one for a DM, up to 8 for a group)"}
        <input
          className="mono"
          value={members}
          onChange={(e) => setMembers(e.target.value)}
          required={kind === "dm"}
          autoFocus={kind === "dm"}
          placeholder="106449829163870339, …"
        />
      </label>
      {error && <p className="error">{error}</p>}
      <div className="row end">
        <button type="button" onClick={onClose}>
          Cancel
        </button>
        <button className="primary">{kind === "channel" ? "Create" : "Open"}</button>
      </div>
    </form>
  );
}

function Conversation({
  channel,
  me,
  names,
  messages,
  onSend,
  onUpload,
  onToggleMute,
  onRetry,
  typing,
  onTyping,
  onReact,
  onOpenThread,
  presenceOf,
}: {
  channel: Channel;
  me: string;
  names: Directory;
  messages: Message[];
  onSend: (body: string, files: Attachment[]) => void;
  onUpload: (file: File) => Promise<Attachment>;
  onToggleMute: () => void;
  onRetry: (key: string) => void;
  typing: string[];
  onTyping: () => void;
  onReact: (msgSeq: number, emoji: string, add: boolean) => void;
  onOpenThread: (root: Message) => void;
  presenceOf: (userId: string) => Presence;
}) {
  const [draft, setDraft] = useState("");
  const [note, setNote] = useState("");
  // Files picked for the next message (ADR-0024): uploaded at once, sent with it.
  const [files, setFiles] = useState<PendingFile[]>([]);
  const picker = useRef<HTMLInputElement>(null);
  // The message being edited: its key, the new text, and the edit_id reused on retries.
  const [editing, setEditing] = useState<{ key: string; text: string; editId: string } | null>(null);
  // Read receipts (DMs): "Seen" under my latest message the others have read.
  const isDm = channel.kind === "dm" || channel.kind === "group_dm";
  const lastMine = isDm ? [...messages].reverse().find((m) => m.authorId === me && m.msgSeq > 0) : undefined;
  const readers = lastMine ? Object.entries(channel.seenBy ?? {}).filter(([, seq]) => seq >= lastMine.msgSeq).length : 0;
  const lastSeenKey = lastMine?.key;
  const seenText = readers === 0 ? "" : channel.kind === "dm" ? "Seen" : `Seen by ${readers}`;
  const bottom = useRef<HTMLDivElement>(null);
  // Block bodies: an effect may only return a cleanup function, and scrollIntoView
  // returns a Promise in browsers with scroll promises.
  useEffect(() => {
    void bottom.current?.scrollIntoView({ block: "end" });
  }, [messages.length, channel.id]);
  useEffect(() => {
    setNote("");
    setEditing(null);
    setFiles([]);
  }, [channel.id]);

  const addFiles = (picked: FileList | null) => {
    for (const file of Array.from(picked ?? [])) {
      const id = crypto.randomUUID();
      if (file.size > MAX_FILE_BYTES) {
        setFiles((fs) => [...fs, { id, name: file.name, size: file.size, error: `over ${formatSize(MAX_FILE_BYTES)}` }]);
        continue;
      }
      setFiles((fs) => [...fs, { id, name: file.name, size: file.size }]);
      onUpload(file)
        .then((attachment) => setFiles((fs) => fs.map((f) => (f.id === id ? { ...f, attachment } : f))))
        .catch((err) => setFiles((fs) => fs.map((f) => (f.id === id ? { ...f, error: errorText(err) } : f))));
    }
  };
  const ready = files.flatMap((f) => (f.attachment ? [f.attachment] : []));
  const uploading = files.some((f) => !f.attachment && !f.error);

  // Edits and deletes are sequenced like messages; the result arrives as an event.
  const saveEdit = async (m: Message) => {
    if (!editing) return;
    const body = editing.text.trim();
    if (!body || body === m.body) return setEditing(null);
    try {
      await api.editMessage(await session.accessToken(), channel.id, m.msgSeq, body, editing.editId);
      setEditing(null);
    } catch (err) {
      setNote(errorText(err));
    }
  };

  const remove = async (m: Message) => {
    if (!confirm("Delete this message for everyone?")) return;
    try {
      await api.deleteMessage(await session.accessToken(), channel.id, m.msgSeq);
    } catch (err) {
      setNote(errorText(err));
    }
  };

  const submit = (e?: FormEvent) => {
    e?.preventDefault();
    const body = draft.trim();
    if ((!body && ready.length === 0) || uploading) return;
    onSend(toTokens(body, channel.members, me, names), ready);
    setDraft("");
    setFiles([]);
  };

  const membership = async (action: "invite" | "leave") => {
    setNote("");
    try {
      const token = await session.accessToken();
      if (action === "invite") {
        const user = prompt("User ID to invite")?.trim();
        if (!user) return;
        const r = await api.join(token, channel.id, user);
        setNote(r.status === "pending" ? "Invite sent; waiting for confirmation" : "Invited");
      } else {
        await api.remove(token, channel.id, me);
      }
    } catch (err) {
      setNote(errorText(err));
    }
  };

  const canManage = channel.kind !== "dm";

  return (
    <section className="conversation">
      <header className="conv-header">
        <div>
          <div className="strong">{channelTitle(channel, me, names)}</div>
          <div className="muted small" title={channel.members.map((m) => memberLabel(m, me, names, channel.roles)).join(", ")}>
            {channel.members.length} member{channel.members.length === 1 ? "" : "s"} · {channel.kind.replace("_", " ")} ·{" "}
            {memberList(channel.members, me, names, channel.roles)}
          </div>
        </div>
        <div className="row">
          <button onClick={onToggleMute} aria-pressed={channel.muted}>
            {channel.muted ? "Unmute" : "Mute"}
          </button>
          {canManage && (
            <>
              <button onClick={() => membership("invite")}>Invite</button>
              <button onClick={() => membership("leave")}>Leave</button>
            </>
          )}
        </div>
      </header>
      {note && <p className="note">{note}</p>}
      <ol className="messages">
        {messages.map((m) => (
          <li key={m.key} className={`message ${m.authorId === me ? "own" : ""} ${m.status}`}>
            <div className="meta">
              {m.authorId !== me && <PresenceDot status={presenceOf(m.authorId)} />}
              <span className="strong">{m.authorId === me ? "You" : names.label(m.authorId)}</span>
              <time className="muted small">{new Date(m.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</time>
              {m.status === "pending" && <span className="muted small">sending…</span>}
              {m.status === "sent" && <span className="muted small">sent</span>}
              {m.editedAt && !m.deleted && <span className="muted small" title={new Date(m.editedAt).toLocaleString()}>(edited)</span>}
              {m.authorId === me && m.msgSeq > 0 && !m.deleted && editing?.key !== m.key && (
                <span className="actions small">
                  <button className="link" onClick={() => setEditing({ key: m.key, text: m.body, editId: crypto.randomUUID() })}>
                    Edit
                  </button>
                  <button className="link" onClick={() => remove(m)}>
                    Delete
                  </button>
                </span>
              )}
            </div>
            {m.deleted ? (
              <div className="body muted">This message was deleted.</div>
            ) : editing?.key === m.key ? (
              <form
                className="edit"
                onSubmit={(e) => {
                  e.preventDefault();
                  void saveEdit(m);
                }}
              >
                <textarea
                  autoFocus
                  value={editing.text}
                  onChange={(e) => setEditing({ ...editing, text: e.target.value })}
                  onKeyDown={(e) => {
                    if (e.key === "Escape") setEditing(null);
                    if (e.key === "Enter" && !e.shiftKey) {
                      e.preventDefault();
                      void saveEdit(m);
                    }
                  }}
                  rows={2}
                />
                <div className="row small">
                  <button type="submit">Save</button>
                  <button type="button" className="link" onClick={() => setEditing(null)}>
                    Cancel
                  </button>
                </div>
              </form>
            ) : (
              <>
                {m.body && <div className="body">{renderBody(m.body, me, names)}</div>}
                {m.attachments && <Attachments files={m.attachments} />}
              </>
            )}
            {m.msgSeq > 0 && !m.deleted && messageIdOf(m) && (
              <button className="link small thread-link" onClick={() => onOpenThread(m)}>
                {m.replyCount ? `${m.replyCount} ${m.replyCount === 1 ? "reply" : "replies"}` : "Reply"}
              </button>
            )}
            {m.key === lastSeenKey && seenText && <div className="seen muted small">{seenText}</div>}
            {m.msgSeq > 0 && !m.deleted && <Reactions reactions={m.reactions ?? {}} onToggle={(emoji, add) => onReact(m.msgSeq, emoji, add)} />}
            {m.error && (
              <div className="error small">
                {m.error}
                {m.status === "failed" && (
                  <>
                    {" · "}
                    <button className="link" onClick={() => onRetry(m.key)}>
                      Retry
                    </button>
                  </>
                )}
              </div>
            )}
          </li>
        ))}
        <div ref={bottom} />
      </ol>
      <div className="typing muted small" aria-live="polite">
        {typing.length > 0 && typingText(typing.map((u) => names.label(u)))}
      </div>
      {!canPost(channel, me) ? (
        // Broadcast channels (plan §6.7): only posters and admins post; the rest read.
        <p className="composer-note muted small">Only posters and admins post in this broadcast channel.</p>
      ) : (
        <form
          className="composer"
          onSubmit={submit}
          onDragOver={(e) => e.preventDefault()}
          onDrop={(e) => {
            e.preventDefault();
            addFiles(e.dataTransfer.files);
          }}
        >
          <PendingFiles files={files} onRemove={(id) => setFiles((fs) => fs.filter((f) => f.id !== id))} />
          <input
            ref={picker}
            type="file"
            multiple
            hidden
            onChange={(e) => {
              addFiles(e.target.files);
              e.target.value = "";
            }}
          />
          <button type="button" onClick={() => picker.current?.click()} aria-label="Attach files" title="Attach files">
            📎
          </button>
          <textarea
            value={draft}
            onChange={(e) => {
              setDraft(e.target.value);
              if (e.target.value.trim()) onTyping();
            }}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) submit(e);
            }}
            placeholder={`Message ${channelTitle(channel, me, names)}`}
            rows={2}
            aria-label="Message"
          />
          <button className="primary" disabled={(!draft.trim() && ready.length === 0) || uploading}>
            {uploading ? "Uploading…" : "Send"}
          </button>
        </form>
      )}
    </section>
  );
}

/**
 * `@Name` of a channel member becomes `<@user_id>`, and `@channel` / `@here` become
 * `<!channel>` / `<!here>`: the tokens the sequencer turns into mentions (ADR-0012).
 * Longer names first, so "@Ann Lee" wins over "@Ann".
 */
function toTokens(body: string, members: string[], me: string, names: Directory): string {
  let out = body.replace(/@(channel|here)\b/g, "<!$1>");
  const people = members
    .filter((id) => id !== me)
    .map((id) => ({ id, name: names.label(id) }))
    .sort((a, b) => b.name.length - a.name.length);
  for (const { id, name } of people) {
    out = out.split(`@${name}`).join(`<@${id}>`);
  }
  return out;
}

/** Shows mention tokens as highlighted names; mentions of me stand out more. */
function renderBody(body: string, me: string, names: Directory): ReactNode[] {
  return body.split(/(<@\d+>|<!(?:channel|here)>)/).map((part, i) => {
    const user = /^<@(\d+)>$/.exec(part)?.[1];
    if (user) {
      return (
        <span key={i} className={`mention ${user === me ? "me" : ""}`}>
          @{user === me ? names.label(me) : names.label(user)}
        </span>
      );
    }
    const everyone = /^<!(channel|here)>$/.exec(part)?.[1];
    if (everyone) {
      return (
        <span key={i} className="mention me">
          @{everyone}
        </span>
      );
    }
    return part;
  });
}

/** "Ann is typing…", "Ann and Bo are typing…", "Several people are typing…". */
function typingText(who: string[]): string {
  if (who.length === 1) return `${who[0]} is typing…`;
  if (who.length === 2) return `${who[0]} and ${who[1]} are typing…`;
  return "Several people are typing…";
}

const QUICK_REACTIONS = ["👍", "❤️", "😂", "🎉", "👀"];

/** A message's reactions as toggles, plus a small picker for the common ones. */
function Reactions({
  reactions,
  onToggle,
}: {
  reactions: Record<string, { count: number; mine: boolean }>;
  onToggle: (emoji: string, add: boolean) => void;
}) {
  const [picking, setPicking] = useState(false);
  const shown = Object.entries(reactions);
  return (
    <div className="reactions">
      {shown.map(([emoji, r]) => (
        <button
          key={emoji}
          className={`reaction ${r.mine ? "mine" : ""}`}
          aria-pressed={r.mine}
          aria-label={`${emoji} ${r.count}${r.mine ? ", including you" : ""}`}
          onClick={() => onToggle(emoji, !r.mine)}
        >
          {emoji} <span className="small">{r.count}</span>
        </button>
      ))}
      {picking ? (
        QUICK_REACTIONS.filter((e) => !reactions[e]?.mine).map((emoji) => (
          <button
            key={emoji}
            className="reaction"
            onClick={() => {
              onToggle(emoji, true);
              setPicking(false);
            }}
          >
            {emoji}
          </button>
        ))
      ) : (
        <button className="reaction add" aria-label="Add a reaction" onClick={() => setPicking(true)}>
          +
        </button>
      )}
    </div>
  );
}

/** A sequenced message's message_id (needed to reply in its thread), if this client knows it. */
function messageIdOf(m: Message): string {
  return m.messageId ?? "";
}

/** A thread next to the conversation: the root, its replies and a composer. */
function ThreadPanel({
  root,
  replies,
  me,
  names,
  onSend,
  onRetry,
  onClose,
}: {
  root: Message;
  replies: Message[];
  me: string;
  names: Directory;
  onSend: (body: string) => void;
  onRetry: (key: string) => void;
  onClose: () => void;
}) {
  const [draft, setDraft] = useState("");
  const submit = (e?: FormEvent) => {
    e?.preventDefault();
    const body = draft.trim();
    if (!body) return;
    onSend(body);
    setDraft("");
  };
  const who = (id: string) => (id === me ? "You" : names.label(id));
  return (
    <aside className="thread" aria-label="Thread">
      <header className="conv-header">
        <div className="strong">Thread</div>
        <button className="link" onClick={onClose} aria-label="Close thread">
          Close
        </button>
      </header>
      <ol className="messages">
        <li className="message root">
          <div className="meta">
            <span className="strong">{who(root.authorId)}</span>
          </div>
          <div className="body">{root.deleted ? "This message was deleted." : renderBody(root.body, me, names)}</div>
        </li>
        {replies.map((r) => (
          <li key={r.key} className={`message ${r.authorId === me ? "own" : ""} ${r.status}`}>
            <div className="meta">
              <span className="strong">{who(r.authorId)}</span>
              {r.status === "pending" && <span className="muted small">sending…</span>}
            </div>
            <div className="body">{renderBody(r.body, me, names)}</div>
            {r.error && (
              <div className="error small">
                {r.error}
                {r.status === "failed" && (
                  <>
                    {" · "}
                    <button className="link" onClick={() => onRetry(r.key)}>
                      Retry
                    </button>
                  </>
                )}
              </div>
            )}
          </li>
        ))}
      </ol>
      <form className="composer" onSubmit={submit}>
        <textarea
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) submit(e);
          }}
          placeholder="Reply…"
          rows={2}
        />
        <button type="submit">Send</button>
      </form>
    </aside>
  );
}

/** Online (filled), away (hollow) or offline (faint), with the status for screen readers. */
function PresenceDot({ status }: { status: Presence }) {
  return <span className={`presence ${status}`} role="img" aria-label={status} title={status} />;
}

/** Search results: where each hit is, who wrote it, and the text. */
function SearchResults({
  hits,
  note,
  channelName,
  names,
  me,
  onPick,
  onClose,
}: {
  hits: SearchHit[];
  note: string;
  channelName: (id: string) => string;
  names: Directory;
  me: string;
  onPick: (hit: SearchHit) => void;
  onClose: () => void;
}) {
  return (
    <section className="conversation">
      <header className="conv-header">
        <div className="strong">
          {hits.length} result{hits.length === 1 ? "" : "s"}
        </div>
        <button className="link" onClick={onClose}>
          Close
        </button>
      </header>
      {note && <p className="note">{note}</p>}
      <ol className="messages">
        {hits.map((h) => (
          <li key={h.messageId} className="message">
            <button className="link hit" onClick={() => onPick(h)}>
              <div className="meta">
                <span className="strong">{h.authorId === me ? "You" : names.label(h.authorId)}</span>
                <span className="muted small">
                  in {channelName(h.channelId)}
                  {h.reply ? " · thread reply" : ""}
                </span>
              </div>
              <div className="body">{renderBody(h.body, me, names)}</div>
            </button>
          </li>
        ))}
      </ol>
    </section>
  );
}

/** Browser notifications for new messages (Web Push): off until the user turns them on. */
function NotificationsToggle() {
  const [state, setState] = useState<PushState | "busy">("busy");
  const [note, setNote] = useState("");
  useEffect(() => {
    void pushState().then(setState).catch(() => setState("unsupported"));
  }, []);
  if (state === "unsupported") return null;
  const toggle = async () => {
    setNote("");
    setState("busy");
    try {
      setState(state === "on" ? await disablePush() : await enablePush());
    } catch (err) {
      setState("off");
      setNote(errorText(err));
    }
  };
  return (
    <div className="notifications">
      {state === "blocked" ? (
        <span className="muted small">Notifications are blocked in this browser's settings</span>
      ) : (
        <button className="link" disabled={state === "busy"} onClick={() => void toggle()}>
          {state === "on" ? "Turn off notifications" : "Enable notifications"}
        </button>
      )}
      {note && <div className="error small">{note}</div>}
    </div>
  );
}
