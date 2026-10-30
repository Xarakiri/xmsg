// SPDX-License-Identifier: Apache-2.0
import { useEffect, useState, type FormEvent, type ReactNode } from "react";
import { canPost, mentionCount, unread, type Channel, type ConnectionStatus, type Directory, type Message, type Presence, type SearchHit, type Workspace } from "@xmsg/client-core";
import { AttachButton, Attachments, PendingFiles, useFileDrafts } from "./files.tsx";
import { pushState, enablePush, disablePush, type PushState } from "./push.ts";

export type Theme = "dark" | "light";

export function ThemeToggle({ theme, onToggle }: { theme: Theme; onToggle: () => void }) {
  return <button className="link status-theme" onClick={onToggle} title="Toggle colour theme">[{theme === "dark" ? "light" : "dark"}]</button>;
}

export function StatusLine({ workspace, channel, status, theme, onToggle }: { workspace: string; channel: string; status: ConnectionStatus | "offline"; theme: Theme; onToggle: () => void }) {
  return <header className="status-line"><span>xmsg v0.9 <b>·</b> workspace:{workspace || "default"} <b>·</b> {channel || "not connected"} <b>·</b> {location.host}</span><span className={`connection ${status}`}><i />{status === "online" ? "connected" : status === "offline" ? "offline" : status}</span><ThemeToggle theme={theme} onToggle={onToggle} /></header>;
}

const AVATAR_COLORS = ["violet", "amber", "pink", "green", "blue", "cyan"];
export function Avatar({ id, name, url, presence, size = "normal" }: { id: string; name: string; url?: string | null; presence?: Presence; size?: "small" | "normal" | "large" }) {
  let hash = 0;
  for (const char of id) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  const initials = name.split(/\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]).join("").toUpperCase() || "?";
  return <span className={`avatar ${size} ${AVATAR_COLORS[hash % AVATAR_COLORS.length]}`} title={name}>{url ? <img src={url} alt="" /> : <span>{initials}</span>}{presence && <i className={`avatar-presence ${presence}`} aria-label={presence} />}</span>;
}

export function KeyHints({ inCall = false }: { inCall?: boolean }) {
  return <footer className="key-hints"><span>^K search</span><span>^T thread</span><span>^N new channel</span><span>/huddle huddle</span>{inCall && <><span>m mute</span><span>v video</span><span>s share</span><span>q leave</span></>}<span>esc close</span></footer>;
}

export interface SidebarChannel { channel: Channel; title: string; presence?: Presence }
export function Sidebar({ rooms, dms, calls, selected, onSelect, onNewChannel, onNewDm, onWorkspaces, onProfile, onSignOut }: { rooms: SidebarChannel[]; dms: SidebarChannel[]; calls: SidebarChannel[]; selected: string | null; onSelect: (id: string) => void; onNewChannel: () => void; onNewDm: () => void; onWorkspaces: () => void; onProfile: () => void; onSignOut: () => void }) {
  return <aside className="sidebar"><SidebarSection title="CHANNELS" onAdd={onNewChannel} items={rooms} selected={selected} onSelect={onSelect} /><SidebarSection title="DIRECT" onAdd={onNewDm} items={dms} selected={selected} onSelect={onSelect} /><SidebarSection title="VOICE" items={calls} selected={selected} onSelect={onSelect} /><div className="sidebar-tools"><button className="link" onClick={onWorkspaces}>[ workspaces / invites ]</button><button className="link" onClick={onProfile}>[ profile / settings ]</button><NotificationsToggle /><button className="link" onClick={onSignOut}>[ sign out ]</button></div></aside>;
}
function SidebarSection({ title, items, selected, onSelect, onAdd }: { title: string; items: SidebarChannel[]; selected: string | null; onSelect: (id: string) => void; onAdd?: () => void }) {
  return <section className="sidebar-section"><h2>{title}{onAdd && <button className="link" onClick={onAdd} aria-label={`New ${title.toLowerCase()}`}>+</button>}</h2><ul>{items.map(({ channel, title: label, presence }) => { const count = channel.muted ? 0 : unread(channel); const mentions = mentionCount(channel); return <li key={channel.id}><button className={`channel-item ${selected === channel.id ? "selected" : ""} ${channel.muted ? "muted" : ""}`} onClick={() => onSelect(channel.id)}><span className="selection">{selected === channel.id ? "▸" : " "}</span>{presence && <span className={`presence ${presence}`} />}<span className="channel-label">{label}</span>{mentions > 0 && selected !== channel.id ? <span className="unread">[@{mentions}]</span> : count > 0 && selected !== channel.id ? <span className="unread">[{count}]</span> : null}{title === "VOICE" && <span className="muted">({channel.call?.participants.length ?? 0})</span>}</button></li>; })}</ul></section>;
}

export function channelTitle(c: Channel, me: string, names: Directory): string {
  if (c.name) return c.kind === "public" || c.kind === "private" || c.kind === "broadcast" ? `#${c.name}` : c.name;
  const others = c.members.filter((m) => m !== me);
  if (c.kind === "dm" || c.kind === "group_dm") return others.length ? `@${others.map((m) => names.label(m)).join(", ")}` : "@direct";
  return `#…${c.id.slice(-6)}`;
}
export function messageIdOf(m: Message): string { return m.messageId ?? ""; }
export function renderBody(body: string, me: string, names: Directory): ReactNode[] {
  return body.split(/(<@\d+>|<!(?:channel|here)>)/).map((part, i) => {
    const user = /^<@(\d+)>$/.exec(part)?.[1];
    if (user) return <span key={i} className={`mention ${user === me ? "me" : ""}`}>@{names.label(user)}</span>;
    const everyone = /^<!(channel|here)>$/.exec(part)?.[1];
    return everyone ? <span key={i} className="mention me">@{everyone}</span> : part;
  });
}
export function toTokens(body: string, members: string[], me: string, names: Directory): string {
  let out = body.replace(/@(channel|here)\b/g, "<!$1>");
  const people = members.filter((id) => id !== me).map((id) => ({ id, name: names.label(id) })).sort((a, b) => b.name.length - a.name.length);
  for (const { id, name } of people) out = out.split(`@${name}`).join(`<@${id}>`);
  return out;
}
export function errorText(e: unknown): string {
  return e instanceof Error && "status" in e ? (typeof e.status === "number" && e.status >= 500 ? "Service unavailable, try again" : e.message) : "Can't reach the server";
}

export function MessageRow({ message: m, me, names, presence, onOpenThread, onReact, onEdit, onDelete, onRetry, compact = false }: { message: Message; me: string; names: Directory; presence?: Presence; onOpenThread?: () => void; onReact?: (emoji: string, add: boolean) => void; onEdit?: (text: string) => Promise<void>; onDelete?: () => void; onRetry?: () => void; compact?: boolean }) {
  const [editing, setEditing] = useState(false);
  const [picking, setPicking] = useState(false);
  const [text, setText] = useState(m.body);
  const name = m.authorId === me ? "you" : names.label(m.authorId);
  const profile = names.profile(m.authorId);
  const color = AVATAR_COLORS[[...m.authorId].reduce((n, c) => (n * 31 + c.charCodeAt(0)) >>> 0, 0) % AVATAR_COLORS.length];
  const actionable = m.msgSeq > 0 && !m.deleted;
  return <li className={`message-row ${m.status} ${compact ? "compact" : ""}`}><Avatar id={m.authorId} name={name} url={profile?.avatar_url} presence={presence} size="small" /><time>{new Date(m.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</time><span className={`author ${color}`} title={name}>{name}</span><div className="message-content">{actionable && <span className="message-actions">{onOpenThread && !m.replyCount && messageIdOf(m) && <button className="link" onClick={onOpenThread}>reply</button>}{onReact && <button className="link" aria-label="Add reaction" onClick={() => setPicking(!picking)}>+</button>}{onEdit && <button className="link" onClick={() => setEditing(true)}>edit</button>}{onDelete && <button className="link" onClick={onDelete}>delete</button>}</span>}{m.deleted ? <div className="muted">[deleted]</div> : editing ? <form className="edit-row" onSubmit={(e) => { e.preventDefault(); void onEdit?.(text).then(() => setEditing(false)); }}><input value={text} onChange={(e) => setText(e.target.value)} autoFocus /><button>save</button><button type="button" className="link" onClick={() => setEditing(false)}>cancel</button></form> : <>{m.body && <div className="message-body">{renderBody(m.body, me, names)}{m.status === "pending" && <span className="muted"> · sending…</span>}{m.editedAt && <span className="muted"> (edited)</span>}</div>}{m.attachments && <Attachments files={m.attachments} />}</>}{picking && onReact && <div className="reactions reaction-picker">{["👍", "❤️", "😂", "🎉", "👀"].map((emoji) => <button className="reaction" key={emoji} onClick={() => { onReact(emoji, true); setPicking(false); }}>{emoji}</button>)}</div>}{onOpenThread && m.replyCount! > 0 && actionable && messageIdOf(m) && <button className="thread-link link" onClick={onOpenThread}>└─ ▣ thread #{m.msgSeq} ({m.replyCount} {m.replyCount === 1 ? "reply" : "replies"}) → panel</button>}{onReact && actionable && <Reactions reactions={m.reactions ?? {}} onToggle={onReact} />}{m.error && <div className="error">{m.error}{onRetry && <> · <button className="link" onClick={onRetry}>retry</button></>}</div>}</div></li>;
}
function Reactions({ reactions, onToggle }: { reactions: Record<string, { count: number; mine: boolean }>; onToggle: (emoji: string, add: boolean) => void }) {
  return <div className="reactions">{Object.entries(reactions).map(([emoji, r]) => <button key={emoji} className={`reaction ${r.mine ? "mine" : ""}`} onClick={() => onToggle(emoji, !r.mine)}>{emoji} {r.count}</button>)}</div>;
}

export function Prompt({ channel, members, me, names, onSend, onUpload, onTyping, onCommand, readOnly = false }: { channel: string; members: string[]; me: string; names: Directory; onSend: (body: string, files: import("@xmsg/client-core").Attachment[]) => void; onUpload: (file: File) => Promise<import("@xmsg/client-core").Attachment>; onTyping?: () => void; onCommand?: (command: string) => void; readOnly?: boolean }) {
  const [draft, setDraft] = useState("");
  const drafts = useFileDrafts(onUpload, errorText);
  const submit = (e?: FormEvent) => {
    e?.preventDefault();
    const body = draft.trim();
    if (body.startsWith("/") && onCommand) { onCommand(body); setDraft(""); return; }
    if ((!body && drafts.ready.length === 0) || drafts.uploading) return;
    onSend(toTokens(body, members, me, names), drafts.ready); setDraft(""); drafts.reset();
  };
  if (readOnly) return <p className="composer-note">Only posters and admins post in this broadcast channel.</p>;
  return <form className="prompt" onSubmit={submit} onDragOver={(e) => e.preventDefault()} onDrop={(e) => { e.preventDefault(); drafts.add(e.dataTransfer.files); }}><PendingFiles files={drafts.files} onRemove={drafts.remove} /><span className="prompt-label">{channel} <b>›</b></span><AttachButton onPick={drafts.add} /><textarea aria-label={`Message ${channel}`} value={draft} rows={1} placeholder="type a message or /huddle" onChange={(e) => { setDraft(e.target.value); if (e.target.value.trim()) onTyping?.(); }} onKeyDown={(e) => { if (e.key === "Enter" && !e.shiftKey) submit(e); }} /><span className="send-hints">↵ send&nbsp;&nbsp;⇧↵ newline</span><button className="prompt-send" disabled={(!draft.trim() && drafts.ready.length === 0) || drafts.uploading}>send</button></form>;
}

export function ConversationHeader({ channel, title, me, names, onCall, onMute, onInvite, onLeave }: { channel: Channel; title: string; me: string; names: Directory; onCall: () => void; onMute: () => void; onInvite?: () => void; onLeave?: () => void }) {
  return <header className="conversation-header"><div><strong>{title}</strong><span className="muted"> · {channel.members.length} members · {channel.kind.replace("_", " ")}</span></div><div className="header-actions"><button className="link" onClick={onCall}>{channel.call ? `join ${channel.call.huddle ? "huddle" : "call"} (${channel.call.participants.length})` : channel.kind === "dm" ? "call" : "huddle"}</button><button className="link" onClick={onMute}>{channel.muted ? "unmute" : "mute"}</button>{onInvite && <button className="link" onClick={onInvite}>invite</button>}{onLeave && <button className="link" onClick={onLeave}>leave</button>}</div></header>;
}

export function ThreadPanel({ root, replies, me, names, channel, onSend, onUpload, onRetry, onClose }: { root: Message; replies: Message[]; me: string; names: Directory; channel: string; onSend: (body: string, files: import("@xmsg/client-core").Attachment[]) => void; onUpload: (file: File) => Promise<import("@xmsg/client-core").Attachment>; onRetry: (key: string) => void; onClose: () => void }) {
  return <aside className="right-panel thread-panel"><header className="panel-title">THREAD #{root.msgSeq} · {channel.toUpperCase()}<button className="link" onClick={onClose}>×</button></header><ol className="message-list"><MessageRow message={root} me={me} names={names} compact />{replies.length > 0 && <li className="reply-divider">— {replies.length} replies —</li>}{replies.map((reply) => <MessageRow key={reply.key} message={reply} me={me} names={names} onRetry={() => onRetry(reply.key)} compact />)}</ol><Prompt channel={`#t${root.msgSeq}`} members={[]} me={me} names={names} onSend={onSend} onUpload={onUpload} /></aside>;
}

export function SearchBar({ query, setQuery, inputRef, onSubmit }: { query: string; setQuery: (q: string) => void; inputRef: React.RefObject<HTMLInputElement | null>; onSubmit: () => void }) {
  return <form className="search-bar" onSubmit={(e) => { e.preventDefault(); onSubmit(); }}><span>/search ›</span><input ref={inputRef} type="search" value={query} onChange={(e) => setQuery(e.target.value)} placeholder="search messages" aria-label="Search messages" /><button>run</button></form>;
}
export function SearchResults({ hits, note, me, names, channelName, onPick, onClose }: { hits: SearchHit[]; note: string; me: string; names: Directory; channelName: (id: string) => string; onPick: (hit: SearchHit) => void; onClose: () => void }) {
  return <section className="search-results"><header className="panel-title">SEARCH · {hits.length} RESULTS<button className="link" onClick={onClose}>×</button></header>{note && <p className="error">{note}</p>}<ol className="message-list">{hits.map((hit) => <li className="search-hit" key={hit.messageId}><button className="link" onClick={() => onPick(hit)}><span className="author">{hit.authorId === me ? "you" : names.label(hit.authorId)}</span><span className="muted"> {channelName(hit.channelId)}{hit.reply ? " · reply" : ""}</span><div>{renderBody(hit.body, me, names)}</div></button></li>)}</ol></section>;
}

export function AuthScreen({ mode, setMode, name, setName, email, setEmail, password, setPassword, passwordAgain, setPasswordAgain, code, setCode, error, busy, providers, onSubmit }: { mode: "in" | "up" | "code"; setMode: (mode: "in" | "up" | "code") => void; name: string; setName: (v: string) => void; email: string; setEmail: (v: string) => void; password: string; setPassword: (v: string) => void; passwordAgain: string; setPasswordAgain: (v: string) => void; code: string; setCode: (v: string) => void; error: string; busy: boolean; providers: { id: string; name: string; url: string }[]; onSubmit: (e: FormEvent) => void }) {
  return <main className="auth-shell"><section className="auth-terminal"><header><strong>xmsg · {mode === "up" ? "create account" : mode === "code" ? "recovery login" : "log in"}</strong><span>{location.host}</span></header><form onSubmit={onSubmit}>{mode === "up" && <label>name<input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} autoComplete="name" autoFocus /></label>}<label>email<input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required autoComplete="email" /></label>{mode === "code" ? <label>recovery code<input value={code} onChange={(e) => setCode(e.target.value)} required autoComplete="one-time-code" placeholder="xxxx-xxxx-xxxx-xxxx" /></label> : <><label>password<input type="password" value={password} onChange={(e) => setPassword(e.target.value)} required minLength={mode === "up" ? 10 : undefined} maxLength={mode === "up" ? 256 : undefined} autoComplete={mode === "up" ? "new-password" : "current-password"} /></label>{mode === "up" && <><label>password again<input type="password" value={passwordAgain} onChange={(e) => setPasswordAgain(e.target.value)} required minLength={10} maxLength={256} autoComplete="new-password" /></label><p className="password-rule">10–256 characters</p></>}</>}{error && <p className="error" role="alert">{error}</p>}<button className="auth-submit" disabled={busy}>[ {busy ? "working…" : mode === "up" ? "create account" : "connect"} ↵ ]</button></form><footer>{mode === "up" ? <button className="link" onClick={() => setMode("in")}>have an account? [l] log in</button> : <><button className="link" onClick={() => setMode("up")}>no account? [r] register</button><button className="link" onClick={() => setMode(mode === "code" ? "in" : "code")}>{mode === "code" ? "use password" : "use recovery code"}</button></>}</footer>{providers.length > 0 && <div className="sso-links">{providers.map((p) => <a key={p.id} href={p.url}>[ sso: {p.name} ]</a>)}</div>}</section><div className="auth-hints">tab next field · ↵ submit · esc cancel</div></main>;
}

export function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: ReactNode }) {
  return <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}><section className="modal"><header className="panel-title">{title}<button className="link" onClick={onClose}>×</button></header>{children}</section></div>;
}

export function ProfilePanel({ me, names, onSave, onClose }: { me: string; names: Directory; onSave: (name: string, avatar: string) => Promise<void>; onClose: () => void }) {
  const profile = names.profile(me);
  const [name, setName] = useState(profile?.name ?? names.label(me));
  const [avatar, setAvatar] = useState(profile?.avatar_url ?? "");
  const [error, setError] = useState("");
  return <Modal title="PROFILE / SETTINGS" onClose={onClose}><form className="modal-form" onSubmit={(e) => { e.preventDefault(); onSave(name.trim(), avatar.trim()).then(onClose).catch((err) => setError(errorText(err))); }}><div className="profile-preview"><Avatar id={me} name={name} url={avatar} size="large" /><span className="mono">{me}</span></div><label>display name<input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} /></label><label>avatar URL<input type="url" value={avatar} onChange={(e) => setAvatar(e.target.value)} placeholder="https://…" /></label>{error && <p className="error">{error}</p>}<button>[ save profile ]</button></form></Modal>;
}

export function WorkspacesPanel({ workspaces, onCreate, onInvite, onAccept, onClose }: { workspaces: Workspace[]; onCreate: (name: string, slug: string) => Promise<void>; onInvite: (workspaceId: string) => Promise<string>; onAccept: (code: string) => Promise<void>; onClose: () => void }) {
  const [name, setName] = useState(""); const [slug, setSlug] = useState(""); const [code, setCode] = useState(""); const [note, setNote] = useState("");
  return <Modal title="WORKSPACES / INVITES" onClose={onClose}><div className="modal-form"><ul className="workspace-list">{workspaces.map((w) => <li key={w.workspace_id}><span>{w.name} <span className="muted">:{w.slug} · {w.role}</span></span><button className="link" onClick={() => onInvite(w.workspace_id).then((value) => { setNote(`invite code: ${value}`); return navigator.clipboard?.writeText(value); }).catch((e) => setNote(errorText(e)))}>create invite</button></li>)}</ul>{note && <p className="note-text">{note}</p>}<form onSubmit={(e) => { e.preventDefault(); onAccept(code.trim()).then(() => setNote("Invite accepted")).catch((err) => setNote(errorText(err))); }}><label>accept invite code<input value={code} onChange={(e) => setCode(e.target.value)} required /></label><button>[ accept ]</button></form><form onSubmit={(e) => { e.preventDefault(); onCreate(name.trim(), slug.trim()).then(() => { setName(""); setSlug(""); setNote("Workspace created"); }).catch((err) => setNote(errorText(err))); }}><label>new workspace name<input value={name} onChange={(e) => setName(e.target.value)} required /></label><label>slug<input value={slug} onChange={(e) => setSlug(e.target.value)} required pattern="[a-z0-9-]+" /></label><button>[ create workspace ]</button></form></div></Modal>;
}

function NotificationsToggle() {
  const [state, setState] = useState<PushState | "busy">("busy"); const [note, setNote] = useState("");
  useEffect(() => { void pushState().then(setState).catch(() => setState("unsupported")); }, []);
  if (state === "unsupported") return null;
  const toggle = async () => { setState("busy"); try { setState(state === "on" ? await disablePush() : await enablePush()); } catch (e) { setState("off"); setNote(errorText(e)); } };
  return <div>{state === "blocked" ? <span className="muted">notifications blocked</span> : <button className="link" disabled={state === "busy"} onClick={() => void toggle()}>[ {state === "on" ? "disable" : "enable"} web push ]</button>}{note && <span className="error">{note}</span>}</div>;
}

export { canPost };
