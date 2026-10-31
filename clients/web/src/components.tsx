// SPDX-License-Identifier: Apache-2.0
import { useEffect, useRef, useState, type FormEvent, type ReactNode } from "react";
import { canPost, mentionCount, unread, type Channel, type ConnectionStatus, type Directory, type Message, type Presence, type SearchHit, type Workspace } from "@xmsg/client-core";
import { autocomplete } from "./chatlog.ts";
import { AttachButton, Attachments, PendingFiles, useFileDrafts } from "./files.tsx";
import { pushState, enablePush, disablePush, type PushState } from "./push.ts";
import { isDesktop } from "./session.ts";

export type Theme = "dark" | "light";

export function StatusLine({ workspace, channel, status }: { workspace: string; channel: string; status: ConnectionStatus | "offline" }) {
  const text = status === "online" ? "connected" : status === "connecting" ? "reconnecting…" : status === "offline" ? "offline · messages will send when connected" : status;
  return <header className="status-line"><span>xmsg v0.9 <b>·</b> {workspace ? <>workspace:{workspace} <b>·</b> </> : null}{channel || "not connected"} <b>·</b> {location.host}</span><span className={`connection ${status}`}><i />{text}</span></header>;
}

const AVATAR_COLORS = ["violet", "amber", "pink", "green", "blue", "cyan"];
export function Avatar({ id, name, url, presence, size = "normal" }: { id: string; name: string; url?: string | null; presence?: Presence; size?: "small" | "normal" | "large" }) {
  let hash = 0;
  for (const char of id) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  const initials = name.split(/\s+/).filter(Boolean).slice(0, 2).map((part) => part[0]).join("").toUpperCase() || "?";
  return <span className={`avatar ${size} ${AVATAR_COLORS[hash % AVATAR_COLORS.length]}`} title={name}>{url ? <img src={url} alt="" /> : <span>{initials}</span>}{presence && <i className={`avatar-presence ${presence}`} aria-label={presence} />}</span>;
}

export function KeyHints({ inCall = false }: { inCall?: boolean }) {
  return <footer className="key-hints">{inCall ? <><span>m mute</span><span>v video</span><span>s share</span><span>q leave</span><span>esc close</span></> : <><span>^K search</span><span>^T thread</span><span>^N new channel</span><span>/huddle huddle</span><span>esc close</span></>}</footer>;
}

export interface SidebarChannel { channel: Channel; title: string; presence?: Presence }
export function Sidebar({ rooms, dms, calls, selected, onSelect, onNewChannel, onNewDm, onAdmin, meName, onOpenSettings }: { rooms: SidebarChannel[]; dms: SidebarChannel[]; calls: SidebarChannel[]; selected: string | null; onSelect: (id: string) => void; onNewChannel: () => void; onNewDm: () => void; onAdmin?: () => void; meName: string; onOpenSettings: () => void }) {
  return <aside className="sidebar"><SidebarSection title="CHANNELS" onAdd={onNewChannel} items={rooms} selected={selected} onSelect={onSelect} /><SidebarSection title="DIRECT" onAdd={onNewDm} items={dms} selected={selected} onSelect={onSelect} />{calls.length > 0 && <SidebarSection title="VOICE" items={calls} selected={selected} onSelect={onSelect} />}<div className="sidebar-tools">{onAdmin && <button className="link" onClick={onAdmin}>[ admin ]</button>}<button className="link sidebar-me" onClick={onOpenSettings} title="settings">{meName}</button></div></aside>;
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
  return e instanceof Error && "status" in e ? (typeof e.status === "number" && e.status >= 500 ? "Service unavailable, try again" : e.status === 403 ? "not allowed" : e.message) : "Can't reach the server";
}

export function MessageRow({ message: m, me, names, presence, onOpenThread, onReact, onEdit, onDelete, onRetry, compact = false, start = true, editRequest }: { message: Message; me: string; names: Directory; presence?: Presence; onOpenThread?: () => void; onReact?: (emoji: string, add: boolean) => void; onEdit?: (text: string) => Promise<void>; onDelete?: () => void; onRetry?: () => void; compact?: boolean; start?: boolean; editRequest?: number }) {
  const [editing, setEditing] = useState(false);
  const [picking, setPicking] = useState(false);
  const [text, setText] = useState(m.body);
  useEffect(() => { if (editRequest) setEditing(true); }, [editRequest]);
  const name = m.authorId === me ? "you" : names.label(m.authorId);
  const profile = names.profile(m.authorId);
  const color = AVATAR_COLORS[[...m.authorId].reduce((n, c) => (n * 31 + c.charCodeAt(0)) >>> 0, 0) % AVATAR_COLORS.length];
  const actionable = m.msgSeq > 0 && !m.deleted;
  const head = start ? <><Avatar id={m.authorId} name={name} url={profile?.avatar_url} presence={presence} size="small" /><time>{new Date(m.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</time><span className={`author ${color}`} title={name}>{name}</span></> : <><span aria-hidden="true" /><time className="quiet">{new Date(m.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}</time><span aria-hidden="true" /></>;
  return <li className={`message-row ${m.status} ${compact ? "compact" : ""} ${start ? "" : "grouped"}`}>{head}<div className="message-content">{actionable && <span className="message-actions">{onOpenThread && !m.replyCount && messageIdOf(m) && <button className="link" onClick={onOpenThread}>reply</button>}{onReact && <button className="link" aria-label="Add reaction" onClick={() => setPicking(!picking)}>+</button>}{onEdit && <button className="link" onClick={() => setEditing(true)}>edit</button>}{onDelete && <button className="link" onClick={onDelete}>delete</button>}</span>}{m.deleted ? <div className="muted">[deleted]</div> : editing ? <form className="edit-row" onSubmit={(e) => { e.preventDefault(); void onEdit?.(text).then(() => setEditing(false)); }}><input value={text} onChange={(e) => setText(e.target.value)} autoFocus /><button>save</button><button type="button" className="link" onClick={() => setEditing(false)}>cancel</button></form> : <>{m.body && <div className="message-body">{renderBody(m.body, me, names)}{m.status === "pending" && <span className="muted"> · sending…</span>}{m.editedAt && <span className="muted"> (edited)</span>}</div>}{m.attachments && <Attachments files={m.attachments} />}</>}{picking && onReact && <div className="reactions reaction-picker">{["👍", "❤️", "😂", "🎉", "👀"].map((emoji) => <button className="reaction" key={emoji} onClick={() => { onReact(emoji, true); setPicking(false); }}>{emoji}</button>)}</div>}{onOpenThread && m.replyCount! > 0 && actionable && messageIdOf(m) && <button className="thread-link link" onClick={onOpenThread}>└─ ▣ thread #{m.msgSeq} ({m.replyCount} {m.replyCount === 1 ? "reply" : "replies"}) → panel</button>}{onReact && actionable && <Reactions reactions={m.reactions ?? {}} onToggle={onReact} />}{m.error && <div className="error">{m.error}{onRetry && <> · <button className="link" onClick={onRetry}>retry</button></>}</div>}</div></li>;
}
function Reactions({ reactions, onToggle }: { reactions: Record<string, { count: number; mine: boolean }>; onToggle: (emoji: string, add: boolean) => void }) {
  return <div className="reactions">{Object.entries(reactions).map(([emoji, r]) => <button key={emoji} className={`reaction ${r.mine ? "mine" : ""}`} onClick={() => onToggle(emoji, !r.mine)}>{emoji} {r.count}</button>)}</div>;
}

export function Prompt({ channel, members, me, names, draftKey, membersList = [], channelsList = [], commands = [], onSend, onUpload, onTyping, onCommand, onEditLast, readOnly = false }: { channel: string; members: string[]; me: string; names: Directory; draftKey: string; membersList?: { id: string; name: string }[]; channelsList?: { id: string; name: string }[]; commands?: { name: string; hint: string }[]; onSend: (body: string, files: import("@xmsg/client-core").Attachment[]) => void; onUpload: (file: File) => Promise<import("@xmsg/client-core").Attachment>; onTyping?: () => void; onCommand?: (command: string) => void; onEditLast?: () => void; readOnly?: boolean }) {
  const [draft, setDraft] = useState(() => safeGet(draftKey) ?? "");
  const [ac, setAc] = useState<{ trigger: "@" | "#" | "/"; query: string; from: number } | null>(null);
  const [acIndex, setAcIndex] = useState(0);
  const area = useRef<HTMLTextAreaElement>(null);
  const drafts = useFileDrafts(onUpload, errorText);
  useEffect(() => {
    setDraft(safeGet(draftKey) ?? "");
    setAc(null);
  }, [draftKey]);
  useEffect(() => { safeSet(draftKey, draft); }, [draftKey, draft]);
  const items = ac ? autocomplete(ac.trigger, ac.query, { members: membersList, channels: channelsList, commands }).slice(0, 8) : [];
  const submit = (e?: FormEvent) => {
    e?.preventDefault();
    const body = draft.trim();
    if (body.startsWith("/") && onCommand) { onCommand(body); setDraft(""); setAc(null); return; }
    if ((!body && drafts.ready.length === 0) || drafts.uploading) return;
    onSend(toTokens(body, members, me, names), drafts.ready); setDraft(""); setAc(null); drafts.reset();
  };
  const onChange = (value: string) => {
    setDraft(value);
    if (value.trim()) onTyping?.();
    const caret = area.current?.selectionStart ?? value.length;
    const found = detectTrigger(value, caret);
    setAc(found);
    setAcIndex(0);
  };
  const onKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (ac && items.length > 0) {
      if (e.key === "ArrowDown") { e.preventDefault(); setAcIndex((i) => (i + 1) % items.length); return; }
      if (e.key === "ArrowUp") { e.preventDefault(); setAcIndex((i) => (i - 1 + items.length) % items.length); return; }
      if (e.key === "Tab" || e.key === "Enter") { e.preventDefault(); pick(items[Math.min(acIndex, items.length - 1)]!); return; }
      if (e.key === "Escape") { e.preventDefault(); setAc(null); return; }
    }
    if (e.key === "Enter" && !e.shiftKey) submit(e);
    if (e.key === "ArrowUp" && draft === "" && onEditLast) { e.preventDefault(); onEditLast(); }
  };
  const pick = (item: import("./chatlog.ts").AutocompleteItem) => {
    if (!ac) return;
    const caret = area.current?.selectionStart ?? draft.length;
    setDraft(draft.slice(0, ac.from) + item.insert + draft.slice(caret));
    setAc(null);
    area.current?.focus();
  };
  const rows = Math.min(8, Math.max(1, draft.split("\n").length));
  if (readOnly) return <p className="composer-note">Only posters and admins post in this broadcast channel.</p>;
  return <form className="prompt" onSubmit={submit} onDragOver={(e) => e.preventDefault()} onDrop={(e) => { e.preventDefault(); drafts.add(e.dataTransfer.files); }}><PendingFiles files={drafts.files} onRemove={drafts.remove} /><span className="prompt-label">{channel} <b>›</b></span><AttachButton onPick={drafts.add} /><div className="composer-area">{ac && items.length > 0 && <ul className="autocomplete" role="listbox">{items.map((item, i) => <li key={item.label + i}><button type="button" className={i === acIndex ? "active" : ""} onMouseDown={(e) => { e.preventDefault(); pick(item); }}>{item.label}{item.hint && <span className="muted"> {item.hint}</span>}</button></li>)}</ul>}<textarea ref={area} aria-label={`Message ${channel}`} value={draft} rows={rows} placeholder="type a message or /huddle" onChange={(e) => onChange(e.target.value)} onKeyDown={onKeyDown} onPaste={(e) => { if (e.clipboardData.files.length > 0) { e.preventDefault(); drafts.add(e.clipboardData.files); } }} /></div><span className="send-hints">↵ send&nbsp;&nbsp;⇧↵ newline</span></form>;
}

/** The trigger token being typed at the caret: `@`, `#` or `/` plus the partial word. */
export function detectTrigger(value: string, caret: number): { trigger: "@" | "#" | "/"; query: string; from: number } | null {
  const before = value.slice(0, caret);
  const m = /(^|\s)([@#/])([^\s@#/]*)$/.exec(before);
  if (!m) return null;
  return { trigger: m[2] as "@" | "#" | "/", query: m[3]!, from: caret - m[3]!.length - 1 };
}

function safeGet(key: string): string | undefined {
  try { return localStorage.getItem(key) ?? undefined; } catch { return undefined; }
}
function safeSet(key: string, value: string): void {
  try { value ? localStorage.setItem(key, value) : localStorage.removeItem(key); } catch { /* storage unavailable: drafts stay in memory */ }
}

export function ConversationHeader({ title, onCall, onOpenInfo }: { title: string; onCall: () => void; onOpenInfo: () => void }) {
  return <header className="conversation-header"><button className="link header-title" onClick={onOpenInfo} title="channel info"><strong>{title}</strong></button><div className="header-actions"><button className="link" onClick={onCall}>huddle</button></div></header>;
}

/** Mute, invite and leave, behind the channel name (ADR-0026 interface cleanup). */
export function ChannelInfoPanel({ channel, title, me, names, onMute, onInvite, onLeave, onClose }: { channel: Channel; title: string; me: string; names: Directory; onMute: () => void; onInvite?: () => void; onLeave?: () => void; onClose: () => void }) {
  return <Modal title={title.toUpperCase()} onClose={onClose}><div className="modal-form channel-info"><p className="muted">{channel.members.length} members</p><ul className="member-list">{channel.members.map((id) => <li key={id}><Avatar id={id} name={id === me ? "you" : names.label(id)} url={names.profile(id)?.avatar_url} size="small" />{id === me ? "you" : names.label(id)}</li>)}</ul><div className="info-actions"><button className="link" onClick={() => { onMute(); onClose(); }}>{channel.muted ? "unmute" : "mute"}</button>{onInvite && <button className="link" onClick={() => { onInvite(); onClose(); }}>invite people</button>}{onLeave && <button className="link" onClick={() => { onLeave(); onClose(); }}>leave channel</button>}</div></div></Modal>;
}

export function ThreadPanel({ root, replies, me, names, channel, draftKey, membersList, channelsList, commands, onSend, onUpload, onRetry, onClose }: { root: Message; replies: Message[]; me: string; names: Directory; channel: string; draftKey: string; membersList?: { id: string; name: string }[]; channelsList?: { id: string; name: string }[]; commands?: { name: string; hint: string }[]; onSend: (body: string, files: import("@xmsg/client-core").Attachment[]) => void; onUpload: (file: File) => Promise<import("@xmsg/client-core").Attachment>; onRetry: (key: string) => void; onClose: () => void }) {
  return <aside className="right-panel thread-panel"><header className="panel-title">THREAD #{root.msgSeq} · {channel.toUpperCase()}<button className="link" onClick={onClose}>×</button></header><ol className="message-list"><MessageRow message={root} me={me} names={names} compact />{replies.length > 0 && <li className="reply-divider">— {replies.length} replies —</li>}{replies.map((reply) => <MessageRow key={reply.key} message={reply} me={me} names={names} onRetry={() => onRetry(reply.key)} compact />)}</ol><Prompt channel={`#t${root.msgSeq}`} members={[]} me={me} names={names} draftKey={draftKey} membersList={membersList} channelsList={channelsList} commands={commands} onSend={onSend} onUpload={onUpload} /></aside>;
}

export function SearchResults({ hits, note, me, names, channelName, onPick, onClose }: { hits: SearchHit[]; note: string; me: string; names: Directory; channelName: (id: string) => string; onPick: (hit: SearchHit) => void; onClose: () => void }) {
  return <section className="search-results"><header className="panel-title">SEARCH · {hits.length} RESULTS<button className="link" onClick={onClose}>×</button></header>{note && <p className="error">{note}</p>}<ol className="message-list">{hits.map((hit) => <li className="search-hit" key={hit.messageId}><button className="link" onClick={() => onPick(hit)}><span className="author">{hit.authorId === me ? "you" : names.label(hit.authorId)}</span><span className="muted"> {channelName(hit.channelId)}{hit.reply ? " · reply" : ""}</span><div>{renderBody(hit.body, me, names)}</div></button></li>)}</ol></section>;
}

export function AuthScreen({ mode, setMode, name, setName, email, setEmail, password, setPassword, passwordAgain, setPasswordAgain, code, setCode, error, busy, providers, joining, note, title, canRegister = true, locked = false, onSubmit }: { mode: "in" | "up" | "code" | "reset"; setMode: (mode: "in" | "up" | "code" | "reset") => void; name: string; setName: (v: string) => void; email: string; setEmail: (v: string) => void; password: string; setPassword: (v: string) => void; passwordAgain: string; setPasswordAgain: (v: string) => void; code: string; setCode: (v: string) => void; error: string; busy: boolean; providers: { id: string; name: string; url: string }[]; joining?: string; note?: string; title?: string; canRegister?: boolean; locked?: boolean; onSubmit: (e: FormEvent) => void }) {
  const heading = mode === "up" ? (title ?? "create account") : mode === "code" ? "recovery login" : mode === "reset" ? "reset password" : "log in";
  return <main className="auth-shell"><section className="auth-terminal"><header><strong>xmsg · {heading}</strong><span>{location.host}</span></header>{joining && <p className="joining-note">joining {joining}</p>}{note && <p className="joining-note">{note}</p>}<form onSubmit={onSubmit}>{mode === "up" && <label>name<input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} autoComplete="name" autoFocus /></label>}<label>email<input type="email" value={email} onChange={(e) => setEmail(e.target.value)} required autoComplete="email" autoFocus={mode === "in" || mode === "reset"} /></label>{mode === "code" || mode === "reset" ? <label>{mode === "reset" ? "reset code" : "recovery code"}<input value={code} onChange={(e) => setCode(e.target.value)} required autoComplete="one-time-code" placeholder="xxxx-xxxx-xxxx-xxxx" /></label> : null}{mode !== "code" && <><label>{mode === "reset" ? "new password" : "password"}<input type="password" value={password} onChange={(e) => setPassword(e.target.value)} required minLength={mode === "up" || mode === "reset" ? 10 : undefined} maxLength={mode === "up" || mode === "reset" ? 256 : undefined} autoComplete={mode === "in" ? "current-password" : "new-password"} /></label>{(mode === "up" || mode === "reset") && <><label>password again<input type="password" value={passwordAgain} onChange={(e) => setPasswordAgain(e.target.value)} required minLength={10} maxLength={256} autoComplete="new-password" /></label><p className="password-rule">10–256 characters</p></>}</>}{error && <p className="error" role="alert">{error}</p>}<button className="auth-submit" disabled={busy || locked}>[ {busy ? "working…" : mode === "up" ? "create account" : mode === "reset" ? "reset password" : "connect"} ↵ ]</button></form><footer>{mode === "reset" ? <button className="link" onClick={() => setMode("in")}>back to log in</button> : mode === "up" ? <button className="link" onClick={() => setMode("in")}>have an account? log in</button> : <>{canRegister && <button className="link" onClick={() => setMode("up")}>no account? register</button>}<button className="link" onClick={() => setMode("reset")}>forgot password?</button><button className="link" onClick={() => setMode(mode === "code" ? "in" : "code")}>{mode === "code" ? "use password" : "use recovery code"}</button></>}</footer>{providers.length > 0 && mode !== "reset" && <div className="sso-links">{providers.map((p) => <a key={p.id} href={p.url}>[ sso: {p.name} ]</a>)}</div>}</section><div className="auth-hints">tab next field · ↵ submit</div></main>;
}

/** Ready-made invite link with copy; used after setup and in empty channels. */
export function InviteLinkAction({ onCreate, onLater }: { onCreate: () => Promise<string>; onLater?: () => void }) {
  const [link, setLink] = useState(""); const [copied, setCopied] = useState(false); const [error, setError] = useState(""); const [busy, setBusy] = useState(false);
  const create = () => {
    setBusy(true); setError("");
    onCreate().then((value) => { setLink(value); setCopied(false); }).catch((err) => setError(errorText(err))).finally(() => setBusy(false));
  };
  if (!link) return <span className="invite-link-action"><button className="link" disabled={busy} onClick={create}>[ invite link ]</button>{onLater && <button className="link" onClick={onLater}>[ later ]</button>}{error && <span className="error"> {error}</span>}</span>;
  return <span className="invite-link-action">{link} <button className="link" onClick={() => { void navigator.clipboard?.writeText(link).then(() => setCopied(true)); }}>[ copy ]</button>{copied && <span className="muted"> copied</span>}{onLater && <button className="link" onClick={onLater}>[ later ]</button>}</span>;
}

/** First run, owner/admin only (ADR-0026): name the workspace, create its first channel. Not skippable. */
export function SetupScreen({ workspaceName, setWorkspaceName, channelName, setChannelName, error, busy, onSubmit }: { workspaceName: string; setWorkspaceName: (v: string) => void; channelName: string; setChannelName: (v: string) => void; error: string; busy: boolean; onSubmit: (e: FormEvent) => void }) {
  return <main className="auth-shell"><section className="auth-terminal"><header><strong>xmsg · set up workspace</strong><span>{location.host}</span></header><form onSubmit={onSubmit}><label>workspace name<input value={workspaceName} onChange={(e) => setWorkspaceName(e.target.value)} required maxLength={80} autoFocus /></label><label>first channel<input value={channelName} onChange={(e) => setChannelName(e.target.value)} required maxLength={80} /></label>{error && <p className="error" role="alert">{error}</p>}<button className="auth-submit" disabled={busy}>[ {busy ? "working…" : "create workspace"} ⏎ ]</button></form><div className="auth-hints">↵ create · this can't be skipped</div></section></main>;
}

/** Signed in, nothing to do yet: someone else owns the workspace and hasn't finished setup. */
export function WaitingScreen() {
  return <main className="auth-shell"><div className="waiting-state"><p className="muted">waiting for the workspace owner to finish setup</p></div></main>;
}

export function Modal({ title, onClose, children }: { title: string; onClose: () => void; children: ReactNode }) {
  return <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}><section className="modal"><header className="panel-title">{title}<button className="link" onClick={onClose}>×</button></header>{children}</section></div>;
}

function NotificationsToggle() {
  const [state, setState] = useState<PushState | "busy">("busy"); const [note, setNote] = useState("");
  useEffect(() => { void pushState().then(setState).catch(() => setState("unsupported")); }, []);
  if (isDesktop || state === "unsupported" || state === "blocked") return null;
  const toggle = async () => { setState("busy"); try { setState(state === "on" ? await disablePush() : await enablePush()); } catch (e) { setState("off"); setNote(errorText(e)); } };
  return <div>{<button className="link" disabled={state === "busy"} onClick={() => void toggle()}>[ {state === "on" ? "disable" : "enable"} web push ]</button>}{note && <span className="error">{note}</span>}</div>;
}

/** Profile, notifications, theme, server (desktop), workspaces and sign out — one panel. */
export function SettingsPanel({ me, names, theme, onToggleTheme, workspaces, currentWorkspaceId, serverInitial, onSaveProfile, onSaveServer, onAcceptInvite, onSignOut, onClose }: { me: string; names: Directory; theme: Theme; onToggleTheme: () => void; workspaces: Workspace[]; currentWorkspaceId: string; serverInitial?: string; onSaveProfile: (name: string, avatar: string) => Promise<void>; onSaveServer?: (url: string) => void; onAcceptInvite: (code: string) => Promise<void>; onSignOut: () => void; onClose: () => void }) {
  const profile = names.profile(me);
  const [name, setName] = useState(profile?.name ?? names.label(me));
  const [avatar, setAvatar] = useState(profile?.avatar_url ?? "");
  const [server, setServer] = useState(serverInitial ?? "");
  const [code, setCode] = useState("");
  const [note, setNote] = useState("");
  const [error, setError] = useState("");
  const multiple = workspaces.length > 1;
  return <Modal title="SETTINGS" onClose={onClose}><div className="modal-form settings-panel">
    <form onSubmit={(e) => { e.preventDefault(); setError(""); onSaveProfile(name.trim(), avatar.trim()).then(() => setNote("profile saved")).catch((err) => setError(errorText(err))); }}><h3>profile</h3><div className="profile-preview"><Avatar id={me} name={name} url={avatar} size="large" /><span className="mono">{me}</span></div><label>display name<input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} /></label><label>avatar URL<input type="url" value={avatar} onChange={(e) => setAvatar(e.target.value)} placeholder="https://…" /></label><button>[ save profile ]</button></form>
    <section><h3>notifications</h3><NotificationsToggle /></section>
    <section><h3>theme</h3><button className="link" onClick={onToggleTheme}>[ {theme === "dark" ? "light" : "dark"} ]</button></section>
    {multiple && <section><h3>workspace</h3><ul className="workspace-list">{workspaces.map((w) => <li key={w.workspace_id}><span>{w.name} <span className="muted">:{w.slug} · {w.role}</span></span>{w.workspace_id !== currentWorkspaceId && <button className="link" onClick={() => { localStorage.setItem("xmsg.workspace", w.workspace_id); location.reload(); }}>[ switch ]</button>}</li>)}</ul></section>}
    <form onSubmit={(e) => { e.preventDefault(); onAcceptInvite(code.trim()).then(() => { setNote("invite accepted"); setCode(""); }).catch((err) => setNote(errorText(err))); }}><h3>invite</h3><label>accept invite code<input value={code} onChange={(e) => setCode(e.target.value)} required /></label><button>[ accept ]</button></form>
    {onSaveServer && <form onSubmit={(e) => { e.preventDefault(); onSaveServer(server.trim()); }}><h3>server</h3><label>server URL<input value={server} onChange={(e) => setServer(e.target.value)} placeholder="https://…" spellCheck={false} /></label><p className="muted small">Changing the server reloads the app.</p><button>[ save server ]</button></form>}
    {(note || error) && <p className={error ? "error" : "note-text"}>{error || note}</p>}
    <button className="link sign-out" onClick={onSignOut}>[ sign out ]</button>
  </div></Modal>;
}

export type WorkspaceRole = Workspace["role"];

export interface AdminMember { user_id: string; name: string; avatar_url: string | null; role: WorkspaceRole }

/** Workspace administration for owners and admins (ADR-0026); replaces the main column. */
export function AdminPanel({ workspace, myRole, me, members, defaults, publicChannels, signup, initialTab, onRename, onSetSignup, onAddDefault, onRemoveDefault, onSetRole, onRemoveMember, onResetPassword, onCreateInvite, onClose }: { workspace: Workspace; myRole: WorkspaceRole; me: string; members: AdminMember[]; defaults: { channelId: string; name: string }[]; publicChannels: { channelId: string; name: string }[]; signup: "invite_only" | "open"; initialTab?: "workspace" | "channels" | "members" | "invites" | "reset"; onRename: (name: string) => Promise<void>; onSetSignup: (signup: "invite_only" | "open") => Promise<void>; onAddDefault: (channelId: string, addExistingMembers: boolean) => Promise<void>; onRemoveDefault: (channelId: string) => Promise<void>; onSetRole: (userId: string, role: "admin" | "member") => Promise<void>; onRemoveMember: (userId: string) => Promise<void>; onResetPassword: (userId: string) => Promise<{ code: string }>; onCreateInvite: () => Promise<string>; onClose: () => void }) {
  const [tab, setTab] = useState<"workspace" | "channels" | "members" | "invites">(initialTab === "reset" ? "members" : (initialTab ?? "workspace"));
  const [name, setName] = useState(workspace.name);
  const [pick, setPick] = useState(""); const [addExisting, setAddExisting] = useState(false);
  const [invite, setInvite] = useState(""); const [copied, setCopied] = useState(false);
  const [note, setNote] = useState("");
  const [signupLocal, setSignupLocal] = useState(signup);
  useEffect(() => setSignupLocal(signup), [signup]);
  const [reset, setReset] = useState<{ userId: string; code: string } | null>(initialTab === "reset" ? { userId: "1003", code: "xmsg-reset-4f8e2a-9c1b" } : null);
  const [resetCopied, setResetCopied] = useState(false);
  const run = (action: Promise<unknown>, ok: string) => action.then(() => setNote(ok)).catch((err) => setNote(errorText(err)));
  const changeSignup = (next: "invite_only" | "open") => {
    if (next === signupLocal) return;
    const previous = signupLocal;
    setSignupLocal(next); // optimistic; rolled back on failure
    onSetSignup(next).catch((err) => { setSignupLocal(previous); setNote(errorText(err)); });
  };
  const mayReset = (m: AdminMember) => m.user_id !== me && (myRole === "owner" ? m.role !== "owner" : m.role === "member");
  const candidates = publicChannels.filter((c) => !defaults.some((d) => d.channelId === c.channelId));
  return <section className="admin-panel"><header className="panel-title">ADMIN · {workspace.name.toUpperCase()}<button className="link" onClick={onClose}>×</button></header><nav className="admin-tabs">{(["workspace", "channels", "members", "invites"] as const).map((item) => <button key={item} className={`link ${tab === item ? "active" : ""}`} onClick={() => setTab(item)}>{item}</button>)}</nav>
    {tab === "workspace" && <div className="modal-form"><form onSubmit={(e) => { e.preventDefault(); run(onRename(name.trim()), "workspace renamed"); }}><label>name<input value={name} onChange={(e) => setName(e.target.value)} required maxLength={80} /></label><button>[ save ]</button></form><fieldset className="signup-policy"><legend>sign-up</legend><label><input type="radio" name="signup-policy" checked={signupLocal === "invite_only"} onChange={() => changeSignup("invite_only")} /><span className="radio-dot" aria-hidden="true">({signupLocal === "invite_only" ? "•" : " "})</span>invite only</label><label><input type="radio" name="signup-policy" checked={signupLocal === "open"} onChange={() => changeSignup("open")} /><span className="radio-dot" aria-hidden="true">({signupLocal === "open" ? "•" : " "})</span>open</label></fieldset></div>}
    {tab === "channels" && <div className="modal-form"><h3>default channels</h3><ul className="workspace-list">{defaults.map((d) => <li key={d.channelId}><span>#{d.name}</span><button className="link" onClick={() => run(onRemoveDefault(d.channelId), `removed #${d.name}`)}>[ remove ]</button></li>)}</ul>{candidates.length > 0 && <form onSubmit={(e) => { e.preventDefault(); if (!pick) return; run(onAddDefault(pick, addExisting).then(() => { setPick(""); setAddExisting(false); }), "default channel added"); }}><label>add from public channels<select value={pick} onChange={(e) => setPick(e.target.value)}><option value="">choose…</option>{candidates.map((c) => <option key={c.channelId} value={c.channelId}>#{c.name}</option>)}</select></label><label className="checkbox"><input type="checkbox" checked={addExisting} onChange={(e) => setAddExisting(e.target.checked)} />add existing members</label><button>[ add default ]</button></form>}{defaults.length === 0 && <p className="muted">no default channels yet — new members would land nowhere.</p>}</div>}
    {tab === "members" && <div className="modal-form"><ul className="member-rows">{members.map((m) => <li key={m.user_id}><div className="member-row"><span>{m.name}{m.user_id === me ? " (you)" : ""} <span className="muted">· {m.role}</span></span>{mayReset(m) && <button className="link" onClick={() => { setResetCopied(false); run(onResetPassword(m.user_id).then((reply) => setReset({ userId: m.user_id, code: reply.code })), "reset code created"); }}>[ reset password ]</button>}{myRole === "owner" && m.role !== "owner" && m.user_id !== me && <span className="member-actions"><select value={m.role} onChange={(e) => run(onSetRole(m.user_id, e.target.value as "admin" | "member"), `role updated`)}><option value="admin">admin</option><option value="member">member</option></select><button className="link" onClick={() => run(onRemoveMember(m.user_id), `removed ${m.name}`)}>[ remove ]</button></span>}</div>{reset?.userId === m.user_id && <p className="reset-code">{reset.code} <button className="link" onClick={() => { void navigator.clipboard?.writeText(reset.code).then(() => setResetCopied(true)); }}>[ copy ]</button>{resetCopied && <span className="muted"> copied</span>} <span className="muted">· expires in 1 hour · shown once</span></p>}</li>)}</ul></div>}
    {tab === "invites" && <div className="modal-form"><button onClick={() => { setCopied(false); run(onCreateInvite().then((value) => { setInvite(value); }), "invite created"); }}>[ create invite link ]</button>{invite && <p className="invite-code">{invite} <button className="link" onClick={() => { void navigator.clipboard?.writeText(invite).then(() => setCopied(true)); }}>[ copy ]</button>{copied && <span className="muted"> copied</span>}</p>}<p className="muted small">anyone with the link can join {workspace.name}.</p></div>}
    {note && <p className="note-text">{note}</p>}
  </section>;
}

export { canPost };
