// SPDX-License-Identifier: Apache-2.0
// ^K: one quick switcher for channels, people and message search. Keyboard first:
// arrows move, ⏎ jumps (or searches), esc closes. A plain-text query filters as typed.

import { useEffect, useMemo, useRef, useState } from "react";

export interface SwitcherProps {
  channels: { id: string; title: string; unread: number }[];
  people: { id: string; name: string }[];
  me: string;
  onPickChannel: (id: string) => void;
  /** Open (or start) a DM with this person. */
  onPickPerson: (id: string) => void;
  onSearch: (q: string) => void;
  onClose: () => void;
}

interface Item {
  key: string;
  label: string;
  hint?: string;
  run: () => void;
}

export function Switcher({ channels, people, me, onPickChannel, onPickPerson, onSearch, onClose }: SwitcherProps) {
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => input.current?.focus(), []);
  const items = useMemo<Item[]>(() => {
    const q = query.trim().toLowerCase();
    const hit = (value: string) => !q || value.toLowerCase().includes(q);
    const out: Item[] = [];
    for (const c of channels.filter((c) => hit(c.title))) out.push({ key: `c:${c.id}`, label: c.title, hint: c.unread > 0 ? `[${c.unread}]` : undefined, run: () => onPickChannel(c.id) });
    for (const p of people.filter((p) => p.id !== me && hit(p.name))) out.push({ key: `p:${p.id}`, label: `@${p.name}`, hint: "dm", run: () => onPickPerson(p.id) });
    if (q) out.push({ key: "search", label: `search messages for "${query.trim()}"`, run: () => onSearch(query.trim()) });
    return out;
  }, [query, channels, people, me, onPickChannel, onPickPerson, onSearch]);
  const clamped = Math.min(active, Math.max(0, items.length - 1));
  const choose = (item: Item | undefined) => {
    if (!item) return;
    onClose();
    item.run();
  };
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <section className="switcher" role="dialog" aria-label="Quick switcher">
        <input
          ref={input}
          value={query}
          placeholder="jump to a channel, a person, or search…"
          aria-label="Quick switcher"
          onChange={(e) => { setQuery(e.target.value); setActive(0); }}
          onKeyDown={(e) => {
            if (e.key === "ArrowDown") { e.preventDefault(); setActive((i) => Math.min(i + 1, items.length - 1)); }
            if (e.key === "ArrowUp") { e.preventDefault(); setActive((i) => Math.max(i - 1, 0)); }
            if (e.key === "Enter") { e.preventDefault(); choose(items[clamped]); }
            if (e.key === "Escape") { e.preventDefault(); onClose(); }
          }}
        />
        <ul>{items.map((item, i) => (
          <li key={item.key}><button className={i === clamped ? "active" : ""} onMouseEnter={() => setActive(i)} onClick={() => choose(item)}>{item.label}{item.hint && <span className="muted"> {item.hint}</span>}</button></li>
        ))}{items.length === 0 && <li className="muted switcher-empty">no matches</li>}</ul>
      </section>
    </div>
  );
}

const SHORTCUTS: [string, string][] = [
  ["^K", "jump: channels, people, search"],
  ["^N", "new channel"],
  ["^T", "open thread"],
  ["^I", "invite people"],
  ["⌥↑ / ⌥↓", "previous / next channel"],
  ["/huddle", "start a huddle"],
  ["↑", "edit your last message (empty composer)"],
  ["esc", "mark read · close panel"],
  ["?", "this list"],
];

export function ShortcutsPanel({ onClose }: { onClose: () => void }) {
  useEffect(() => {
    const keys = (e: KeyboardEvent) => { if (e.key === "Escape") { e.preventDefault(); onClose(); } };
    window.addEventListener("keydown", keys);
    return () => window.removeEventListener("keydown", keys);
  }, [onClose]);
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <section className="switcher shortcuts" role="dialog" aria-label="Shortcuts">
        <h3>shortcuts</h3>
        <ul>{SHORTCUTS.map(([key, what]) => <li key={key}><span className="shortcut-key">{key}</span> {what}</li>)}</ul>
        <button className="link" onClick={onClose}>[ esc close ]</button>
      </section>
    </div>
  );
}

/** Invite to a channel: type a name, arrows move, ⏎ toggles, several may be picked. */
export function MemberPicker({ people, exclude, busy = false, onInvite, onClose }: { people: { id: string; name: string }[]; exclude: string[]; busy?: boolean; onInvite: (ids: string[]) => void; onClose: () => void }) {
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const [picked, setPicked] = useState<Set<string>>(new Set());
  const input = useRef<HTMLInputElement>(null);
  useEffect(() => input.current?.focus(), []);
  const excluded = new Set(exclude);
  const q = query.trim().toLowerCase();
  const items = people.filter((p) => !excluded.has(p.id) && (!q || p.name.toLowerCase().includes(q)));
  const clamped = Math.min(active, Math.max(0, items.length - 1));
  const toggle = (id: string) => setPicked((prev) => { const next = new Set(prev); next.has(id) ? next.delete(id) : next.add(id); return next; });
  const invite = () => {
    if (picked.size === 0 || busy) return;
    onInvite([...picked]);
  };
  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <section className="switcher" role="dialog" aria-label="Invite people">
        <input
          ref={input}
          value={query}
          placeholder="type a name…"
          aria-label="Find people to invite"
          onChange={(e) => { setQuery(e.target.value); setActive(0); }}
          onKeyDown={(e) => {
            if (e.key === "ArrowDown") { e.preventDefault(); setActive((i) => Math.min(i + 1, items.length - 1)); }
            if (e.key === "ArrowUp") { e.preventDefault(); setActive((i) => Math.max(i - 1, 0)); }
            if (e.key === "Enter") { e.preventDefault(); if (items[clamped]) toggle(items[clamped].id); }
            if (e.key === "Escape") { e.preventDefault(); onClose(); }
          }}
        />
        <ul>{items.map((p, i) => (
          <li key={p.id}><button className={i === clamped ? "active" : ""} onMouseEnter={() => setActive(i)} onClick={() => toggle(p.id)}><span className="radio-dot" aria-hidden="true">[{picked.has(p.id) ? "x" : " "}]</span>{p.name}</button></li>
        ))}{items.length === 0 && <li className="muted switcher-empty">{q ? "no matches" : "everyone is already here"}</li>}</ul>
        <div className="picker-footer"><button className="link" disabled={picked.size === 0 || busy} onClick={invite}>[ invite {picked.size > 0 ? `${picked.size} ⏎` : "⏎"} ]</button><button className="link" onClick={onClose}>[ esc cancel ]</button></div>
      </section>
    </div>
  );
}
