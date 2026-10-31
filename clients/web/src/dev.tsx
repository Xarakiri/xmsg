// SPDX-License-Identifier: Apache-2.0
import { useState } from "react";
import { createRoot } from "react-dom/client";
import { Directory, type Channel, type Message } from "@xmsg/client-core";
import { AuthScreen, ConversationHeader, KeyHints, MessageRow, Prompt, Sidebar, StatusLine, ThreadPanel, channelTitle, type Theme } from "./components.tsx";
import "./styles.css";
import "./dev.css";

const me = "1001";
const names = new Directory(async () => []);
for (const [user_id, name] of [["1001", "you"], ["1002", "aiko"], ["1003", "marcus"], ["1004", "dmitri"], ["1005", "sara"]] as const) names.remember({ user_id, name, avatar_url: null });
const baseChannel = { eventSeq: 1, syncedSeq: 1, membershipSeq: 1, readSeq: 1, muted: false, notifyLevel: "all" as const, left: false, mentions: [] };
const gateway: Channel = { ...baseChannel, id: "9001", kind: "public", name: "gateway", members: ["1001", "1002", "1003", "1004"], msgSeq: 4 };
const dm: Channel = { ...baseChannel, id: "9002", kind: "dm", name: "", members: ["1001", "1002"], msgSeq: 3 };
const huddle: Channel = { ...gateway, call: { callId: "call-1", huddle: true, participants: ["1001", "1002", "1004"], startedAt: Date.now() - 600000 } };
const messages: Message[] = [
  { key: "1", channelId: gateway.id, authorId: "1002", body: "gateway deploy is ready for review", msgSeq: 1, messageId: "101", status: "sent", at: Date.now() - 2400000, replyCount: 2, reactions: { "👍": { count: 2, mine: false, seq: 1 } } },
  { key: "2", channelId: gateway.id, authorId: "1003", body: "can we publish the full spike report?", msgSeq: 2, messageId: "102", status: "sent", at: Date.now() - 1800000 },
  { key: "3", channelId: gateway.id, authorId: "1004", body: "shipping phase 1b to eu-west", msgSeq: 3, messageId: "103", status: "sent", at: Date.now() - 900000 },
  { key: "4", channelId: gateway.id, authorId: me, body: "great run — dashboards are updated", msgSeq: 4, messageId: "104", status: "sent", at: Date.now() - 300000 },
];
const replies: Message[] = [
  { ...messages[0]!, key: "r1", msgSeq: 1, body: "raw csv is in the shared drive" },
  { ...messages[0]!, key: "r2", msgSeq: 2, authorId: me, body: "svg please — adding it to the deck" },
];
const noFiles = async () => { throw new Error("fixtures are read-only"); };
const noop = () => {};

type Screen = "channel" | "dm" | "huddle" | "share" | "login" | "register";
const screens: Screen[] = ["channel", "dm", "huddle", "share", "login", "register"];

function DevApp() {
  const [screen, setScreen] = useState<Screen>("channel"); const [theme, setTheme] = useState<Theme>("dark");
  document.documentElement.dataset.theme = theme;
  return <><nav className="dev-nav"><strong>minimal cli / fixtures</strong>{screens.map((item) => <button className={item === screen ? "active" : ""} key={item} onClick={() => setScreen(item)}>{item}</button>)}<span /><button onClick={() => setTheme(theme === "dark" ? "light" : "dark")}>{theme} → {theme === "dark" ? "light" : "dark"}</button></nav><div className="dev-stage">{screen === "login" || screen === "register" ? <><StatusLine workspace="" channel="not connected" status="offline" theme={theme} onToggle={noop} /><AuthScreen mode={screen === "login" ? "in" : "up"} setMode={noop} name="Aiko Tanaka" setName={noop} email="aiko@acme.dev" setEmail={noop} password="correct-horse" setPassword={noop} passwordAgain="correct-horse" setPasswordAgain={noop} code="" setCode={noop} error="" busy={false} providers={[]} onSubmit={(e) => e.preventDefault()} /></> : <ChatFixture screen={screen} theme={theme} />}</div></>;
}

function ChatFixture({ screen, theme }: { screen: Exclude<Screen, "login" | "register">; theme: Theme }) {
  const current = screen === "dm" ? dm : screen === "huddle" || screen === "share" ? huddle : gateway;
  const title = channelTitle(current, me, names);
  const rooms = [gateway, { ...gateway, id: "9003", name: "sequencer", msgSeq: 13 }, { ...gateway, id: "9004", name: "random" }];
  return <div className="terminal-app"><StatusLine workspace="acme" channel={title} status="online" theme={theme} onToggle={noop} /><div className={`workspace-grid ${screen !== "dm" ? "with-panel" : ""}`}><Sidebar rooms={rooms.map((channel) => ({ channel, title: channelTitle(channel, me, names) }))} dms={[{ channel: dm, title: "@aiko", presence: "online" }]} calls={screen === "huddle" || screen === "share" ? [{ channel: huddle, title: "huddle-01" }] : []} selected={current.id} onSelect={noop} onNewChannel={noop} onNewDm={noop} onWorkspaces={noop} onProfile={noop} onSignOut={noop} /><main className="main-terminal">{screen === "share" && <SharePreview />}<section className="conversation"><ConversationHeader channel={current} title={title} me={me} names={names} onCall={noop} onMute={noop} onInvite={noop} onLeave={noop} /><ol className="message-list">{messages.map((message) => <MessageRow key={message.key} message={{ ...message, channelId: current.id }} me={me} names={names} onOpenThread={noop} onReact={noop} />)}</ol><Prompt channel={title} members={current.members} me={me} names={names} onSend={noop} onUpload={noFiles} /></section></main>{screen === "channel" && <ThreadPanel root={messages[0]!} replies={replies} me={me} names={names} channel={title} onSend={noop} onUpload={noFiles} onRetry={noop} onClose={noop} />}{(screen === "huddle" || screen === "share") && <aside className="right-panel call-panel"><header className="panel-title">◉ HUDDLE-01<button className="link">×</button></header><div className="call-status connected"><i />3 in call</div><div className="call-content"><section className="participant-list"><h3>— participants —</h3>{["aiko", "dmitri", "you"].map((name, index) => <div className="participant-row" key={name}><div><span className={index === 0 ? "speaker" : ""}>{name}</span><span className="muted"> · {index === 0 ? "speaking" : index === 2 ? "muted" : "live"}</span></div>{index < 2 && <div className="level-meter">{"▮".repeat(index === 0 ? 11 : 3)}{"▯".repeat(index === 0 ? 5 : 13)}</div>}</div>)}</section>{screen === "share" && <><section className="share-section"><h3>— screen share —</h3><span>dmitri <span className="muted">sharing</span></span></section><div className="call-tiles">{["aiko", "dmitri"].map((name) => <div className="call-tile fixture-camera" key={name}><span>{name.slice(0, 1).toUpperCase()}</span><span className="call-name">{name}</span></div>)}</div></>}</div><div className="call-controls"><button className="link">[m] mute</button><button className="link">[v] cam</button><button className="link">[s] share</button><button className="link">[q] leave</button></div></aside>}</div><KeyHints inCall={screen === "huddle" || screen === "share"} /></div>;
}
function SharePreview() {
  return <section className="screen-share fixture-screen-share"><header><span className="error">● LIVE</span> dmitri is sharing <strong>deployment-dashboard</strong></header><div className="fake-dashboard"><div><span>deployment</span><strong>phase 1b</strong></div><div className="fake-chart">╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲╱╲</div><div className="fake-bars">▂▃▅▃▆▅▇▆▅▇▆▇█▇▆▅▆▅▃</div></div></section>;
}

createRoot(document.getElementById("root")!).render(<DevApp />);
