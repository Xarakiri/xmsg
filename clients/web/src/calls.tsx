// SPDX-License-Identifier: Apache-2.0
import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Participant, Room, RoomEvent, Track, type RemoteTrack } from "livekit-client";
import { api, isDesktop, session } from "./session.ts";

type Status = "connecting" | "connected" | "error";

function VideoTrack({ participant, source, className = "" }: { participant: Participant; source: Track.Source; className?: string }) {
  const video = useRef<HTMLVideoElement>(null);
  const publication = participant.getTrackPublication(source);
  const track = publication?.track;
  useEffect(() => { const el = video.current; if (!track || !el) return; track.attach(el); return () => { track.detach(el); }; }, [track]);
  if (!track || publication?.isMuted) return null;
  return <video className={className} ref={video} autoPlay playsInline muted={participant.isLocal} />;
}

function participantName(participant: Participant, label: (id: string) => string) {
  return participant.isLocal ? "you" : label(participant.identity);
}

export function CallPanel({ channelId, title, label, onLeave, onClose }: { channelId: string; title: string; label: (userId: string) => string; onLeave: () => void; onClose: () => void }) {
  const [room] = useState(() => new Room({ adaptiveStream: true, dynacast: true }));
  const [status, setStatus] = useState<Status>("connecting"); const [error, setError] = useState(""); const [, setTick] = useState(0);
  const controls = useRef({ mic: () => {}, camera: () => {}, screen: () => {}, leave: onLeave });
  useEffect(() => {
    const changed = () => setTick((n) => n + 1); let connected = false; const audio = new Map<string, HTMLMediaElement>();
    const subscribed = (track: RemoteTrack) => { if (track.kind !== Track.Kind.Audio) return changed(); const el = track.attach(); el.hidden = true; document.body.appendChild(el); audio.set(track.sid ?? "", el); };
    const unsubscribed = (track: RemoteTrack) => { const el = audio.get(track.sid ?? ""); if (el) { track.detach(el); el.remove(); audio.delete(track.sid ?? ""); } changed(); };
    room.on(RoomEvent.TrackSubscribed, subscribed).on(RoomEvent.TrackUnsubscribed, unsubscribed).on(RoomEvent.ParticipantConnected, changed).on(RoomEvent.ParticipantDisconnected, changed).on(RoomEvent.TrackMuted, changed).on(RoomEvent.TrackUnmuted, changed).on(RoomEvent.LocalTrackPublished, changed).on(RoomEvent.LocalTrackUnpublished, changed).on(RoomEvent.ActiveSpeakersChanged, changed).on(RoomEvent.Disconnected, () => connected && onLeave());
    const levels = setInterval(changed, 150);
    let cancelled = false;
    session.accessToken().then((token) => api.joinCall(token, channelId)).then(async ({ url, token }) => { if (cancelled) return; await room.connect(url, token); connected = true; setStatus("connected"); await room.localParticipant.setMicrophoneEnabled(true).catch(() => setError("No microphone (or no permission)")); }).catch((e: unknown) => { if (!cancelled) { setStatus("error"); setError(e instanceof Error ? e.message : String(e)); } });
    return () => { cancelled = true; clearInterval(levels); room.removeAllListeners(); for (const el of audio.values()) el.remove(); void room.disconnect(); };
  }, [room, channelId, onLeave]);
  const me = room.localParticipant;
  // WKWebView on macOS has no getDisplayMedia: hide screen share instead of failing.
  const canShare = !isDesktop || Boolean(navigator.mediaDevices?.getDisplayMedia);
  const toggle = (what: "mic" | "camera" | "screen") => {
    if (status !== "connected" || (what === "screen" && !canShare)) return;
    const run = what === "mic" ? me.setMicrophoneEnabled(!me.isMicrophoneEnabled) : what === "camera" ? me.setCameraEnabled(!me.isCameraEnabled) : me.setScreenShareEnabled(!me.isScreenShareEnabled, { audio: true });
    run.then(() => setTick((n) => n + 1)).catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  };
  controls.current = { mic: () => toggle("mic"), camera: () => toggle("camera"), screen: () => toggle("screen"), leave: onLeave };
  useEffect(() => {
    const key = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null;
      if (target?.matches("input, textarea, select, [contenteditable=true]") || e.ctrlKey || e.metaKey || e.altKey) return;
      const action = { m: "mic", v: "camera", s: "screen" } as const;
      if (e.key.toLowerCase() in action) { e.preventDefault(); controls.current[action[e.key.toLowerCase() as keyof typeof action]](); }
      if (e.key.toLowerCase() === "q") { e.preventDefault(); controls.current.leave(); }
    };
    window.addEventListener("keydown", key); return () => window.removeEventListener("keydown", key);
  }, []);
  const participants: Participant[] = [me, ...room.remoteParticipants.values()];
  const sharing = participants.find((p) => { const publication = p.getTrackPublication(Track.Source.ScreenShare); return publication?.track && !publication.isMuted; });
  const cameras = participants.filter((p) => { const publication = p.getTrackPublication(Track.Source.Camera); return publication?.track && !publication.isMuted; });
  const slot = document.getElementById("screen-share-slot");
  return <><aside className="right-panel call-panel" aria-label={`Call in ${title}`}><header className="panel-title">◉ {title.toUpperCase()}<button className="link" onClick={onClose}>×</button></header><div className={`call-status ${status}`}><i />{status === "connected" ? `${participants.length} in call` : status}</div>{error && <p className="error">{error}</p>}{status === "connected" && <div className="call-content"><section className="participant-list"><h3>— participants —</h3>{participants.map((participant) => { const state = !participant.isMicrophoneEnabled ? "muted" : participant.isSpeaking ? "speaking" : "live"; const level = participant.isMicrophoneEnabled ? Math.min(16, Math.max(0, Math.round(participant.audioLevel * 16))) : 0; return <div className="participant-row" key={participant.identity}><div><span className={participant.isSpeaking ? "speaker" : ""}>{participantName(participant, label)}</span><span className="muted"> · {state}</span></div>{participant.isMicrophoneEnabled && <div className="level-meter" aria-label={`Audio level ${level} of 16`}>{"▮".repeat(level)}{"▯".repeat(16 - level)}</div>}</div>; })}</section>{sharing && <section className="share-section"><h3>— screen share —</h3><span>{participantName(sharing, label)} <span className="muted">sharing</span></span></section>}{cameras.length > 0 && <div className="call-tiles">{cameras.map((participant) => <div className={`call-tile ${participant.isSpeaking ? "speaking" : ""}`} key={participant.identity}><VideoTrack participant={participant} source={Track.Source.Camera} /><span className="call-name">{participantName(participant, label)}{participant.isSpeaking && " · speaking"}</span></div>)}</div>}</div>}<div className="call-controls"><button className="link" onClick={() => toggle("mic")} disabled={status !== "connected"}>[m] {me.isMicrophoneEnabled ? "mute" : "unmute"}</button><button className="link" onClick={() => toggle("camera")} disabled={status !== "connected"}>[v] cam</button><button className="link" onClick={() => toggle("screen")} disabled={status !== "connected"}>[s] share</button>{!canShare && <span className="muted small"> screen share unavailable in this webview</span>}<button className="link" onClick={onLeave}>[q] leave</button></div></aside>{sharing && slot && createPortal(<section className="screen-share"><header><span className="error">● LIVE</span> {participantName(sharing, label)} is sharing</header><VideoTrack participant={sharing} source={Track.Source.ScreenShare} /></section>, slot)}</>;
}
