// SPDX-License-Identifier: Apache-2.0
// Calls (ADR-0025): the call panel. The calls service gives us a LiveKit URL and token for
// the channel's call; LiveKit carries the audio, video and screen shares.

import { useEffect, useRef, useState } from "react";
import { Participant, Room, RoomEvent, Track, type RemoteTrack } from "livekit-client";
import { api, session } from "./session.ts";

type Status = "connecting" | "connected" | "error";

/** A participant's tile: their screen share if any, else their camera, else their name. */
function Tile({ participant, label }: { participant: Participant; label: string }) {
  const video = useRef<HTMLVideoElement>(null);
  const screen = participant.getTrackPublication(Track.Source.ScreenShare);
  const camera = participant.getTrackPublication(Track.Source.Camera);
  const shown = screen?.track && !screen.isMuted ? screen : camera?.track && !camera.isMuted ? camera : undefined;
  const track = shown?.track;
  useEffect(() => {
    const el = video.current;
    if (!track || !el) return;
    track.attach(el);
    return () => {
      track.detach(el);
    };
  }, [track]);
  return (
    <div className={`call-tile ${participant.isSpeaking ? "speaking" : ""}`}>
      {track ? <video ref={video} autoPlay playsInline muted={participant.isLocal} /> : <div className="call-avatar">{label.slice(0, 1).toUpperCase()}</div>}
      <span className="call-name small">
        {label}
        {!participant.isMicrophoneEnabled && " 🔇"}
        {shown?.source === Track.Source.ScreenShare && " · sharing"}
      </span>
    </div>
  );
}

export function CallPanel({ channelId, title, label, onLeave }: { channelId: string; title: string; label: (userId: string) => string; onLeave: () => void }) {
  const [room] = useState(() => new Room({ adaptiveStream: true, dynacast: true }));
  const [status, setStatus] = useState<Status>("connecting");
  const [error, setError] = useState("");
  // Bumped on every room change, so the tiles and controls re-read the room.
  const [, setTick] = useState(0);

  useEffect(() => {
    const changed = () => setTick((n) => n + 1);
    // A failed connect also fires Disconnected: only a call we were in ends by it.
    let connected = false;
    const audio = new Map<string, HTMLMediaElement>();
    const subscribed = (track: RemoteTrack) => {
      if (track.kind !== Track.Kind.Audio) return changed();
      const el = track.attach();
      el.hidden = true;
      document.body.appendChild(el);
      audio.set(track.sid ?? "", el);
    };
    const unsubscribed = (track: RemoteTrack) => {
      const el = audio.get(track.sid ?? "");
      if (el) {
        track.detach(el);
        el.remove();
        audio.delete(track.sid ?? "");
      }
      changed();
    };
    room
      .on(RoomEvent.TrackSubscribed, subscribed)
      .on(RoomEvent.TrackUnsubscribed, unsubscribed)
      .on(RoomEvent.ParticipantConnected, changed)
      .on(RoomEvent.ParticipantDisconnected, changed)
      .on(RoomEvent.TrackMuted, changed)
      .on(RoomEvent.TrackUnmuted, changed)
      .on(RoomEvent.LocalTrackPublished, changed)
      .on(RoomEvent.LocalTrackUnpublished, changed)
      .on(RoomEvent.ActiveSpeakersChanged, changed)
      .on(RoomEvent.Disconnected, () => connected && onLeave());
    let cancelled = false;
    session
      .accessToken()
      .then((token) => api.joinCall(token, channelId))
      .then(async ({ url, token }) => {
        if (cancelled) return;
        await room.connect(url, token);
        connected = true;
        setStatus("connected");
        // Talking by default; the camera is the user's choice.
        await room.localParticipant.setMicrophoneEnabled(true).catch(() => setError("No microphone (or no permission)"));
      })
      .catch((e: unknown) => {
        if (cancelled) return;
        setStatus("error");
        setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
      room.removeAllListeners();
      for (const el of audio.values()) el.remove();
      void room.disconnect();
    };
    // One room per mounted panel: a new channel mounts a new panel.
  }, [room, channelId, onLeave]);

  const me = room.localParticipant;
  const toggle = (what: "mic" | "camera" | "screen") => {
    const run =
      what === "mic"
        ? me.setMicrophoneEnabled(!me.isMicrophoneEnabled)
        : what === "camera"
          ? me.setCameraEnabled(!me.isCameraEnabled)
          : me.setScreenShareEnabled(!me.isScreenShareEnabled, { audio: true });
    run
      .then(() => setTick((n) => n + 1))
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  };
  const participants: Participant[] = [me, ...room.remoteParticipants.values()];

  return (
    <section className="call-panel" aria-label={`Call in ${title}`}>
      <header className="row">
        <span className="strong">{title}</span>
        <span className="muted small">{status === "connecting" ? "Connecting…" : status === "error" ? "Couldn't join" : `${participants.length} in the call`}</span>
      </header>
      {error && <p className="error small">{error}</p>}
      {status === "connected" && (
        <div className="call-tiles">
          {participants.map((p) => (
            <Tile key={p.identity} participant={p} label={p.isLocal ? "You" : label(p.identity)} />
          ))}
        </div>
      )}
      <div className="row call-controls">
        <button onClick={() => toggle("mic")} disabled={status !== "connected"} aria-pressed={!me.isMicrophoneEnabled}>
          {me.isMicrophoneEnabled ? "Mute" : "Unmute"}
        </button>
        <button onClick={() => toggle("camera")} disabled={status !== "connected"} aria-pressed={me.isCameraEnabled}>
          {me.isCameraEnabled ? "Stop video" : "Video"}
        </button>
        <button onClick={() => toggle("screen")} disabled={status !== "connected"} aria-pressed={me.isScreenShareEnabled}>
          {me.isScreenShareEnabled ? "Stop sharing" : "Share screen"}
        </button>
        <button className="danger" onClick={onLeave}>
          Leave
        </button>
      </div>
    </section>
  );
}
