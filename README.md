# tether

Web terminal for a VPS. Single Go binary, browser client included.

Like ttyd, but built around unreliable connections. The resilience model is
closer to mosh than to ttyd: the server keeps its own copy of the screen
state (vendored vt10x) and sends clients deltas instead of raw bytes. Frames
carry sequence numbers and are kept in a ring buffer, so a reconnecting
client requests what it missed and continues where it was.

## Build

    cd web && bun install && bun run build.mjs
    go build -o tether .

web/dist is committed, so `go build -o tether .` alone works when the
frontend is unchanged.

## Run

    ./tether -p 7690 -c user:pass

    -p port           listen port, default 7690
    -a addr           bind address, default 0.0.0.0
    -c user:pass      basic auth with per-IP lockout, off by default
    -idle dur         reap viewer-less sessions, default 30m, 0 disables
    -shared           route every client into one session
    -maxupload bytes  per-file upload cap, default 64 MiB
    -rtcport port     TCP port for ICE-TCP candidates, default 8443, 0 disables
    -uploads bool     accept file uploads, default true

To keep it running across reboots:

    sudo cp contrib/tether.service /etc/systemd/system/
    sudo systemctl daemon-reload && sudo systemctl enable --now tether

Plain HTTP, no TLS. Put a reverse proxy in front if that matters.

## Client

xterm.js and an embedded JetBrains Mono copy. Selecting text copies it,
Ctrl+V pastes. OSC 52 from applications works in both directions. Phones get
a bar with Esc, Ctrl, arrows and similar keys.

## Files

Pasting or dropping a non-text file uploads it over POST /upload into
tether-uploads/ inside the working directory of the terminal's foreground
process, with ~/tether-uploads/ as fallback. Text pastes are not intercepted.

To send a file from the server to the browser:

    scripts/tdl report.pdf

prints an iTerm2 compatible OSC 1337 sequence and the browser shows a save
button. Works from any pane because it rides the PTY stream.

## Transport

Binary WebSocket frames: 9 byte header (type, sequence number, length), then
a type specific body. Screen updates are diffed per row span and varint
encoded, permessage-deflate is enabled. On reconnect the client sends the last
sequence it applied and receives the missing frames from the ring, or a
keyframe when out of range.

An optional WebRTC data channel mode sends screen updates unordered and
unreliable. Gaps trigger keyframe resync over the WebSocket.
Open the page with ?nortc to disable it.
On networks that filter outbound UDP (common on mobile carriers), direct
candidates fail and clients fall back to a TURN relay over TCP. Configure one
with -turn / -turn-user / -turn-pass; clients fetch their ICE config from /ice
at runtime, so no deployment-specific values end up in this repo. The bundled
contrib/tether.service expects them in /etc/default/tether.

## Sessions

Each browser gets its own shell, keyed by a localStorage id. -shared puts
every client in the same terminal instead. Viewer-less sessions are reaped
after 30 minutes unless -idle 0.

## Notes

Scrollback from before a reconnect is not restored. A server restart kills
all sessions. In shared mode the most recent resize applies to everyone.

MIT, see LICENSE. vt10x keeps its original license in internal/vt/.
