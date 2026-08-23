// Package session owns PTY-backed terminal sessions: a server-side vt state
// mirror, adaptive output coalescing into protocol frames, a bounded replay
// ring for resuming dropped clients, and multi-viewer fanout.
package session

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creack/pty"
	"golang.org/x/sys/unix"

	"tether/internal/proto"
	"tether/internal/vt"
)

// Config controls session behavior.
type Config struct {
	Shell       string        // login shell to spawn
	Command     []string      // full spawn command; overrides Shell when set
	IdleTimeout time.Duration // kill sessions with no viewers after this (0 = never)
}

type storedFrame struct {
	seq  uint32
	typ  uint8
	body []byte
}

// Subscriber is one attached WebSocket view.
type Subscriber struct {
	ch      chan []byte
	lastSeq uint32
}

// Ch exposes the frame queue for the connection writer pump.
func (s *Subscriber) Ch() <-chan []byte { return s.ch }

// Session is one live PTY plus its mirror state.
type Session struct {
	ID string

	cfg     Config
	term    vt.Terminal
	state   *vt.State
	diff    *screenDiff
	ttyFile *os.File
	cmd     *exec.Cmd

	mu        sync.RWMutex // guards alive, subs, ring
	alive     bool
	subs      map[*Subscriber]struct{}
	ring      []storedFrame
	ringBytes int    // live sum of len(body) across ring; maintained by emit
	seq       uint32 // last assigned sequence number (writes hold state lock)
	done      chan struct{}
	closeOnce sync.Once
	debug     bool

	title         string
	modes         uint32
	replyW        *replyWriter // PTY-initiated responses (DSR/DA) loop back to input
	clip          *osc52
	lastAlt       bool          // last AltScreen bit sent to clients
	lastModesEcho time.Time     // last unconditional TModes echo (loss healing)
	rttNanos      atomic.Int64  // EWMA of client-measured RTT
	idleSince     atomic.Int64  // unix nano of last activity
	lastEmit      atomic.Int64  // unix nano of last fanout emission
	kick          chan struct{} // wakes the coalescer for instant flush after idle

}

// Manager creates and reaps sessions.
type Manager struct {
	mu       sync.Mutex
	sessions map[string]*Session
	cfg      Config
}

func NewManager(cfg Config) *Manager {
	if cfg.Shell == "" {
		cfg.Shell = os.Getenv("SHELL")
	}
	if cfg.Shell == "" {
		cfg.Shell = "/bin/bash"
	}
	if cfg.IdleTimeout < 0 { // 0 = keep sessions indefinitely
		cfg.IdleTimeout = 30 * time.Minute
	}
	return &Manager{sessions: map[string]*Session{}, cfg: cfg}
}

// Get returns an existing live session.
func (m *Manager) Get(sid string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[sid]
	if s != nil && !s.IsAlive() {
		delete(m.sessions, sid)
		return nil
	}
	return s
}

// Create spawns a new PTY session with the given initial grid size.
func (m *Manager) Create(cols, rows uint16) (*Session, error) {
	if cols < 2 || rows < 2 || cols > 500 || rows > 200 {
		cols, rows = 80, 24
	}
	argv := m.cfg.Command
	if len(argv) == 0 {
		argv = []string{m.cfg.Shell, "-l"}
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "TERM=xterm-256color")
	ttyFile, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: uint16(cols), Rows: uint16(rows)})
	if err != nil {
		return nil, fmt.Errorf("spawn shell: %w", err)
	}
	rw := &replyWriter{}
	term, state := vt.NewWithState(vt.WithWriter(rw), vt.WithSize(int(cols), int(rows)))
	s := &Session{
		ID:      newSID(),
		cfg:     m.cfg,
		term:    term,
		state:   state,
		ttyFile: ttyFile,
		cmd:     cmd,
		alive:   true,
		subs:    map[*Subscriber]struct{}{},
		done:    make(chan struct{}),
		kick:    make(chan struct{}, 1),
		clip:    newOsc52(),
		title:   "",
		modes:   0xffffffff, // force initial MODES emission
		replyW:  rw,
		debug:   os.Getenv("TETHER_DEBUG") != "",
	}
	s.diff = newScreenDiff(int(cols), int(rows))
	state.SetScrollHook(func(rows [][]vt.Glyph) { s.diff.onScroll(len(rows)) })
	now := time.Now().UnixNano()
	s.idleSince.Store(now)

	m.mu.Lock()
	m.sessions[s.ID] = s
	m.mu.Unlock()

	go s.readLoop()
	go s.coalesceLoop()
	return s, nil
}

// Reap kills sessions that have had no viewers beyond the idle timeout.
func (m *Manager) Reap() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, s := range m.sessions {
		if !s.IsAlive() {
			delete(m.sessions, id)
			continue
		}
		if m.cfg.IdleTimeout > 0 && s.viewers() == 0 &&
			time.Since(time.Unix(0, s.idleSince.Load())) > m.cfg.IdleTimeout {
			log.Printf("session %s idle %.0fm; reaping", id,
				time.Since(time.Unix(0, s.idleSince.Load())).Minutes())
			s.shutdown()
			delete(m.sessions, id)
		}
	}
}

// --- session internals ----------------------------------------------------

func (s *Session) IsAlive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.alive
}

func (s *Session) viewers() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subs)
}

func (s *Session) touch() { s.idleSince.Store(time.Now().UnixNano()) }

// NoteRTT folds a client RTT sample into the EWMA (milliseconds).
func (s *Session) NoteRTT(ms float64) {
	old := float64(s.rttNanos.Load()) / 1e6
	var ema float64
	if old == 0 {
		ema = ms
	} else {
		ema = 0.75*old + 0.25*ms
	}
	s.rttNanos.Store(int64(ema * 1e6))
}

// WriteInput forwards keystroke bytes to the PTY.
//
// Mouse-report sequences are validated against the authoritative mirror: a
// client with a stale "mouse tracking on" belief (missed TModes frame, buggy
// app that exited dirty, ...) must not be able to spray SGR fragments into
// whatever is in the foreground. When the mirror says no mouse tracking is
// active, such frames are dropped instead of typed.
func (s *Session) WriteInput(p []byte) {
	s.touch()
	if len(p) > 0 && p[0] == 0x1b && !s.mouseTracking() && looksLikeMouseReport(p) {
		if s.debug {
			log.Printf("[%s] dropped %d stale mouse-report bytes", s.ID, len(p))
		}
		return
	}
	if s.debug {
		log.Printf("[%s] input %d bytes", s.ID, len(p))
	}
	if _, err := s.ttyFile.Write(p); err != nil {
		log.Printf("session %s input: %v", s.ID, err)
	}
}

// mouseTracking reports whether any mouse reporting mode is active.
func (s *Session) mouseTracking() bool {
	m := s.modesBits()
	return m&(proto.MouseBtnBit|proto.MouseMotionBit|proto.MouseAnyBit) != 0
}

// looksLikeMouseReport recognizes X10 and SGR mouse encodings at the start
// of an input chunk (clients send each report as its own frame).
func looksLikeMouseReport(p []byte) bool {
	if len(p) < 3 || p[0] != 0x1b || p[1] != '[' {
		return false
	}
	if p[2] == 'M' { // X10: ESC [ M b x y
		return len(p) >= 6
	}
	if p[2] == '<' { // SGR: ESC [ < b ; x ; y M| m
		last := p[len(p)-1]
		return last == 'M' || last == 'm'
	}
	return false
}

// Resize updates the PTY and mirror grid; kernel delivers SIGWINCH.
func (s *Session) Resize(cols, rows uint16) {
	s.touch()
	if cols < 2 || rows < 2 || cols > 500 || rows > 200 {
		return
	}
	_ = pty.Setsize(s.ttyFile, &pty.Winsize{Cols: cols, Rows: rows})
	s.term.Resize(int(cols), int(rows)) // takes state lock itself
}

// shutdown terminates the child process; reader loop finalizes the rest.
func (s *Session) shutdown() {
	if s.cmd.Process != nil {
		_ = s.cmd.Process.Kill()
	}
}

// readLoop drains PTY output into the mirror, scanner, and reply loopback.
func (s *Session) readLoop() {
	buf := make([]byte, 32<<10)
	tail := []byte{}
	for {
		n, err := s.ttyFile.Read(buf)
		if n > 0 {
			if s.debug {
				log.Printf("[%s] read %d bytes", s.ID, n)
			}
			chunk := append(tail, buf[:n]...)
			written := 0
			for written < len(chunk) {
				w, werr := s.term.Write(chunk[written:])
				if w > 0 {
					written += w
				}
				if werr != nil || w == 0 {
					break
				}
			}
			// Feed the OSC scanner exactly the bytes the emulator consumed,
			// once. Feeding before writing (or re-feeding carried tail bytes)
			// duplicates input on partial writes and corrupts in-flight
			// base64 payloads.
			s.clip.feed(chunk[:written])
			tail = append(tail[:0], chunk[written:]...)
			if reps := s.replyW.take(); len(reps) > 0 {
				_, _ = s.ttyFile.Write(reps)
			}
			s.drainClip()
			s.touch()
			select {
			case s.kick <- struct{}{}:
			default:
			}
		}
		if err != nil {
			s.finish()
			return
		}
	}
}

func (s *Session) drainClip() {
	for {
		select {
		case text := <-s.clip.out:
			s.state.Lock()
			s.emit(proto.TClip, []byte(text))
			s.state.Unlock()
		case fo := <-s.clip.files:
			log.Printf("[%s] file offer scanned: %s (%d bytes)", s.ID, fo.Name, len(fo.Data))
			s.emitEphemeral(proto.TFile, encodeFileOffer(fo))
		default:
			return
		}
	}
}

// encodeFileOffer packs name + payload: u16le nameLen | name | u32le len | data.
func encodeFileOffer(fo FileOffer) []byte {
	b := make([]byte, 2+len(fo.Name)+4+len(fo.Data))
	b[0] = byte(len(fo.Name))
	b[1] = byte(len(fo.Name) >> 8)
	copy(b[2:], fo.Name)
	n := len(fo.Data)
	off := 2 + len(fo.Name)
	b[off] = byte(n)
	b[off+1] = byte(n >> 8)
	b[off+2] = byte(n >> 16)
	b[off+3] = byte(n >> 24)
	copy(b[off+4:], fo.Data)
	return b
}

// emitEphemeral fans a frame out without assigning a sequence number or
// occupying replay-ring space. Ephemeral frames are best-effort notifications
// (file offers); clients must not sequence-guard them.
func (s *Session) emitEphemeral(typ uint8, body []byte) {
	data := proto.Encode(make([]byte, 0, len(body)+proto.HeaderSize), proto.Frame{Type: typ, Body: body})
	s.mu.Lock()
	for sub := range s.subs {
		select {
		case sub.ch <- data:
		default:
			delete(s.subs, sub)
			close(sub.ch)
		}
	}
	s.mu.Unlock()
}

// finish marks the session dead once the child exits and notifies viewers.
func (s *Session) finish() {
	s.mu.Lock()
	wasAlive := s.alive
	s.alive = false
	s.mu.Unlock()
	if !wasAlive {
		return
	}
	s.state.Lock()
	s.emit(proto.TExit, nil)
	s.state.Unlock()
	time.Sleep(300 * time.Millisecond) // let EXIT flush to sockets
	s.mu.Lock()
	for sub := range s.subs {
		close(sub.ch)
		delete(s.subs, sub)
	}
	s.mu.Unlock()
	s.closeOnce.Do(func() { close(s.done) })
}

// emit assigns the next sequence number, records the frame in the replay
// ring, and fans it out. The vt state lock MUST be held.
func (s *Session) emit(typ uint8, body []byte) {
	s.lastEmit.Store(time.Now().UnixNano())
	s.seq++
	f := storedFrame{seq: s.seq, typ: typ, body: body}
	s.ring = append(s.ring, f)
	s.ringBytes += len(body)
	// Evict by count and by bytes: keyframes are large, and with -idle 0 an
	// eternal session must not accumulate unbounded replay history. Byte
	// total is tracked incrementally; a full scan here would run on every
	// emission (up to ~80/s) while holding the state lock.
	const maxFrames = 512
	const maxBytes = 4 << 20
	cut := 0
	for (len(s.ring)-cut > 64 && s.ringBytes > maxBytes) || len(s.ring)-cut > maxFrames {
		s.ringBytes -= len(s.ring[cut].body)
		cut++
	}
	if cut > 0 {
		s.ring = s.ring[cut:]
	}
	// state lock (held by callers) orders seq/ring, mu guards the map.
	data := proto.Encode(make([]byte, 0, len(body)+proto.HeaderSize), proto.Frame{Type: typ, Seq: f.seq, Body: body})
	s.mu.Lock()
	for sub := range s.subs {
		select {
		case sub.ch <- data:
		default:
			// Slow consumer: sever it; the client resumes via replay.
			delete(s.subs, sub)
			close(sub.ch)
		}
	}
	s.mu.Unlock()
}

// modesBits snapshots wire mode bits from the mirror.
func (s *Session) modesBits() uint32 {
	st := s.state
	var m uint32
	btn, motion, any, sgr, x10 := st.MouseTracking()
	if btn {
		m |= proto.MouseBtnBit
	}
	if motion {
		m |= proto.MouseMotionBit
	}
	if any {
		m |= proto.MouseAnyBit
	}
	if sgr {
		m |= proto.MouseSGRBit
	}
	if x10 {
		m |= proto.MouseX10Bit
	}
	if st.BracketedPaste() {
		m |= proto.BracketPasteBit
	}
	if st.AppCursor() {
		m |= proto.AppCursorBit
	}
	if st.AltScreen() {
		m |= proto.AltScreenBit
	}
	if st.CursorVisible() {
		m |= proto.CursorVisibleBit
	}
	return m
}

// coalesceLoop periodically encodes mirror changes into broadcast frames.
func (s *Session) coalesceLoop() {
	interval := 12 * time.Millisecond
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		case <-s.kick:
			// PTY produced output; flush instantly when the link has been
			// quiet for at least one interval (burst-start latency win),
			// otherwise let the ticker keep the agreed cadence.
			if time.Since(time.Unix(0, s.lastEmit.Load())) < interval {
				continue
			}
		}
		if !s.IsAlive() {
			continue
		}
		// Adapt cadence to measured RTT: slower links get fatter, rarer frames.
		if rtt := float64(s.rttNanos.Load()) / 1e6; rtt > 40 {
			want := time.Duration(rtt/4) * time.Millisecond
			if want < 12*time.Millisecond {
				want = 12 * time.Millisecond
			}
			if want > 60*time.Millisecond {
				want = 60 * time.Millisecond
			}
			if want != interval {
				interval = want
				t.Reset(interval)
			}
		} else if interval != 12*time.Millisecond && rtt > 0 && rtt < 25 {
			interval = 12 * time.Millisecond
			t.Reset(interval)
		}

		s.state.Lock()
		if title := s.state.Title(); title != s.title {
			s.title = title
			s.emit(proto.TTitle, []byte(title))
		}
		if modes := s.modesBits(); modes != s.modes {
			s.modes = modes
			s.emit(proto.TModes, binary.LittleEndian.AppendUint32(nil, modes))
			s.lastModesEcho = time.Now()
			if alt := modes&proto.AltScreenBit != 0; alt != s.lastAlt {
				s.lastAlt = alt
				s.emit(proto.TKeyframe, s.diff.keyframe(s.state))
			}
		} else if time.Since(s.lastModesEcho) > 3*time.Second {
			// Mode frames ride whichever transport the viewer is on,
			// including the lossy datagram path; a lost TModes leaves a
			// client stuck with stale beliefs (e.g. wheel forwarded into
			// the void). Echoing current bits periodically self-heals that.
			s.lastModesEcho = time.Now()
			s.emit(proto.TModes, binary.LittleEndian.AppendUint32(nil, modes))
		}
		if cols, rows := s.state.Size(); cols != s.diff.cols || rows != s.diff.rows {
			s.emit(proto.TKeyframe, s.diff.keyframe(s.state))
		} else if body := s.diff.delta(s.state); body != nil {
			if s.debug {
				log.Printf("[%s] delta %d bytes seq=%d", s.ID, len(body), s.seq+1)
			}
			s.emit(proto.TDelta, body)
		}
		s.state.Unlock()
	}
}

// Attach validates a client handshake and streams the Subscriber up to date:
// SERVERHELLO, then either replayed missed frames or a fresh keyframe.
// The returned Subscriber is already registered for live broadcasts.
func (s *Session) Attach(hello proto.ClientHello) (*Subscriber, bool) {
	sub := &Subscriber{ch: make(chan []byte, 1024)}

	s.state.Lock() // blocks coalescer: history + registration stay ordered
	defer s.state.Unlock()
	cur := s.seq
	cols, rows := s.state.Size()
	helloFrame := proto.Encode(nil, proto.Frame{Type: proto.TInit, Seq: cur,
		Body: proto.AppendServerHello(nil, s.ID, uint16(cols), uint16(rows), cur+1, s.modesBits())})
	select {
	case sub.ch <- helloFrame:
	default:
		return nil, false
	}
	fresh := hello.LastSeq == 0 || hello.LastSeq > cur ||
		(len(s.ring) > 0 && hello.LastSeq+1 < s.ring[0].seq)

	if fresh {
		kf := proto.Encode(nil, proto.Frame{Type: proto.TKeyframe, Seq: cur + 1, Body: s.diff.keyframe(s.state)})
		select {
		case sub.ch <- kf:
		default:
			return nil, false
		}
	} else {
		for _, f := range s.ring {
			if f.seq > hello.LastSeq {
				select {
				case sub.ch <- proto.Encode(nil, proto.Frame{Type: f.typ, Seq: f.seq, Body: f.body}):
				default:
					return nil, false
				}
			}
		}
	}

	s.touch()
	if !s.alive {
		// Deliver the exit notice even though the child is gone.
		s.seq++
		select {
		case sub.ch <- proto.Encode(nil, proto.Frame{Type: proto.TExit, Seq: s.seq}):
		default:
		}
	}
	s.mu.Lock()
	s.subs[sub] = struct{}{}
	s.mu.Unlock()
	return sub, true
}

// Detach unregisters a viewer.
func (s *Session) Detach(sub *Subscriber) {
	s.mu.Lock()
	delete(s.subs, sub)
	s.mu.Unlock()
	s.touch()
}

// RequestKeyframe emits the current mode bits plus a full-screen keyframe to
// every viewer, resynchronizing anyone who lost intermediate frames (e.g. on
// an unreliable transport) — including stale mouse-tracking beliefs.
func (s *Session) RequestKeyframe() {
	s.state.Lock()
	defer s.state.Unlock()
	if !s.IsAlive() {
		return
	}
	bits := s.modesBits()
	s.emit(proto.TModes, binary.LittleEndian.AppendUint32(nil, bits))
	s.modes = bits
	s.lastAlt = bits&proto.AltScreenBit != 0
	s.emit(proto.TKeyframe, s.diff.keyframe(s.state))
}

// Done exposes session termination.
func (s *Session) Done() <-chan struct{} { return s.done }

// CurrentSeq reports the newest assigned sequence number.
func (s *Session) CurrentSeq() uint32 {
	s.state.Lock()
	defer s.state.Unlock()
	return s.seq
}

// CloseSub detaches a viewer and releases its queue; a blocked writer pump
// exits after draining whatever is buffered. Holds the state lock so emit's
// fanout can never send on the closing channel.
func (s *Session) CloseSub(sub *Subscriber) {
	s.state.Lock()
	defer s.state.Unlock()
	s.mu.Lock()
	delete(s.subs, sub)
	s.mu.Unlock()
	close(sub.ch)
}

// CreateWithID spawns a session registered under a specific id, replacing any
// dead session that previously held it.
func (m *Manager) CreateWithID(id string, cols, rows uint16) (*Session, error) {
	m.mu.Lock()
	if old := m.sessions[id]; old != nil && old.IsAlive() {
		m.mu.Unlock()
		return old, nil
	}
	delete(m.sessions, id)
	m.mu.Unlock()
	s, err := m.Create(cols, rows)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	delete(m.sessions, s.ID)
	s.ID = id
	m.sessions[id] = s
	m.mu.Unlock()
	return s, nil
}

// Primary returns the shared session in shared mode, or the only live session
// when exactly one exists; nil when ambiguous. Used by out-of-band features
// (uploads) that must pick "the" terminal.
func (m *Manager) Primary() *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sessions) == 1 {
		for _, s := range m.sessions {
			return s
		}
	}
	return nil
}

// ForegroundDir resolves the working directory of the terminal's current
// foreground process group: TIOCGPGRP on the PTY names the group, and
// /proc/<pid>/cwd yields its directory. Returns "" when nothing resolvable
// runs (rare) or the platform refuses the ioctl. Used to route uploads into
// the directory the user is actually looking at.
func (s *Session) ForegroundDir() string {
	pgid, err := unix.IoctlGetInt(int(s.ttyFile.Fd()), unix.TIOCGPGRP)
	if err != nil || pgid <= 1 {
		return ""
	}
	// The group leader is almost always the shell/app the user is looking
	// at; its cwd answers the question without walking /proc. Fall back to
	// a member scan only when that readlink fails.
	if cwd, err := os.Readlink(fmt.Sprintf("/proc/%d/cwd", pgid)); err == nil && cwd != "" {
		return cwd
	}
	leader := ""
	member := ""
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return ""
	}
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile("/proc/" + e.Name() + "/stat")
		if err != nil {
			continue
		}
		// pgrp is field 5; comm (field 2) may contain spaces/parens, so parse
		// fields after the last ')'.
		idx := bytes.LastIndexByte(stat, ')')
		if idx < 0 {
			continue
		}
		fields := bytes.Fields(stat[idx+1:])
		if len(fields) < 3 {
			continue
		}
		pgrp, err := strconv.Atoi(string(fields[2]))
		if err != nil || pgrp != pgid {
			continue
		}
		cwd, err := os.Readlink("/proc/" + e.Name() + "/cwd")
		if err != nil || cwd == "" {
			continue
		}
		if pid == pgid {
			leader = cwd
			break
		}
		if member == "" {
			member = cwd
		}
	}
	if leader != "" {
		return leader
	}
	return member
}
