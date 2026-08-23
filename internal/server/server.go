// Package server wires HTTP routes, WebSocket upgrades, optional basic auth,
// and per-connection frame pumps to the session manager.
package server

import (
	"crypto/subtle"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/logging"
	"github.com/pion/webrtc/v3"

	"tether/internal/proto"
	"tether/internal/session"
)

// Options configures the server.
type Options struct {
	AuthUser  string // empty disables basic auth
	AuthPass  string
	Mgr       *session.Manager
	Shared    bool // route every client to one shared session
	Static    http.FileSystem
	UploadDir string // where POST /upload stores files; empty disables uploads
	MaxUpload int64  // per-file byte cap for uploads
	RTCPort   int    // TCP listener for ICE-TCP candidates; 0 disables the datagram path over TCP
	TurnURL   string // e.g. turn:<host>:3478?transport=tcp; served via /ice, empty disables
	TurnUser  string
	TurnPass  string
}

// Server is the tether HTTP frontend.
type Server struct {
	opts     Options
	upgrader websocket.Upgrader
	pingSeq  atomic.Uint32
	rtcAPI   *webrtc.API // non-nil when ICE-TCP is enabled via RTCPort
}

// New builds a Server. When RTCPort is set, an ICE-TCP mux listens there so
// clients on UDP-hostile networks (carrier NAT filtering UDP) can still run
// the datagram path; failure only downgrades to UDP candidates.
func New(opts Options) *Server {
	s := &Server{
		opts: opts,
		upgrader: websocket.Upgrader{
			EnableCompression: true, // permessage-deflate when client offers it
			ReadBufferSize:    4 << 10,
			WriteBufferSize:   16 << 10,
			// Same-host origins only. Basic auth gates requests, but a
			// permissive origin check would let any web page the user
			// visits drive the terminal via cross-site WebSocket using
			// the browser's cached credentials.
			CheckOrigin: func(r *http.Request) bool {
				o := r.Header.Get("Origin")
				if o == "" {
					return true // non-browser clients (curl, probes)
				}
				u, err := url.Parse(o)
				return err == nil && u.Host == r.Host
			},
		},
	}
	se := webrtc.SettingEngine{}
	if opts.RTCPort > 0 {
		if err := enableICETCP(&se, opts.RTCPort); err != nil {
			log.Printf("rtc: ICE-TCP on :%d unavailable (%v); udp candidates only", opts.RTCPort, err)
		} else {
			log.Printf("rtc: ICE-TCP listening on :%d", opts.RTCPort)
		}
	}
	if os.Getenv("TETHER_DEBUG") != "" {
		lf := logging.NewDefaultLoggerFactory()
		lf.Writer = os.Stderr
		lf.ScopeLevels["ice"] = logging.LogLevelDebug
		lf.ScopeLevels["dtls"] = logging.LogLevelDebug
		lf.ScopeLevels["sctp"] = logging.LogLevelDebug
		lf.ScopeLevels["api"] = logging.LogLevelDebug
		se.LoggerFactory = lf
	}
	s.rtcAPI = webrtc.NewAPI(webrtc.WithSettingEngine(se))
	return s
}

// Handler returns the root http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	if s.opts.UploadDir != "" {
		mux.HandleFunc("/upload", s.handleUpload)
	}
	if s.opts.TurnURL != "" {
		mux.HandleFunc("/ice", s.handleIce)
	}
	mux.HandleFunc("/offers", s.handleOffers)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.Handle("/", s.staticHandler())
	return s.auth(mux)
}

// handleIce hands clients their STUN/TURN configuration. Credentials live in
// server flags, never in the served bundle: the repo stays free of deployment
// specifics and rotating the relay only means restarting the process.
func (s *Server) handleIce(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	servers := []any{}
	if s.opts.TurnURL != "" {
		servers = append(servers, map[string]any{
			"urls":       []string{s.opts.TurnURL},
			"username":   s.opts.TurnUser,
			"credential": s.opts.TurnPass,
		})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"iceServers": servers})
}

// handleUpload stores a raw-body upload. The client sends the file bytes as
// the body and its name in X-Filename. Target: tether-uploads/ inside the live
// session's foreground working directory (so files land where the user is
// looking), falling back to the configured home root. Auth-wrapped by the mux;
// size capped hard via MaxBytesReader.
func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := r.Header.Get("X-Filename")
	if dec, derr := url.PathUnescape(name); derr == nil {
		name = dec // client encodeURIComponent()s the header value
	}
	name = sanitizeUploadName(name)

	dir, fallback := s.uploadTarget()
	r.Body = http.MaxBytesReader(w, r.Body, s.opts.MaxUpload)

	// Spill the body to a private temp file first: r.Body is single-shot, so
	// the fallback retry below (read-only or vanished cwd) must be able to
	// re-read the bytes. This also keeps RAM use independent of file size.
	spill, err := os.CreateTemp("", "tether-upload-*")
	if err != nil {
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	defer spill.Close()
	defer os.Remove(spill.Name())
	total, err := io.Copy(spill, r.Body)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			http.Error(w, "file exceeds size cap", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	if total == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}
	if _, err := spill.Seek(0, io.SeekStart); err != nil {
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}

	write := func(dir string) (string, error) {
		tmp, err := os.CreateTemp(dir, ".tether-upload-*")
		if err != nil {
			return "", err
		}
		defer os.Remove(tmp.Name()) // no-op after successful rename
		if _, err := io.Copy(tmp, io.NewSectionReader(spill, 0, total)); err != nil {
			return "", err
		}
		if err := tmp.Close(); err != nil {
			return "", err
		}
		path := uniquePath(dir, name)
		if err := os.Chmod(tmp.Name(), 0o644); err != nil {
			return "", err
		}
		return path, os.Rename(tmp.Name(), path)
	}

	path, err := write(dir)
	if err != nil && !fallback {
		// Foreground cwd may be read-only or vanished: retry at the home root.
		if dir2, ok2 := s.uploadTargetHome(); ok2 && os.MkdirAll(dir2, 0o755) == nil {
			if p2, e2 := write(dir2); e2 == nil {
				path, fallback = p2, true
				err = nil
			}
		}
	}
	if err != nil {
		log.Printf("upload: write %s: %v", path, err)
		http.Error(w, "storage error", http.StatusInternalServerError)
		return
	}
	log.Printf("upload: %s (%d bytes) from %s", path, total, r.RemoteAddr)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"path":     path,
		"disp":     displayPath(path),
		"bytes":    total,
		"fallback": fallback,
	})
}

var ()

// uploadTarget picks where an upload lands: tether-uploads/ inside the live
// session's foreground working directory when resolvable, else the configured
// home root. Second return = true when using the home fallback.
func (s *Server) uploadTarget() (dir string, fallback bool) {
	sess := s.primarySession()
	if sess != nil {
		if cwd := sess.ForegroundDir(); cwd != "" && saneDir(cwd) {
			cand := filepath.Join(cwd, "tether-uploads")
			if os.MkdirAll(cand, 0o755) == nil {
				return cand, false
			}
		}
	}
	home, _ := s.uploadTargetHome()
	_ = os.MkdirAll(home, 0o755)
	return home, true
}

func (s *Server) uploadTargetHome() (string, bool) {
	if s.opts.UploadDir != "" {
		return s.opts.UploadDir, true
	}
	return "", false
}

// primarySession resolves "the" terminal for out-of-band features.
func (s *Server) primarySession() *session.Session {
	if s.opts.Shared {
		if sess := s.opts.Mgr.Get("shared"); sess != nil {
			return sess
		}
	}
	return s.opts.Mgr.Primary()
}

var homeDir, _ = os.UserHomeDir()

func displayPath(p string) string {
	if homeDir != "" && strings.HasPrefix(p, homeDir+"/") {
		return "~" + p[len(homeDir):]
	}
	return p
}

func saneDir(d string) bool {
	return filepath.IsAbs(d) &&
		!strings.HasPrefix(d, "/proc/") && !strings.HasPrefix(d, "/sys/") && !strings.HasPrefix(d, "/dev/")
}

// sanitizeUploadName reduces a client-supplied name to a safe base element.
func sanitizeUploadName(name string) string {
	name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.Map(func(r rune) rune {
		if r < 32 || r == '/' {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || name == string(filepath.Separator) {
		return time.Now().Format("upload-20060102-150405")
	}
	return name
}

// uniquePath appends -N suffixes on collision instead of clobbering.
func uniquePath(dir, name string) string {
	p := filepath.Join(dir, name)
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	for i := 1; ; i++ {
		if _, err := os.Stat(p); err != nil {
			return p
		}
		p = filepath.Join(dir, fmt.Sprintf("%s-%d%s", base, i, ext))
	}
}

// staticHandler serves embedded assets with immutable caching for hashed
// bundles and no-store for the entry document.
func (s *Server) staticHandler() http.Handler {
	fs := http.FileServer(s.opts.Static)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Cache-Control", "no-store")
		} else {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		fs.ServeHTTP(w, r)
	})
}

// auth wraps h with optional constant-time basic authentication.
func (s *Server) auth(h http.Handler) http.Handler {
	if s.opts.AuthUser == "" && s.opts.AuthPass == "" {
		return h
	}
	user := []byte(s.opts.AuthUser)
	pass := []byte(s.opts.AuthPass)
	var failMu sync.Mutex
	fails := map[string]*authFails{} // keyed by remote IP
	// Evict stale records so scanners churning through throwaway IPs cannot
	// grow the map without bound on long-lived processes.
	go func() {
		for range time.Tick(10 * time.Minute) {
			failMu.Lock()
			for ip, f := range fails {
				if time.Since(f.blockedUntil) > time.Hour {
					delete(fails, ip)
				}
			}
			failMu.Unlock()
		}
	}()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/offers" {
			// Local processes (tdl) hand over file offers without knowing
			// the password; handleOffers enforces its own loopback guard.
			s.handleOffers(w, r)
			return
		}
		ip, _, _ := net.SplitHostPort(r.RemoteAddr)
		failMu.Lock()
		f := fails[ip]
		if f != nil && time.Now().Before(f.blockedUntil) {
			failMu.Unlock()
			w.Header().Set("Retry-After", "30")
			http.Error(w, "too many attempts", http.StatusTooManyRequests)
			return
		}
		failMu.Unlock()

		u, p, ok := r.BasicAuth()
		ok = ok &&
			subtle.ConstantTimeCompare([]byte(u), user) == 1 &&
			subtle.ConstantTimeCompare([]byte(p), pass) == 1
		if !ok {
			failMu.Lock()
			if f == nil {
				f = &authFails{}
				fails[ip] = f
			}
			f.count++
			if f.count >= 5 { // escalating lockout: brute force becomes pointless
				f.blockedUntil = time.Now().Add(time.Duration(intMin(f.count-4, 12)) * 10 * time.Second)
				log.Printf("auth: %d failures from %s; blocking", f.count, ip)
			}
			failMu.Unlock()
			w.Header().Set("WWW-Authenticate", `Basic realm="tether"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		failMu.Lock()
		delete(fails, ip) // success clears the record
		failMu.Unlock()
		h.ServeHTTP(w, r)
	})
}

// handleOffers accepts a file offer from a local process and fans it out to
// every live session's viewers as a T_FILE frame. Multiplexer panes swallow
// OSC sequences, so tdl posts here instead of relying on the PTY stream.
// Restricted to loopback: anything able to reach 127.0.0.1 on this box could
// type into the terminal anyway, so an extra secret adds nothing.
func (s *Server) handleOffers(w http.ResponseWriter, r *http.Request) {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil || (host != "127.0.0.1" && host != "::1") {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := sanitizeUploadName(r.Header.Get("X-Filename"))
	if name == "" {
		http.Error(w, "missing X-Filename", http.StatusBadRequest)
		return
	}
	body := http.MaxBytesReader(w, r.Body, s.opts.MaxUpload)
	data, err := io.ReadAll(body)
	if err != nil {
		http.Error(w, "read error", http.StatusBadRequest)
		return
	}
	if len(data) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}
	n := s.opts.Mgr.BroadcastOffer(name, data)
	log.Printf("offers: %s (%d bytes) to %d session(s)", name, len(data), n)
	w.WriteHeader(http.StatusNoContent)
}

type authFails struct {
	count        int
	blockedUntil time.Time
}

const (
	sharedSID      = "shared" // session id used when Options.Shared is set
	writeWait      = 10 * time.Second
	pongWait       = 15 * time.Second
	firstMsgWait   = 8 * time.Second
	maxClientFrame = 128 << 10
)

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade: %v", err)
		return
	}
	conn.SetReadLimit(maxClientFrame)

	hello, err := s.readHello(conn)
	if err != nil {
		_ = conn.Close()
		return
	}

	sess, freshSID, err := s.resolveSession(hello.SID)
	if err != nil {
		log.Printf("session: %v", err)
		_ = conn.Close()
		return
	}

	link := newLink(s, sess, conn)
	if !link.attach(hello, freshSID) {
		_ = conn.Close()
		return
	}
	defer link.close()

	// Liveness pings ride the WebSocket in their own sequence namespace.
	stopPing := make(chan struct{})
	go func() {
		t := time.NewTicker(4 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopPing:
				return
			case <-t.C:
				seq := s.pingSeq.Add(1)
				link.writeMu.Lock()
				_ = conn.SetWriteDeadline(time.Now().Add(writeWait))
				err := conn.WriteMessage(websocket.BinaryMessage,
					proto.Encode(nil, proto.Frame{Type: proto.TPing, Seq: seq}))
				link.writeMu.Unlock()
				if err != nil {
					return
				}
			}
		}
	}()

	s.readLoop(conn, sess, link)

	close(stopPing)
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	_ = conn.WriteControl(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second))
	_ = conn.Close()
}

// readHello requires CInit as the first binary message.
func (s *Server) readHello(c *websocket.Conn) (proto.ClientHello, error) {
	_ = c.SetReadDeadline(time.Now().Add(firstMsgWait))
	mt, data, err := c.ReadMessage()
	if err != nil {
		return proto.ClientHello{}, err
	}
	if mt != websocket.BinaryMessage || len(data) < proto.HeaderSize ||
		data[0] != proto.CInit {
		return proto.ClientHello{}, errors.New("expected CInit frame")
	}
	bodyLen := int(binary.LittleEndian.Uint32(data[5:9]))
	if len(data) < proto.HeaderSize+bodyLen {
		return proto.ClientHello{}, errors.New("truncated CInit")
	}
	return proto.ReadClientHello(data[proto.HeaderSize : proto.HeaderSize+bodyLen])
}

// resolveSession maps a handshake to a live session, creating one when needed.
// Returns the effective sid (a new one when the old session is gone).
func (s *Server) resolveSession(sid string) (*session.Session, string, error) {
	if s.opts.Shared {
		sess, err := s.opts.Mgr.CreateWithID(sharedSID, 80, 24)
		if err != nil {
			return nil, "", err
		}
		return sess, sharedSID, nil
	}
	if sid != "" {
		if sess := s.opts.Mgr.Get(sid); sess != nil {
			return sess, sid, nil
		}
	}
	sess, err := s.opts.Mgr.Create(80, 24) // client resizes right after attach
	if err != nil {
		return nil, "", err
	}
	return sess, sess.ID, nil
}

// readLoop dispatches client frames until error or close. Text frames are
// RTC signaling / keyframe requests; a mid-stream CInit re-syncs the active
// transport with replay from the client's last applied sequence.
func (s *Server) readLoop(c *websocket.Conn, sess *session.Session, link *link) {
	defer c.Close()
	for {
		_ = c.SetReadDeadline(time.Now().Add(pongWait))
		mt, data, err := c.ReadMessage()
		if err != nil {
			log.Printf("ws read end: %v", err)
			return
		}
		if mt == websocket.TextMessage {
			if link != nil {
				link.onText(data)
			}
			continue
		}
		if mt != websocket.BinaryMessage || len(data) < proto.HeaderSize {
			continue
		}
		typ := data[0]
		seq := binary.LittleEndian.Uint32(data[1:5])
		bodyLen := int(binary.LittleEndian.Uint32(data[5:9]))
		if bodyLen < 0 || len(data) < proto.HeaderSize+bodyLen {
			continue
		}
		body := data[proto.HeaderSize : proto.HeaderSize+bodyLen]

		switch typ {
		case proto.CInit:
			if h, err := proto.ReadClientHello(body); err == nil {
				link.resync(h)
			}
		case proto.CInput:
			sess.WriteInput(body)
		case proto.CResize:
			if len(body) >= 4 {
				sess.Resize(binary.LittleEndian.Uint16(body),
					binary.LittleEndian.Uint16(body[2:]))
			}
		case proto.CPong:
			// liveness satisfied by read deadline; nothing else to do
		case proto.CPing:
			// The pump, ping ticker and this path all write to c; gorilla
			// panics on concurrent writes, so take the same lock they use.
			link.writeMu.Lock()
			_ = c.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.WriteMessage(websocket.BinaryMessage,
				proto.Encode(nil, proto.Frame{Type: proto.TPong, Seq: seq}))
			link.writeMu.Unlock()
		case proto.CRtt:
			if len(body) >= 2 {
				sess.NoteRTT(float64(binary.LittleEndian.Uint16(body)))
			}
		}
	}
}

// intMin is a tiny helper for the auth backoff calculation.
func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
