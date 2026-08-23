package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/ice/v2"
	"github.com/pion/webrtc/v3"

	"tether/internal/proto"
	"tether/internal/session"
)

// enableICETCP attaches a passive TCP listener to the engine's candidate
// gathering. Browsers behind networks that drop UDP (common on mobile
// carriers) can complete connectivity checks over that TCP path instead; the
// DataChannel keeps its unreliable, unordered semantics because loss handling
// lives in SCTP, not in the socket.
func enableICETCP(se *webrtc.SettingEngine, port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	mux := ice.NewTCPMuxDefault(ice.TCPMuxParams{
		Listener:        ln,
		ReadBufferSize:  8192,
		WriteBufferSize: 4 << 20,
	})
	se.SetICETCPMux(mux)
	return nil
}

// WebRTC datagram transport. The browser opens a DataChannel
// ("tether", ordered:false, maxRetransmits:0) so screen updates behave like
// mosh's datagrams: packet loss costs freshness, never correctness — the
// client detects sequence gaps and asks for a keyframe resync over the
// reliable WebSocket, which stays connected for input, pongs, and signaling.

const rtcSignal = `
signal message JSON carried as WebSocket text frames:
  {"type":"offer","sdp":"..."}          client -> server, RTC offer
  {"type":"answer","sdp":"..."}         server -> client
  {"type":"ice","candidate":{...}}      either direction, trickle ICE
  {"type":"kf"}                         client -> server, resync request
`

type signalMsg struct {
	Type string `json:"type"`
	SDP  string `json:"sdp,omitempty"`
	Cand *struct {
		Candidate string `json:"candidate"`
		Mid       string `json:"sdpMid"`
		Line      uint16 `json:"sdpMLineIndex"`
	} `json:"candidate,omitempty"`
}

// link owns one connection's transports (WebSocket plus optional DataChannel).
type link struct {
	srv     *Server
	sess    *session.Session
	conn    *websocket.Conn
	writeMu sync.Mutex // gorilla allows one concurrent WriteMessage; pings race the pump

	mu      sync.Mutex
	sub     *session.Subscriber // active frame sink
	dc      *webrtc.DataChannel
	pc      *webrtc.PeerConnection
	gen     uint64 // bumped on every transport swap
	closing bool
}

func newLink(srv *Server, sess *session.Session, conn *websocket.Conn) *link {
	return &link{srv: srv, sess: sess, conn: conn}
}

// attach registers the first subscriber (WebSocket sink).
func (l *link) attach(hello proto.ClientHello, sid string) bool {
	sub, ok := l.sess.Attach(proto.ClientHello{Version: hello.Version, SID: sid})
	if ok {
		l.sub = sub
		l.startWSPump(sub, 1)
	}
	return ok
}

func (l *link) nextGen() uint64 { l.gen++; return l.gen }

func (l *link) startWSPump(sub *session.Subscriber, gen uint64) {
	go func() {
		conn := l.conn
		for frame := range sub.Ch() {
			l.writeMu.Lock()
			_ = conn.SetWriteDeadline(writeDeadline())
			err := conn.WriteMessage(websocketBinary, frame)
			l.writeMu.Unlock()
			if err != nil {
				break
			}
		}
		l.pumpDied(gen)
	}()
}

func (l *link) startDCPump(dc *webrtc.DataChannel, sub *session.Subscriber, gen uint64) {
	go func() {
		var dropped int
		for frame := range sub.Ch() {
			// Unreliable channel: drop under backpressure instead of queueing;
			// the client's gap detector will request a keyframe.
			if dc.BufferedAmount() > 512<<10 {
				dropped++
				continue
			}
			if err := dc.Send(frame); err != nil {
				break
			}
		}
		if dropped > 0 {
			log.Printf("[%s] rtc: dropped %d frames under backpressure", l.sess.ID, dropped)
		}
		l.pumpDied(gen)
	}()
}

// pumpDied handles a transport pump exiting. Expected after swaps (stale gen);
// an unexpected WS death closes the socket so readLoop unwinds cleanly.
func (l *link) pumpDied(gen uint64) {
	l.mu.Lock()
	current := l.gen == gen && !l.closing
	dcActive := l.dc != nil
	l.mu.Unlock()
	if !current || dcActive {
		return
	}
	_ = l.conn.Close()
}

// swapToDC retires the WebSocket sink and replays onto the DataChannel.
func (l *link) swapToDC(dc *webrtc.DataChannel) {
	l.mu.Lock()
	if l.closing || l.dc != nil {
		l.mu.Unlock()
		return
	}
	old := l.sub
	l.nextGen()
	l.sess.CloseSub(old)
	ns, ok := l.sess.Attach(proto.ClientHello{})
	if !ok {
		l.mu.Unlock()
		_ = dc.Close()
		return
	}
	l.sub = ns
	l.dc = dc
	gen := l.gen
	l.mu.Unlock()

	log.Printf("[%s] rtc: datachannel open; frames now over UDP", l.sess.ID)
	l.startDCPump(dc, ns, gen)
}

// revertToWS returns framing to the WebSocket after DataChannel loss.
func (l *link) revertToWS(reason string) {
	l.mu.Lock()
	if l.closing || l.dc == nil {
		l.mu.Unlock()
		return
	}
	old := l.sub
	dc := l.dc
	l.nextGen()
	l.sess.CloseSub(old)
	ns, ok := l.sess.Attach(proto.ClientHello{})
	if !ok {
		l.mu.Unlock()
		_ = l.conn.Close()
		return
	}
	l.sub = ns
	l.dc = nil
	gen := l.gen
	l.mu.Unlock()

	log.Printf("[%s] rtc: reverting to websocket (%s)", l.sess.ID, reason)
	_ = dc.Close()
	l.startWSPump(ns, gen)
}

// resync reattaches the CURRENT transport with replay from the client's seq.
func (l *link) resync(hello proto.ClientHello) {
	l.mu.Lock()
	old := l.sub
	dc := l.dc
	l.nextGen()
	if old != nil {
		l.sess.CloseSub(old)
	}
	ns, ok := l.sess.Attach(hello)
	if !ok {
		l.mu.Unlock()
		_ = l.conn.Close()
		return
	}
	l.sub = ns
	gen := l.gen
	l.mu.Unlock()

	if dc != nil {
		l.startDCPump(dc, ns, gen)
	} else {
		l.startWSPump(ns, gen)
	}
}

func (l *link) close() {
	l.mu.Lock()
	l.closing = true
	sub := l.sub
	dc := l.dc
	pc := l.pc
	l.sub = nil
	l.mu.Unlock()

	if sub != nil {
		l.sess.CloseSub(sub)
	}
	if dc != nil {
		_ = dc.Close()
	}
	if pc != nil {
		_ = pc.Close()
	}
}
func writeDeadline() time.Time { return time.Now().Add(writeWait) }

const (
	websocketBinary = websocket.BinaryMessage
	websocketText   = websocket.TextMessage
)

// onText processes a signaling / control text frame from the client.
func (l *link) onText(text []byte) {
	var m signalMsg
	if err := json.Unmarshal(text, &m); err != nil {
		log.Printf("rtc: bad signal: %v", err)
		return
	}
	log.Printf("rtc: signal type=%q", m.Type)
	switch m.Type {
	case "kf":
		l.sess.RequestKeyframe()
	case "offer":
		l.handleOffer(m.SDP)
	case "ice":
		if m.Cand == nil {
			return
		}
		l.mu.Lock()
		pc := l.pc
		l.mu.Unlock()
		if pc == nil {
			return
		}
		mid := m.Cand.Mid
		cand := webrtc.ICECandidateInit{Candidate: m.Cand.Candidate, SDPMid: &mid}
		if err := pc.AddICECandidate(cand); err != nil {
			log.Printf("rtc: AddICECandidate: %v (%s)", err, m.Cand.Candidate)
		} else if l.srv.opts.debugRTC() {
			log.Printf("rtc: remote candidate %s", m.Cand.Candidate)
		}
	}
}

// debugRTC reports whether pion-level RTC diagnostics are enabled.
func (o Options) debugRTC() bool { return os.Getenv("TETHER_DEBUG") != "" }

func (l *link) handleOffer(sdp string) {
	l.mu.Lock()
	if l.closing || l.pc != nil || sdp == "" {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()

	cfg := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"}},
			{URLs: []string{"stun:stun.cloudflare.com:3478"}},
		},
	}
	var pc *webrtc.PeerConnection
	var err error
	if l.srv.rtcAPI != nil {
		pc, err = l.srv.rtcAPI.NewPeerConnection(cfg)
	} else {
		pc, err = webrtc.NewPeerConnection(cfg)
	}
	if err != nil {
		return
	}

	l.mu.Lock()
	if l.closing || l.pc != nil { // lost a race with close or duplicate offer
		l.mu.Unlock()
		_ = pc.Close()
		return
	}
	l.pc = pc
	conn := l.conn
	l.mu.Unlock()
	sendText := func(v any) {
		b, err := json.Marshal(v)
		if err != nil {
			log.Printf("rtc: marshal signal: %v", err)
			return
		}
		l.writeMu.Lock()
		defer l.writeMu.Unlock()
		_ = conn.SetWriteDeadline(writeDeadline())
		if werr := conn.WriteMessage(websocketText, b); werr != nil {
			log.Printf("rtc: signal write failed: %v", werr)
		}
	}

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			if l.srv.opts.debugRTC() {
				log.Printf("rtc: local gathering complete")
			}
			return
		}
		if l.srv.opts.debugRTC() {
			log.Printf("rtc: local candidate %s", c.ToJSON().Candidate)
		}
		init := c.ToJSON()
		sendText(signalMsg{Type: "ice", Cand: &struct {
			Candidate string `json:"candidate"`
			Mid       string `json:"sdpMid"`
			Line      uint16 `json:"sdpMLineIndex"`
		}{Candidate: init.Candidate, Mid: derefStr(init.SDPMid)}})
	})

	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		switch st {
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			// Forget the pc so a later offer (network switch, dc toggle)
			// can negotiate fresh; without this the first failure would
			// disable WebRTC for the whole WebSocket lifetime.
			l.mu.Lock()
			if l.pc == pc {
				l.pc = nil
			}
			l.mu.Unlock()
			l.revertToWS("rtc " + st.String())
		}
	})

	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		if dc.Label() != "tether" {
			_ = dc.Close()
			return
		}
		dc.OnOpen(func() { l.swapToDC(dc) })
		dc.OnClose(func() { l.revertToWS("datachannel closed") })
	})

	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: sdp,
	}); err != nil {
		log.Printf("rtc: remote description: %v", err)
		return
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		log.Printf("rtc: answer: %v", err)
		return
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		log.Printf("rtc: local description: %v", err)
		return
	}
	// Trickle ICE: answer goes out immediately; candidates follow as they
	// gather. Waiting for full gathering here stalls clients behind slow or
	// unreachable STUN paths.
	finalSDP := pc.LocalDescription().SDP
	if l.srv.opts.debugRTC() {
		log.Printf("rtc: answer %d bytes, %d candidates", len(finalSDP), strings.Count(finalSDP, "a=candidate:"))
	}
	sendText(signalMsg{Type: "answer", SDP: finalSDP})
}

// --- small helpers shared with server.go -----------------------------------

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
