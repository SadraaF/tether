package session

import (
	"bytes"
	"encoding/base64"
	"os"
	"strconv"
	"strings"
)

// osc scans the PTY byte stream for two out-of-band OSC sequences:
//
//	ESC ] 52 [;sel] ; <base64> (BEL | ESC \)          clipboard set
//	ESC ] 1337 ; File = k=v;... : <base64> (BEL|ESC\) file offer (iTerm2)
//
// Matched payloads are decoded and delivered on the respective channels; raw
// bytes still flow to the vt emulator untouched. The scanner is incremental
// across chunk splits.

const (
	maxOscClipboard = 256 << 10 // clipboard payloads stay small
	maxOscFile      = 48 << 20  // base64 budget for in-band file offers (~36 MB raw)
)

// FileOffer is a decoded OSC 1337 file emitted by something on the VPS.
type FileOffer struct {
	Name string
	Data []byte
}

type osc52 struct {
	state int
	acc   []byte
	out   chan string
	files chan FileOffer
}

func newOsc52() *osc52 {
	return &osc52{
		out:   make(chan string, 4),
		files: make(chan FileOffer, 2),
	}
}

const (
	oGround = iota
	oEsc
	oOsc     // inside OSC body, accumulating
	oOscEsc  // saw ESC inside OSC; may be start of ST
	oDiscard // OSC we don't care about; scan for terminator only
)

// capFor returns the accumulation budget for the current body: file offers
// are allowed to grow much larger than clipboard sets.
func (s *osc52) capFor() int {
	if bytes.HasPrefix(s.acc, []byte("1337;File=")) {
		return maxOscFile
	}
	return maxOscClipboard
}

// feed advances the scanner over p.
func (s *osc52) feed(p []byte) {
	for _, b := range p {
		switch s.state {
		case oGround:
			if b == 0x1b {
				s.state = oEsc
			}
		case oEsc:
			if b == ']' {
				s.state = oOsc
				s.acc = s.acc[:0]
			} else if b != 0x1b {
				s.state = oGround
			}
		case oOsc:
			switch b {
			case 0x07:
				s.finish()
				s.state = oGround
			case 0x1b:
				s.state = oOscEsc
			default:
				if len(s.acc) < s.capFor() {
					s.acc = append(s.acc, b)
				} else {
					s.state = oDiscard
				}
			}
		case oOscEsc:
			if b == '\\' {
				s.finish()
				s.state = oGround
			} else {
				s.state = oGround
				if b == 0x1b {
					s.state = oEsc
				}
			}
		case oDiscard:
			switch b {
			case 0x07:
				s.state = oGround
			case 0x1b:
				s.state = oOscEsc
			}
		}
	}
}

// finish parses the accumulated OSC body and dispatches it.
func (s *osc52) finish() {
	body := string(s.acc)
	if os.Getenv("TETHER_DEBUG") != "" {
		head := body
		if len(head) > 24 {
			head = head[:24]
		}
		println("osc finish: len=", len(body), " head=", strconv.Quote(head))
	}
	switch {
	case strings.HasPrefix(body, "52;") || body == "52":
		s.finishClipboard(body)
	case strings.HasPrefix(body, "1337;File="):
		s.finishFile(body)
	}
}

func (s *osc52) finishClipboard(body string) {
	parts := strings.SplitN(body, ";", 3) // "52", optional sel, base64
	var b64 string
	switch len(parts) {
	case 2:
		b64 = parts[1]
	case 3:
		b64 = parts[2]
	default:
		return
	}
	if b64 == "" || len(b64)%4 == 1 {
		return
	}
	dec, err := decodeBase64Loose(b64)
	if err != nil || len(dec) == 0 {
		return
	}
	select {
	case s.out <- string(dec):
	default: // consumer backlogged; clipboard sets are best-effort
	}
}

// finishFile parses an iTerm2-style file offer:
//
//	File=name=<urlb64>;size=<n>[;inline=...][:<std-b64 data>]
func (s *osc52) finishFile(body string) {
	rest := body[len("1337;"):]           // "File=..."
	colon := strings.IndexByte(rest, ':') // first colon separates args from data
	if colon < 0 {
		return
	}
	args, b64 := rest[:colon], rest[colon+1:]
	if !strings.HasPrefix(args, "File=") || b64 == "" {
		return
	}
	name := "file.bin"
	size := -1
	for _, kv := range strings.Split(strings.TrimPrefix(args, "File="), ";") {
		eq := strings.IndexByte(kv, '=')
		if eq <= 0 {
			continue
		}
		k, v := kv[:eq], kv[eq+1:]
		switch k {
		case "name":
			if dec, err := decodeBase64Loose(v); err == nil && len(dec) > 0 {
				name = sanitizeFilename(string(dec))
			}
		case "size":
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				size = n
			}
		}
	}
	dec, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(dec) == 0 {
		return
	}
	if size >= 0 && len(dec) != size { // truncated or mangled offer
		return
	}
	offer := FileOffer{Name: name, Data: dec}
	select {
	case s.files <- offer:
	default: // consumer backlogged; a newer offer supersedes ours anyway
	}
}

// decodeBase64Loose accepts standard or URL-safe alphabets.
func decodeBase64Loose(b64 string) ([]byte, error) {
	// Generators like GNU base64 wrap output at 76 columns; strip whitespace
	// so wrapped payloads decode instead of failing on the first newline.
	b64 = strings.Map(func(r rune) rune {
		if r == '\r' || r == '\n' || r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, b64)
	dec, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		dec, err = base64.URLEncoding.DecodeString(b64)
	}
	return dec, err
}

// sanitizeFilename strips path components and control characters so an offer
// can never suggest writing outside a download dialog's default name.
func sanitizeFilename(name string) string {
	name = strings.Map(func(r rune) rune {
		if r < 32 || r == '/' || r == '\\' {
			return -1
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." {
		return "file.bin"
	}
	return name
}
