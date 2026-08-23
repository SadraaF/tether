// Package proto defines tether's binary WebSocket framing: a uniform
// header (type, sequence, length) followed by typed bodies. Sequence numbers
// order server→client state frames so clients can resume after drops by
// requesting replay of anything newer than the last frame they applied.
package proto

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Frame types, server→client.
const (
	TInit     uint8 = 0x01 // handshake: sid, grid size, next seq, modes
	TDelta    uint8 = 0x02 // incremental screen update
	TKeyframe uint8 = 0x03 // full grid snapshot
	TTitle    uint8 = 0x04 // terminal title (utf-8)
	TModes    uint8 = 0x05 // mode bitmask changed
	TClip     uint8 = 0x06 // OSC52 clipboard payload (plain text)
	TPing     uint8 = 0x07 // liveness probe; client answers CPong with same seq
	TExit     uint8 = 0x08 // PTY child process exited
	TPong     uint8 = 0x09 // immediate echo of client's CPing (RTT probe)
	TFile     uint8 = 0x0a // file offered by the server side (OSC 1337)
)

// Frame types, client→server.
const (
	CInit   uint8 = 0x01 // handshake: proto ver, sid, lastSeq seen
	CInput  uint8 = 0x02 // raw bytes for the PTY
	CResize uint8 = 0x03 // cols/rows request
	CPong   uint8 = 0x04 // reply to TPing, same seq
	CPing   uint8 = 0x05 // client RTT probe; server echoes TPong(seq)
	CRtt    uint8 = 0x06 // client-reported RTT EWMA, body u16 ms
)

// MaxPayload bounds a single frame body (keyframes for huge grids fit well under this).
const MaxPayload = 4 << 20

// HeaderSize is the fixed frame header length: type + seq + body length.
const HeaderSize = 9

// Mode bits shared by TInit/TModes bodies.
const (
	MouseBtnBit uint32 = 1 << iota
	MouseMotionBit
	MouseAnyBit
	MouseSGRBit
	MouseX10Bit
	BracketPasteBit
	AppCursorBit
	AltScreenBit
	CursorVisibleBit
)

// ErrShort is returned when a buffer does not contain a complete frame.
var ErrShort = errors.New("proto: short buffer")

// Frame is a decoded wire frame.
type Frame struct {
	Type uint8
	Seq  uint32
	Body []byte
}

// Encode appends the full wire representation of f to dst.
func Encode(dst []byte, f Frame) []byte {
	dst = append(dst, f.Type)
	dst = binary.LittleEndian.AppendUint32(dst, f.Seq)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(f.Body)))
	return append(dst, f.Body...)
}

// DecodeHeader parses type/seq/bodyLen out of a prefix of b.
func DecodeHeader(b []byte) (typ uint8, seq uint32, bodyLen int, err error) {
	if len(b) < HeaderSize {
		return 0, 0, 0, ErrShort
	}
	typ = b[0]
	seq = binary.LittleEndian.Uint32(b[1:5])
	bodyLen = int(binary.LittleEndian.Uint32(b[5:9]))
	if bodyLen < 0 || bodyLen > MaxPayload {
		return 0, 0, 0, fmt.Errorf("proto: bad body length %d", bodyLen)
	}
	return typ, seq, bodyLen, nil
}

// --- cell encoding -------------------------------------------------------
//
// Cells pack as: rune (signed varint), fg (uvarint), bg (uvarint), mode (u8).
// fg/bg are vt Color values: palette indices < 256 or truecolor 0x010000..rrggbb;
// defaults are the special values 0x01000000 / 0x01000001.

// GlyphAttr mirrors the vendored vt attr bits on the wire.
type GlyphAttr uint8

const (
	AttrReverse GlyphAttr = 1 << iota
	AttrUnderline
	AttrBold
	AttrGfx
	AttrItalic
	AttrBlink
)

// PutCell appends one packed cell.
func PutCell(dst []byte, ch rune, fg, bg uint32, attr GlyphAttr) []byte {
	dst = binary.AppendVarint(dst, int64(ch))
	dst = binary.AppendUvarint(dst, uint64(fg))
	dst = binary.AppendUvarint(dst, uint64(bg))
	return append(dst, byte(attr))
}

// CellReader walks packed cells produced by PutCell.
type CellReader struct{ b []byte }

func NewCellReader(b []byte) CellReader { return CellReader{b: b} }

// Next decodes the next cell; ok is false when the buffer is exhausted.
func (r *CellReader) Next() (ch rune, fg, bg uint32, attr GlyphAttr, ok bool) {
	v, n := binary.Varint(r.b)
	if n <= 0 {
		return
	}
	ch = rune(v)
	r.b = r.b[n:]
	fgU, n := binary.Uvarint(r.b)
	fg = uint32(fgU)
	if n <= 0 {
		ok = false
		return
	}
	r.b = r.b[n:]
	bgU, n := binary.Uvarint(r.b)
	bg = uint32(bgU)
	if n <= 0 {
		ok = false
		return
	}
	r.b = r.b[n:]
	if len(r.b) == 0 {
		return
	}
	attr = GlyphAttr(r.b[0])
	r.b = r.b[1:]
	return ch, fg, bg, attr, true
}

// Delta flag bits (Delta.Flags).
const (
	DeltaFlagsNone     uint8 = 0
	DeltaForceFullRows uint8 = 1 << 0 // rows list covers every row; client may fast-path
)

// Delta layout:
//
//	cursorX u16 | cursorY u16 | flags u8 |
//	scrollN u16 |             // primary-screen lines to replay as newlines
//	rowSpanCount u16 |
//	rowSpans: { y u16, spanCount u16, spans: { x u16, n u16, cells... } } ...
type DeltaCursor struct {
	X, Y    uint16
	Visible bool
}

// AppendDeltaHeader writes the fixed part of a delta body.
func AppendDeltaHeader(dst []byte, cur DeltaCursor, scrollN int, rowCount int) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, cur.X)
	dst = binary.LittleEndian.AppendUint16(dst, cur.Y)
	flags := uint8(0)
	if cur.Visible {
		flags |= 1
	}
	dst = append(dst, flags)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(scrollN))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(rowCount))
	return dst
}

// AppendRowSpan writes one row's span list opener.
func AppendRowSpan(dst []byte, y uint16, spanCount int) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, y)
	return binary.LittleEndian.AppendUint16(dst, uint16(spanCount))
}

// Keyframe layout:
//
//	cols u16 | rows u16 | cursorX u16 | cursorY u16 | flags u8 | cells...
//	cells count is cols*rows, row-major.
type GridHead struct {
	Cols, Rows uint16
	Cur        DeltaCursor
}

// AppendGridHead writes the fixed part of a keyframe body.
func AppendGridHead(dst []byte, g GridHead) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, g.Cols)
	dst = binary.LittleEndian.AppendUint16(dst, g.Rows)
	dst = binary.LittleEndian.AppendUint16(dst, g.Cur.X)
	dst = binary.LittleEndian.AppendUint16(dst, g.Cur.Y)
	flags := uint8(0)
	if g.Cur.Visible {
		flags |= 1
	}
	return append(dst, flags)
}

// ReadInit parses an S2C TInit body.
type ServerHello struct {
	SID     string
	Cols    uint16
	Rows    uint16
	NextSeq uint32
	Modes   uint32
}

func ReadServerHello(b []byte) (ServerHello, error) {
	var h ServerHello
	if len(b) < 1 {
		return h, ErrShort
	}
	n := int(b[0])
	if len(b) < 1+n+8 {
		return h, ErrShort
	}
	h.SID = string(b[1 : 1+n])
	o := 1 + n
	h.Cols = binary.LittleEndian.Uint16(b[o:])
	h.Rows = binary.LittleEndian.Uint16(b[o+2:])
	h.NextSeq = binary.LittleEndian.Uint32(b[o+4:])
	h.Modes = binary.LittleEndian.Uint32(b[o+8:])
	return h, nil
}

// AppendServerHello serializes an S2C TInit body.
func AppendServerHello(dst []byte, sid string, cols, rows uint16, nextSeq uint32, modes uint32) []byte {
	dst = append(dst, uint8(len(sid)))
	dst = append(dst, sid...)
	dst = binary.LittleEndian.AppendUint16(dst, cols)
	dst = binary.LittleEndian.AppendUint16(dst, rows)
	dst = binary.LittleEndian.AppendUint32(dst, nextSeq)
	return binary.LittleEndian.AppendUint32(dst, modes)
}

// ClientHello is a parsed C2S CInit body.
type ClientHello struct {
	Version uint8
	SID     string
	LastSeq uint32 // highest seq the client has applied; 0 => fresh view
}

func ReadClientHello(b []byte) (ClientHello, error) {
	var h ClientHello
	if len(b) < 2 {
		return h, ErrShort
	}
	h.Version = b[0]
	n := int(b[1])
	if len(b) < 2+n+4 {
		return h, ErrShort
	}
	h.SID = string(b[2 : 2+n])
	h.LastSeq = binary.LittleEndian.Uint32(b[2+n:])
	return h, nil
}

// AppendClientHello serializes a C2S CInit body.
func AppendClientHello(dst []byte, version uint8, sid string, lastSeq uint32) []byte {
	dst = append(dst, version, uint8(len(sid)))
	dst = append(dst, sid...)
	return binary.LittleEndian.AppendUint32(dst, lastSeq)
}
