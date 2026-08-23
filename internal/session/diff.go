package session

import (
	"encoding/binary"

	"tether/internal/proto"
	"tether/internal/vt"
)

// screenDiff maintains the last state encoded into a wire frame and produces
// minimal delta frames against it. All methods require the vt State lock to
// be held: snapshots run on the coalescer goroutine, while the scroll hook
// fires inside terminal.Write under the same lock.
type screenDiff struct {
	cols, rows int
	sent       [][]wireCell

	pendingScroll int // primary-screen lines scrolled off, replayable as newlines

	curX, curY    int
	curVisible    bool
	firstSnapshot bool // suppresses a pure-cursor no-op first frame
}

type wireCell struct {
	ch   rune
	fg   uint32
	bg   uint32
	attr proto.GlyphAttr
}

const (
	wDefaultFG = uint32(vt.DefaultFG)
	wDefaultBG = uint32(vt.DefaultBG)
)

func blankRow(cols int) []wireCell {
	r := make([]wireCell, cols)
	for i := range r {
		r[i] = wireCell{ch: ' ', fg: wDefaultFG, bg: wDefaultBG}
	}
	return r
}

func newScreenDiff(cols, rows int) *screenDiff {
	s := &screenDiff{cols: cols, rows: rows, curVisible: true, firstSnapshot: true}
	s.sent = make([][]wireCell, rows)
	for i := range s.sent {
		s.sent[i] = blankRow(cols)
	}
	return s
}

// resize rebuilds the reference grid; the next emission is a keyframe.
func (s *screenDiff) resize(cols, rows int) {
	s.cols, s.rows = cols, rows
	s.sent = make([][]wireCell, rows)
	for i := range s.sent {
		s.sent[i] = blankRow(cols)
	}
}

// onScroll is the vt scroll-hook target.
func (s *screenDiff) onScroll(n int) { s.pendingScroll += n }

func (s *screenDiff) cursor(st *vt.State) proto.DeltaCursor {
	cur := st.Cursor()
	return proto.DeltaCursor{X: uint16(cur.X), Y: uint16(cur.Y), Visible: st.CursorVisible()}
}

// keyframe encodes the entire current grid and resets the reference to it.
func (s *screenDiff) keyframe(st *vt.State) []byte {
	cols, rows := st.Size()
	if cols != s.cols || rows != s.rows {
		s.resize(cols, rows)
	}
	body := make([]byte, 0, cols*rows*6+16)
	body = proto.AppendGridHead(body, proto.GridHead{
		Cols: uint16(cols), Rows: uint16(rows),
		Cur: s.cursor(st),
	})
	for y := 0; y < rows; y++ {
		row := s.sent[y]
		for x := 0; x < cols; x++ {
			c := toWire(st.Cell(x, y))
			row[x] = c
			body = proto.PutCell(body, c.ch, c.fg, c.bg, c.attr)
		}
	}
	s.curX, s.curY = int(s.cursor(st).X), int(s.cursor(st).Y)
	s.curVisible = st.CursorVisible()
	s.firstSnapshot = false
	return body
}

type span struct{ x, n int }

// delta encodes changes between the reference grid and st's current grid.
// It returns nil when nothing observable changed since the last emission.
func (s *screenDiff) delta(st *vt.State) []byte {
	cols, rows := st.Size()
	if cols != s.cols || rows != s.rows {
		s.resize(cols, rows)
		return s.keyframe(st) // resize always emits a keyframe
	}

	var updatedRows []int
	var spansByRow map[int][]span

	for y := 0; y < rows; y++ {
		sent := s.sent[y]
		var sp []span
		runX, runN := -1, 0
		gap := 0
		for x := 0; x < cols; x++ {
			c := toWire(st.Cell(x, y))
			if c != sent[x] {
				sent[x] = c
				if runX >= 0 && gap <= 2 {
					runN += gap + 1
					gap = 0
					continue
				}
				if runX >= 0 {
					sp = append(sp, span{runX, runN})
				}
				runX, runN, gap = x, 1, 0
			} else if runX >= 0 {
				gap++
			}
		}
		if runX >= 0 {
			sp = append(sp, span{runX, runN})
		}
		if len(sp) > 0 {
			updatedRows = append(updatedRows, y)
			if spansByRow == nil {
				spansByRow = make(map[int][]span)
			}
			spansByRow[y] = sp
		}
	}

	cur := st.Cursor()
	cursorChanged := cur.X != s.curX || cur.Y != s.curY ||
		st.CursorVisible() != s.curVisible || s.pendingScroll > 0

	if len(updatedRows) == 0 && !cursorChanged {
		if !s.firstSnapshot {
			return nil
		}
		// First frame after attach is handled as a keyframe by the caller;
		// reaching here means an empty delta would be pointless.
		return nil
	}

	body := make([]byte, 0, 32+len(updatedRows)*16)
	body = proto.AppendDeltaHeader(body, s.cursor(st), s.pendingScroll, len(updatedRows))
	for _, y := range updatedRows {
		spans := spansByRow[y]
		body = proto.AppendRowSpan(body, uint16(y), len(spans))
		sent := s.sent[y]
		for _, sp := range spans {
			body = binary.LittleEndian.AppendUint16(body, uint16(sp.x))
			body = binary.LittleEndian.AppendUint16(body, uint16(sp.n))
			for i := sp.x; i < sp.x+sp.n; i++ {
				c := sent[i]
				body = proto.PutCell(body, c.ch, c.fg, c.bg, c.attr)
			}
		}
	}

	s.pendingScroll = 0
	s.curX, s.curY = cur.X, cur.Y
	s.curVisible = st.CursorVisible()
	s.firstSnapshot = false
	return body
}

func toWire(g vt.Glyph) wireCell {
	return wireCell{
		ch: g.Char,
		fg: uint32(g.FG),
		bg: uint32(g.BG),
		// Reverse video is applied server-side (FG/BG swapped in the cell),
		// so bit 0 must not reach the client: SgrWriter would invert the
		// already-inverted pair and reversed cells would render normal.
		attr: proto.GlyphAttr(g.Mode) & 0x3E,
	}
}
