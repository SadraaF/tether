package vt

// NewWithState constructs a Terminal and exposes its underlying State so
// callers can install scroll hooks and query extended mode bits.
func NewWithState(opts ...TerminalOption) (Terminal, *State) {
	t := New(opts...)
	return t, t.(*terminal).State
}

// Extensions to the vendored vt10x emulator: accessors for wire-protocol
// relevant modes and a scrollback capture hook.

// SetScrollHook registers fn to receive deep copies of lines as they scroll
// off the top of the primary screen within a full-screen scroll region.
// Called while the State lock is held; fn must not call back into State.
func (t *State) SetScrollHook(fn func(rows [][]Glyph)) {
	if fn == nil {
		t.scrollHook = nil
		return
	}
	t.scrollHook = func(lines []line) {
		rows := make([][]Glyph, len(lines))
		for i := range lines {
			rows[i] = lines[i]
		}
		fn(rows)
	}
}

// BracketedPaste reports DECSET 2004 state.
func (t *State) BracketedPaste() bool { return t.mode&modeBracketedPaste != 0 }

// AltScreen reports whether the alternate screen buffer is active.
func (t *State) AltScreen() bool { return t.mode&ModeAltScreen != 0 }

// AppCursor reports DECCKM application cursor keys mode.
func (t *State) AppCursor() bool { return t.mode&ModeAppCursor != 0 }

// MouseTracking granularly reports the active xterm mouse tracking mode.
func (t *State) MouseTracking() (normal, motion, any, sgr, x10 bool) {
	return t.mode&ModeMouseButton != 0,
		t.mode&ModeMouseMotion != 0,
		t.mode&ModeMouseMany != 0,
		t.mode&ModeMouseSgr != 0,
		t.mode&ModeMouseX10 != 0
}
