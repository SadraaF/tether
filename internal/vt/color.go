package vt

// ANSI color values
const (
	Black Color = iota
	Red
	Green
	Yellow
	Blue
	Magenta
	Cyan
	LightGrey
	DarkGrey
	LightRed
	LightGreen
	LightYellow
	LightBlue
	LightMagenta
	LightCyan
	White
)

// Default colors are potentially distinct to allow for special behavior.
// For example, a transparent background. Otherwise, the simple case is to
// map default colors to another color.
const (
	DefaultFG Color = 1<<24 + iota
	DefaultBG
	DefaultCursor
)

// RGBFlag marks 24-bit truecolor values (r<<16|g<<8|b). Without it, dark RGB
// values like #000080 would collide with xterm palette indices.
const RGBFlag Color = 1 << 25

// Color maps to the ANSI colors [0, 16) and the xterm colors [16, 256).
type Color uint32

// ANSI returns true if Color is within [0, 16).
func (c Color) ANSI() bool {
	return (c < 16)
}
