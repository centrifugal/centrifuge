package fossilutf8

import (
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/centrifugal/fdelta"
	"github.com/stretchr/testify/require"
)

// checkAlign checks that the aligned delta from source to target is valid UTF-8
// and creates target.
func checkAlign(t *testing.T, source, target []byte) (patch, aligned []byte) {
	t.Helper()
	patch = fdelta.Create(source, target)
	aligned = Align(patch, target)
	require.NotNil(t, aligned)
	require.True(t, utf8.Valid(aligned), "aligned delta %q", aligned)
	applied, err := fdelta.Apply(source, aligned)
	require.NoError(t, err)
	require.Equal(t, target, applied)
	return patch, aligned
}

func TestAlign(t *testing.T) {
	// Texts without repeated parts: a delta may copy a repeated part from another
	// place, which doesn't split a character.
	body := strings.Repeat("shared-body-", 12)
	tail := "0123456789abcdefghijklmnopqrstuvwxyz"
	ru := "Съешь же ещё этих мягких французских булок, да выпей чаю. Широкая электрификация южных губерний даст мощный толчок подъёму сельского хозяйства."
	zh := "的一是不了人我在有他这中大来上国个到说们为子和你地出道也时年得就那要下以生会自着去之过家学对"
	testCases := []struct {
		name   string
		source string
		target string
	}{
		{name: "same leading byte", source: body + "é", target: body + "è"},
		{name: "same continuation byte", source: body + "а" + tail, target: body + "Ѱ" + tail},
		{name: "replace letter", source: ru, target: strings.Replace(ru, "булок", "булак", 1)},
		{name: "insert letter", source: ru, target: strings.Replace(ru, "булок", "будлок", 1)},
		{name: "replace word", source: ru, target: strings.Replace(ru, "булок", "бубликов", 1)},
		{name: "replace han", source: zh, target: strings.Replace(zh, "中", "个", 1)},
		{name: "replace emoji", source: `{"bio":"` + body + `","status":"🟢"}`, target: `{"bio":"` + body + `","status":"🔴"}`},
		{name: "delete emoji", source: body + "🟢🔴" + body, target: body + "🔴" + body},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			patch, _ := checkAlign(t, []byte(tc.source), []byte(tc.target))
			require.False(t, utf8.Valid(patch), "the test case must split a character")
		})
	}
}

func TestAlign_ValidDeltaUnchanged(t *testing.T) {
	body := strings.Repeat("привет как дела ", 10)
	patch, aligned := checkAlign(t, []byte(`{"n":1,"text":"`+body+`"}`), []byte(`{"n":2,"text":"`+body+`"}`))
	require.Equal(t, patch, aligned)
}

func TestAlign_InvalidTarget(t *testing.T) {
	body := strings.Repeat("shared-body-", 12)
	target := []byte(body + "\xa8\xff")
	patch := fdelta.Create([]byte(body+"\xa9\xff"), target)
	require.False(t, utf8.Valid(patch))
	require.Nil(t, Align(patch, target))
}

// Publication data may be anything: a delta with many inserts in a long run of
// continuation bytes must not take time proportional to the run for each insert.
func TestAlign_LongContinuationRun(t *testing.T) {
	rnd := rand.New(rand.NewPCG(1, 2))
	source := make([]byte, 1<<19)
	for i := range source {
		source[i] = 0x80 | byte(rnd.IntN(64))
	}
	target := slices.Clone(source)
	for i := 20; i < len(target); i += 40 {
		target[i] = 0x80 | (target[i]+1)&0x3f
	}
	patch := fdelta.Create(source, target)
	start := time.Now()
	require.Nil(t, Align(patch, target))
	require.Less(t, time.Since(start), time.Second)
}

func TestAlign_MalformedPatch(t *testing.T) {
	target := []byte("abé")
	for _, patch := range []string{
		"\xa9",
		"4\n\xa9",
		"4\n3:ab\xc3",
		"5\n3:ab\xc3;",
		"4\n9:ab\xc3;",
		"4\n3:ab\xc31@0\xa9;",
		"4\n3:ab\xc31#0,0;",
		"4\n3:ab\xc31@0,0",
		"4\n3:ab\xc31@~~~~~~~~~~~~,0;",
		"4\n3:ab\xc31@0,~~~~~~~~~~~~;",
	} {
		require.Nil(t, Align([]byte(patch), target), "patch %q", patch)
	}
}

// alphabet mixes ASCII with 2, 3 and 4 byte characters sharing leading or
// continuation bytes, so deltas between texts of it often split characters.
var alphabet = []rune("ab {}\"éèабопѰ的一中个🟢🔴👍")

func text(b []byte) []rune {
	r := make([]rune, len(b))
	for i, c := range b {
		r[i] = alphabet[int(c)%len(alphabet)]
	}
	return r
}

// edit returns source with n characters at a replaced with insert.
func edit(source []rune, at, n int, insert []rune) []rune {
	at %= len(source) + 1
	n = min(n, len(source)-at)
	r := make([]rune, 0, len(source)-n+len(insert))
	r = append(r, source[:at]...)
	r = append(r, insert...)
	return append(r, source[at+n:]...)
}

func TestAlign_RandomEdits(t *testing.T) {
	rnd := rand.New(rand.NewPCG(1, 2))
	randText := func(n int) []rune {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rnd.IntN(256))
		}
		return text(b)
	}
	for range 5000 {
		// Repeat a part of the text so a delta has more to copy.
		part := randText(8 + rnd.IntN(64))
		source := append(append(randText(rnd.IntN(64)), part...), part...)
		target := source
		for range 1 + rnd.IntN(3) {
			target = edit(target, rnd.IntN(len(target)+1), rnd.IntN(3), randText(rnd.IntN(4)))
		}
		checkAlign(t, []byte(string(source)), []byte(string(target)))
	}
}

func FuzzAlign(f *testing.F) {
	f.Add([]byte("hello world, hello world, hello world"), uint16(14), uint8(1), []byte("x"))
	f.Add([]byte(strings.Repeat("\x05\x06\x07\x0c\x10", 10)), uint16(21), uint8(1), []byte("\x08"))
	f.Add([]byte(strings.Repeat("\x0d\x0e\x0f\x10", 10)), uint16(3), uint8(2), []byte("\x0f\x11\x12"))
	f.Fuzz(func(t *testing.T, source []byte, at uint16, n uint8, insert []byte) {
		s := text(source)
		checkAlign(t, []byte(string(s)), []byte(string(edit(s, int(at), int(n), text(insert)))))
	})
}

func BenchmarkAlign(b *testing.B) {
	ru := strings.Repeat("привет как дела сегодня хорошая погода ", 30)
	source := []byte(`{"n":1,"text":"` + ru + `"}`)
	for _, bc := range []struct {
		name   string
		target []byte
		split  bool
	}{
		{name: "valid", target: []byte(`{"n":2,"text":"` + ru + `"}`)},
		{name: "split", target: []byte(`{"n":1,"text":"` + strings.Replace(ru, "погода", "пагода", 1) + `"}`), split: true},
	} {
		patch := fdelta.Create(source, bc.target)
		if utf8.Valid(patch) == bc.split {
			b.Fatalf("unexpected delta for %s: %q", bc.name, patch)
		}
		b.Run(bc.name+"/create", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				fdelta.Create(source, bc.target)
			}
		})
		b.Run(bc.name+"/align", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				Align(patch, bc.target)
			}
		})
	}
}
