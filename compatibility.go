package centrifuge

const (
	UseSeqGen uint64 = 1 << iota // Use Seq and Gen fields.
)

// CompatibilityFlags is a global set of legacy features we support
// for backwards compatibility.
//
// Should be removed with v1 library release.
var CompatibilityFlags uint64

func hasFlag(flags, flag uint64) bool {
	return flags&flag != 0
}
