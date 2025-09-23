package filter

import (
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/google/cel-go/cel"
)

func buildTestCompareFilter() *protocol.FilterNode {
	return &protocol.FilterNode{
		Op: OpAnd,
		Nodes: []*protocol.FilterNode{
			{
				Op:  OpLeaf,
				Key: "count",
				Cmp: CompareGT,
				Val: "42",
			},
			{
				Op:  OpLeaf,
				Key: "price",
				Cmp: CompareGTE,
				Val: "99.5",
			},
			{
				Op:  OpLeaf,
				Key: "ticker",
				Cmp: CompareContains,
				Val: "GOO",
			},
		},
	}
}

func buildTestCompareCELProgram() cel.Program {
	env, err := cel.NewEnv(
		cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)),
	)
	if err != nil {
		panic(err)
	}

	expr := `int(tags["count"]) > 42 && double(tags["price"]) >= 99.5 && tags["ticker"].contains("GOO")`

	ast, issues := env.Parse(expr)
	if issues != nil && issues.Err() != nil {
		panic(issues.Err())
	}

	checked, issues := env.Check(ast)
	if issues != nil && issues.Err() != nil {
		panic(issues.Err())
	}

	prg, err := env.Program(checked)
	if err != nil {
		panic(err)
	}
	return prg
}

func BenchmarkFilterCompareFilterNode10k(b *testing.B) {
	filter := buildTestCompareFilter()
	const subscribers = 10000

	tags := map[string]string{
		"count":  "100",
		"price":  "120.0",
		"ticker": "GOOG",
	}

	subs := make([]*protocol.FilterNode, subscribers)
	for i := 0; i < subscribers; i++ {
		subs[i] = filter
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		for _, sub := range subs {
			_, _ = Match(sub, tags)
		}
	}
}

func BenchmarkFilterCompareCEL10k(b *testing.B) {
	prg := buildTestCompareCELProgram()

	const subscribers = 10000

	subs := make([]cel.Program, subscribers)
	for i := 0; i < subscribers; i++ {
		subs[i] = prg
	}

	tags := map[string]string{
		"count":  "100",
		"price":  "120.0",
		"ticker": "GOOG",
	}

	// Create a shared activation map outside the loop to reduce allocations.
	activation := map[string]any{"tags": tags}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		for _, sub := range subs {
			out, _, err := sub.Eval(activation)
			if err != nil {
				b.Fatal(err)
			}
			// cast to bool
			if out.Value().(bool) != true {
				b.Fatal("unexpected result")
			}
		}
	}
}

func BenchmarkFilterCompareMemoryFilterNode(b *testing.B) {
	for b.Loop() {
		buildTestCompareFilter()
	}
}

func BenchmarkFilterCompareMemoryFilterNodeHash(b *testing.B) {
	for b.Loop() {
		f := buildTestCompareFilter()
		Hash(f)
	}
}

func BenchmarkFilterCompareCELMemory(b *testing.B) {
	for b.Loop() {
		buildTestCompareCELProgram()
	}
}
