package filter

import (
	"testing"

	"github.com/centrifugal/protocol"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
)

var testTags = map[string]string{
	"count":  "100",
	"price":  "120.0",
	"ticker": "GOOG",
}

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

const testCelExpr = `int(tags["count"]) > 42 && double(tags["price"]) >= 99.5 && tags["ticker"].contains("GOO")`

var testEnv *cel.Env

func init() {
	var err error
	testEnv, err = cel.NewEnv(
		cel.Variable("tags", cel.MapType(cel.StringType, cel.StringType)),
	)
	if err != nil {
		panic(err)
	}
}

func buildTestCompareCELProgram(expr string) cel.Program {
	ast, issues := testEnv.Parse(expr)
	if issues != nil && issues.Err() != nil {
		panic(issues.Err())
	}

	checked, issues := testEnv.Check(ast)
	if issues != nil && issues.Err() != nil {
		panic(issues.Err())
	}

	prg, err := testEnv.Program(checked)
	if err != nil {
		panic(err)
	}
	return prg
}

func BenchmarkCompare_FilterNode(b *testing.B) {
	filter := buildTestCompareFilter()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, _ := Match(filter, testTags)
		if !res {
			b.Fatal("unexpected result")
		}
	}
}

func BenchmarkCompare_CEL(b *testing.B) {
	prg := buildTestCompareCELProgram(testCelExpr)

	// Create a shared activation map outside the loop to reduce allocations.
	activation := map[string]any{"tags": testTags}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		out, _, err := prg.Eval(activation)
		if err != nil {
			b.Fatal(err)
		}
		if out != types.True {
			b.Fatal("unexpected result")
		}
	}
}

func BenchmarkCompare10k_FilterNode(b *testing.B) {
	filter := buildTestCompareFilter()
	const subscribers = 10000

	subs := make([]*protocol.FilterNode, subscribers)
	for i := 0; i < subscribers; i++ {
		subs[i] = filter
	}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		for _, sub := range subs {
			res, _ := Match(sub, testTags)
			if !res {
				b.Fatal("unexpected result")
			}
		}
	}
}

func BenchmarkCompare10k_CEL(b *testing.B) {
	prg := buildTestCompareCELProgram(testCelExpr)

	const subscribers = 10000

	subs := make([]cel.Program, subscribers)
	for i := 0; i < subscribers; i++ {
		subs[i] = prg
	}

	// Create a shared activation map outside the loop to reduce allocations.
	activation := map[string]any{"tags": testTags}

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		for _, sub := range subs {
			out, _, err := sub.Eval(activation)
			if err != nil {
				b.Fatal(err)
			}
			if out != types.True {
				b.Fatal("unexpected result")
			}
		}
	}
}

func BenchmarkCompareCompile_FilterNode(b *testing.B) {
	for b.Loop() {
		buildTestCompareFilter()
	}
}

func BenchmarkCompareCompile_FilterNodeHash(b *testing.B) {
	for b.Loop() {
		f := buildTestCompareFilter()
		Hash(f)
	}
}

func BenchmarkCompareCompile_CEL(b *testing.B) {
	for b.Loop() {
		buildTestCompareCELProgram(testCelExpr)
	}
}
