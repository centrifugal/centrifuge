// Package metricsnap takes snapshots of Prometheus metrics for node info:
// what each metric did since the previous snapshot, flattened into
// key-value items, e.g. "centrifugo.client.num_reply_errors.method.subscribe".
//
// Counters, and the sample count and sum of summaries and histograms, are
// the change since the previous snapshot - so a value divided by the
// snapshot interval is a rate. Gauges and summary quantiles are current
// values. With Options.IncludeHistograms histograms report their count and
// sum the way summaries do, so rates and averages keep working when
// summaries are turned off (native histograms) or removed.
package metricsnap

import (
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Options of a Snapshotter.
type Options struct {
	// Prefixes, when set, keep only metrics whose name starts with one of
	// them.
	Prefixes []string
	// IncludeHistograms adds the count and sum of histograms. Off, while
	// every histogram has a summary twin, snapshots are smaller without
	// them.
	IncludeHistograms bool
}

// Snapshotter takes snapshots of metrics from a gatherer.
type Snapshotter struct {
	gatherer prometheus.Gatherer
	opts     Options

	mu sync.Mutex
	// previous holds the last seen cumulative value of every counter-like
	// series, to report changes.
	previous map[string]float64
}

// New creates a Snapshotter.
func New(gatherer prometheus.Gatherer, opts Options) *Snapshotter {
	return &Snapshotter{
		gatherer: gatherer,
		opts:     opts,
		previous: make(map[string]float64),
	}
}

// Snapshot gathers metrics and returns them flattened. The first snapshot
// reports counters and counts since the process started.
func (s *Snapshotter) Snapshot() (map[string]float64, error) {
	mfs, err := s.gatherer.Gather()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	items := make(map[string]float64)
	for _, mf := range mfs {
		name := mf.GetName()
		if !s.allowed(name) {
			continue
		}
		base, ok := baseKey(name)
		if !ok {
			continue
		}
		for _, m := range mf.GetMetric() {
			labels := labelPath(m.GetLabel())
			switch mf.GetType() {
			case dto.MetricType_COUNTER:
				items[key(base, "", labels)] = s.change(name, "", labels, m.GetCounter().GetValue())
			case dto.MetricType_GAUGE:
				items[key(base, "", labels)] = m.GetGauge().GetValue()
			case dto.MetricType_SUMMARY:
				summary := m.GetSummary()
				items[key(base, "sum", labels)] = s.change(name, "sum", labels, summary.GetSampleSum())
				items[key(base, "count", labels)] = s.change(name, "count", labels, float64(summary.GetSampleCount()))
				for _, q := range summary.GetQuantile() {
					if math.IsNaN(q.GetValue()) {
						continue
					}
					items[key(base, "quantile."+quantileString(q.GetQuantile()), labels)] = q.GetValue()
				}
			case dto.MetricType_HISTOGRAM:
				if !s.opts.IncludeHistograms {
					continue
				}
				// Classic and native histograms alike carry the sample
				// count and sum.
				histogram := m.GetHistogram()
				items[key(base, "sum", labels)] = s.change(name, "sum", labels, histogram.GetSampleSum())
				items[key(base, "count", labels)] = s.change(name, "count", labels, float64(histogram.GetSampleCount()))
			}
		}
	}
	return items, nil
}

// change returns how much a cumulative value grew since the previous
// snapshot. A value which went down - the metric was reset - counts from
// zero.
func (s *Snapshotter) change(name, suffix, labels string, value float64) float64 {
	id := name + "|" + suffix + "|" + labels
	prev, ok := s.previous[id]
	s.previous[id] = value
	if !ok || value < prev {
		return value
	}
	return value - prev
}

func (s *Snapshotter) allowed(name string) bool {
	if len(s.opts.Prefixes) == 0 {
		return true
	}
	for _, p := range s.opts.Prefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// baseKey turns a metric name into its dotted key: namespace, subsystem
// and the rest of the name, e.g. "centrifugo_client_num_reply_errors" into
// "centrifugo.client.num_reply_errors". A name without a subsystem is
// skipped.
func baseKey(name string) (string, bool) {
	parts := strings.SplitN(name, "_", 3)
	if len(parts) < 2 {
		return "", false
	}
	return strings.Join(parts, "."), true
}

// labelPath is "name.value" pairs of labels with a value, in the order
// Prometheus gives them (sorted by name).
func labelPath(pairs []*dto.LabelPair) string {
	chunks := make([]string, 0, 2*len(pairs))
	for _, pair := range pairs {
		if pair.GetValue() == "" {
			continue
		}
		chunks = append(chunks, pair.GetName(), pair.GetValue())
	}
	return strings.Join(chunks, ".")
}

func key(base, suffix, labels string) string {
	k := base
	if suffix != "" {
		k += "." + suffix
	}
	if labels != "" {
		k += "." + labels
	}
	return k
}

// quantileString formats a quantile the way node info always did: 0.5 as
// "500" -> "50", 0.99 as "990" -> "99", 0.999 as "999".
func quantileString(val float64) string {
	q := fmt.Sprintf("%d", int(val*1000))
	if len(q) > 2 {
		q = strings.TrimSuffix(q, "0")
	}
	return q
}
