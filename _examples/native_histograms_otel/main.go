// Example demonstrating Centrifuge metrics exported to OpenTelemetry via the
// client_golang Prometheus bridge.
//
// When centrifuge.MetricsConfig.EnableNativeHistograms is true, Centrifuge
// constructs all distribution metrics as Prometheus native (sparse,
// exponential) histograms. The OpenTelemetry contrib Prometheus bridge
// detects native form and emits OTel ExponentialHistogram data points, which
// most OTel-native backends (incl. GCP) ingest with full fidelity.
//
// Run: go run .  (prints OTel metric JSON to stdout every 5 seconds)
package main

import (
	"context"
	"log"
	"time"

	"github.com/centrifugal/centrifuge"
	"github.com/prometheus/client_golang/prometheus"
	bridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
)

func main() {
	reg := prometheus.NewRegistry()

	node, err := centrifuge.New(centrifuge.Config{
		LogLevel:   centrifuge.LogLevelError,
		LogHandler: func(e centrifuge.LogEntry) { log.Printf("%s: %v", e.Message, e.Fields) },
		Metrics: centrifuge.MetricsConfig{
			RegistererGatherer:     reg,
			EnableNativeHistograms: true,
			GetChannelNamespaceLabel: func(channel string) string {
				return "demo"
			},
		},
	})
	if err != nil {
		log.Fatalf("centrifuge.New: %v", err)
	}

	node.OnSurvey(func(event centrifuge.SurveyEvent, cb centrifuge.SurveyCallback) {
		cb(centrifuge.SurveyReply{Data: []byte("pong")})
	})

	if err := node.Run(); err != nil {
		log.Fatalf("node.Run: %v", err)
	}
	defer func() { _ = node.Shutdown(context.Background()) }()

	exporter, err := stdoutmetric.New()
	if err != nil {
		log.Fatalf("stdoutmetric.New: %v", err)
	}
	producer := bridge.NewMetricProducer(bridge.WithGatherer(reg))
	reader := metric.NewPeriodicReader(exporter,
		metric.WithProducer(producer),
		metric.WithInterval(5*time.Second),
	)
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() { _ = meterProvider.Shutdown(context.Background()) }()

	go generateTraffic(node)

	log.Println("running for 30 seconds — watch for ExponentialHistogram data points in stdout JSON...")
	time.Sleep(30 * time.Second)
	log.Println("done")
}

func generateTraffic(node *centrifuge.Node) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		if _, err := node.Publish("test:channel", []byte(`{"hello":"world"}`)); err != nil {
			log.Printf("publish error: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if _, err := node.Survey(ctx, "ping", nil, ""); err != nil {
			log.Printf("survey error: %v", err)
		}
		cancel()
	}
}
