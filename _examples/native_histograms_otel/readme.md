# Native histograms → OTel example

Demonstrates the end-to-end pipeline behind
`centrifuge.MetricsConfig.EnableNativeHistograms`:

```
Centrifuge metrics (native histograms)
    → prometheus.Registry
    → OTel Prometheus bridge (go.opentelemetry.io/contrib/bridges/prometheus)
    → OTel SDK PeriodicReader
    → stdoutmetric exporter (prints OTel JSON)
```

When the bridge sees native histograms it produces OTel
`ExponentialHistogram` data points — the high-fidelity form most OTel-native
backends prefer. Replace `stdoutmetric` with `otlpmetricgrpc` (or any other
OTLP exporter) to push to a real backend.

## Run

```sh
go run .
```

The active signal is `centrifuge_node_survey_duration_seconds` — the demo
calls `node.Survey(...)` in a loop, so the histogram fills up. Look for:

```json
{
  "Name": "centrifuge_node_survey_duration_seconds",
  "Data": {
    "DataPoints": [{
      "Count": 100,
      "Scale": 3,
      "PositiveBucket": { "Offset": -134, "Counts": [1,1,1,3,5,4,...] },
      ...
    }]
  }
}
```

The `Scale` + `PositiveBucket.{Offset,Counts}` shape is OTel
`ExponentialHistogram` — the form most OTel-native backends ingest with
full fidelity. (Classic Histograms would show `Bounds` and `BucketCounts`
instead.)

`centrifuge_client_command_duration_seconds` appears in the output as well,
also registered as a Histogram, but Count: 0 across all series — this demo
has no real client connections, so the metric isn't observed. The proof
that Summaries became Histograms is the metric being present at all with
`Scale` set rather than Summary's quantile shape.
