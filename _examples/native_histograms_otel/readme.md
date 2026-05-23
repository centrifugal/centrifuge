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

Centrifuge exposes both a Summary and a Histogram for the two duration
metrics that have historically been Summaries — `command_duration_seconds`
and `survey_duration_seconds`. When `EnableNativeHistograms` is on:

- The Summaries are no longer exposed (no-op internally; absent from output).
- The companion `_histogram` metrics switch to native (sparse, exponential)
  schema.
- The bridge translates native histograms to OTel `ExponentialHistogram` —
  the high-fidelity form most OTel-native backends prefer.

Replace `stdoutmetric` with `otlpmetricgrpc` (or any other OTLP exporter)
to push to a real backend.

## Run

```sh
go run .
```

The active signal is `centrifuge_node_survey_duration_seconds_histogram` —
the demo calls `node.Survey(...)` in a loop. Look for:

```json
{
  "Name": "centrifuge_node_survey_duration_seconds_histogram",
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
full fidelity.
