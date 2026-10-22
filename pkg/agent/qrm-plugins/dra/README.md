# DRA provider selection

Enable `qrm-dra-plugin`. The containing agent supplies the concrete startup
`Config` through its options/configuration pipeline, not a file path:

```go
Config{
    ProviderModels: map[string][]string{"nvidia-compute-domain": {"GB200", "GB300"}},
    NativeFlags: NativeFlags{"namespace": json.RawMessage(`"platform"`)},
}
```

`Config.ProviderModels` is a typed `map[string][]string`. An absent provider
uses its registered `DefaultModels`; a configured list replaces the default
and `[]` disables it. Manager passes the Node and that list directly to
`Provider.Match(ctx, node, models)`. It does not normalize, sort or assign
model ownership. Each provider interprets model names. Unknown configured
providers are errors; overlapping lists are allowed unless multiple providers
actually match the Node. Matching/configuration run once at startup.

`Config.NativeFlags` is passed directly to `Provider.New(flags, runtime)`.
Providers treat inputs as read-only. Register with `RegisterProvider` and import
the provider in the agent binary. The returned `Plugin.Run(ctx)` owns the whole
vendor driver and stops on cancellation. No match skips DRA; multiple matches
fail before any plugin is constructed.

Core only selects and runs the provider. The vendor owns kubelet registration,
Prepare/Unprepare, ResourceSlices, CDI and its checkpoint. Kubelet routes claims
by allocation driver; Manager does not forward RPCs or track claims.

Selected providers run in a separate lifecycle within the same Agent. Factory
and Run failures keep DRA NotReady and retry serially with exponential delays
from one second to a thirty-second cap, without canceling the other Components.
Each provider reports Runtime.Ready only after completing native startup.
An unexpected exit resets readiness and retries; parent cancellation stops
retries. Unmatched nodes still skip DRA. Readiness does not imply an Agent
restart; the QRM chart uses only a readiness probe.
