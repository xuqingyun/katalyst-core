# DRA provider selection

Enable `qrm-dra-manager` and supply `--dra-config-file` with a flat JSON/YAML
object. Every compiled provider participates in node matching. No match skips
DRA; multiple matches fail before any plugin is constructed. Matching runs once
at startup, and configuration is not reloaded.

Implement `Provider.Match(ctx, node)` and `Provider.New(config, runtime)`, register
with `RegisterProvider`, and import the provider in the agent binary. The returned
`Plugin.Run(ctx)` owns the complete vendor driver and stops on cancellation.

Core only selects and runs the provider. The vendor implementation owns its
kubelet registration/socket, Prepare/Unprepare, ResourceSlices, CDI and checkpoint.
Kubelet routes claims by allocation driver; Manager does not forward DRA RPCs.

Each provider declares `DefaultModels` and implements
`Match(ctx, node, models)` using the effective model list. Omit
`provider-models` to use compiled defaults, or configure lists per provider:

```yaml
provider-models:
  nvidia-compute-domain: [GB200, GB300]
```

A configured list replaces that provider's defaults. Other providers retain
compiled defaults; `{}` keeps all defaults and `provider: []` disables its
matching. Models are case-insensitive; empty or repeated models (including
across providers) and unknown provider names are rejected. Core consumes this
setting before constructing a vendor. Changing lists does not add hardware
capabilities. Configuration is read only at startup.
