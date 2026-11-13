# Node-selected native DRA plugin lifecycle

Manager selects at most one complete vendor driver per node. Its provider contract
is `Plugin.Start(context.Context)` / `Plugin.Stop(context.Context)`; a factory
receives the resolved configuration, NodeName, client configuration and managed
running context. No Prepare, Unprepare, inventory, RPC, publisher or checkpoint
protocol is part of Katalyst's interface. Native vendor code owns those behaviors.
`PluginFuncs` connects existing native lifecycle method names without copying the
data plane. The former Core StaticPolicy/ResourceBackend/state framework has been
removed; the NVIDIA factory returns the complete native plugin lifecycle.

Every enabled candidate must have a Kubernetes `nodeSelector` LabelSelector with
an explicit positive allowlist (`matchLabels` or `In` values). At startup the
Manager reads the Node through the containing agent's fetcher, resolves all
matches, and only then calls a vendor factory. Zero matches starts no plugin;
multiple matches fail before any vendor construction. Missing selectors and
negative-only rules are rejected. Required means a selected plugin must work,
not that every machine must match it. Hardware auto-detection remains an extra
check after selection; unsupported optional nodes do not fall back to another
vendor. General constructor/startup errors remain visible.

Selection is fixed during the running instance. A label or configuration change
requires Stop/restart; Manager does not change drivers under allocated Claims.
Config maps and selectors are copied at construction. The node fetcher must
return the configured Node identity and honor context cancellation.

Startup cancellation interrupts the factory, vendor Start and optional Ready.
After startup, parent cancellation requests graceful Stop using an independent
shutdown context (default `shutdownTimeoutSeconds: 30`). Running cancellation
follows successful vendor drain/cleanup. Failed or timed-out cleanup retains the
instance, prevents restart and can be retried. Every non-nil object returned by
a factory, including a partial failure, is included in cleanup. Plugins must
honor their contexts; a context cannot forcibly interrupt a vendor's blocking
hardware library call.

Optional `ReadyPlugin` waits for registration/initialization. Optional
`ObservablePlugin` exposes Done/Err for terminal failure; Manager stops the
failed instance and exposes its error to the agent PluginWrapper. Transient
device health and native retry logic stay with the vendor.

Identity and vendor-specific paths stay in config. Manager does not invent a
socket filename; native constructors validate their actual registration/RPC
endpoints. Keep TCE host/container data and runtime paths in the Adapter/Deploy
configuration. Claims are routed directly by kubelet to the registered vendor.
