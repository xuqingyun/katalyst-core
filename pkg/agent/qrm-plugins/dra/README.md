# DRA driver instances

The generic `Manager` runs one registered plugin per configured driver. Kubelet
selects the driver's Unix endpoint from the ResourceClaim allocation. The manager
handles construction, startup rollback and reverse-order shutdown.

Embedding agents supply their compiled vendor implementations using
`RegisterProvider` and `NewManager`, or pass a private factory map to
`NewManagerWithProviders`. The latter snapshots the factory map and configuration
at construction. A `ProviderFactory` receives the resolved `PluginConfig` and
node `Runtime`, and returns a `skeleton.GenericPlugin` whose `Name()` equals the
configured driver. Return a complete native vendor plugin directly when it
owns registration, RPC, publication, checkpoint and cleanup. The optional Core
framework remains available: a factory may build a `ResourceBackend`, inject it
and a `ResourceSlicePublisher` into `NewStaticPolicy`, then return
`NewRegisteredPlugin(policy, runtime.RegistrationDir)`. Neither path is required
by the factory contract.

Use factory closures to inject environment-specific clients or configuration.
Normalize migration defaults before creating the manager. Core does not import
vendor packages or automatically enable a top-level agent. Optional hardware
probes return `ErrUnsupportedNode`; `autoDetect` permits skipping those providers.
Unknown providers and invalid configuration fail startup.

`Publisher` provides driver-scoped ResourceSlice reconciliation and ownership
checks. It defaults to driver-qualified names; `UseGenerateName` creates slices
with the native `<node>-<driver>-` prefix and an API-assigned suffix. Shard labels
keep generated names associated with their inventory partitions across restarts.
`AdoptExisting` validates native inventory before taking ownership, and
`PreserveExistingNames` retains existing resource identity. `SliceName` remains
available for providers that need explicit names. Keep provider-specific
migration checks and state defaults in the embedding application. Each driver must retain distinct
socket, state/checkpoint, CDI and service resources.

The dual-driver tests use real Unix gRPC endpoints with fake backends and API
clients. They check registration, mixed-claim Prepare/Unprepare isolation,
ResourceSlice separation, rollback, and retry after failed shutdown.

## Cancellable native plugin lifecycle

A vendor with initialization that waits on I/O or informer readiness should
add `StartWithContext(context.Context) error` alongside its existing Start/Stop
methods. Manager detects this optional interface and passes its lifecycle
context. Factories receive the same context in `Runtime.Context`; use it for
configuration discovery or hardware probes. Existing Start-only plugins remain
compatible. Their own blocking work must still have cancellation or deadlines.

The containing agent invokes `Manager.StartWithContext` with its Run context.
Stop cancels the current attempt before waiting for the operation lock, including
a pending-stop guard before cancel is published. Cancellation never becomes an
optional-provider skip: it stops later construction and rolls back active
plugins. Shutdown failures remain available for retry, and successful restart
gets a fresh context. No vendor-specific checkpoint or RPC protocol is part of
this lifecycle interface.
