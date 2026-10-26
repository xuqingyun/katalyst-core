# Internal Kubernetes API compatibility

This feature branch builds with effective client-go and Kubernetes HTTP SDK 0.35.0. kubelet uses the pinned personal fork containing the internal PodResources codecs; no local absolute replace is required.

| Path | Compatibility behavior | Verification scope |
| --- | --- | --- |
| PodResources List/GetAllocatable | `--podresources-protocol=internal118` explicitly selects the 1.18 response layout; default `internal135` preserves new memory fields | Real gRPC wire, metadata/device/topology/quantity assertions |
| Reporter | Use actual RPC and required resource validation; internal adapter falls back to checkpoint only for Unimplemented when enabled | Error propagation/capability tests; node lab results tracked separately |
| KCNR | Refresh NotFound invalidates cache and recreates metadata-only object; subsequent reports supply fresh resources | Deletion, competing creation, failed creation, race tests |
| WatchList | Disabled before GenericClientSet creates clients; explicit KUBE_FEATURE_WatchListClient value is honored even if Kubernetes replaced the environment gates | Environment/component-gate tests; internal adapter independently retains its own StreamingList contract |
| pods/eviction | Discover the policy version advertised on core/v1 pods/eviction; 1.18 uses v1beta1 and 1.35 uses v1 | UID, annotation, grace, dry-run and PDB error tests; real API dry-run |
| IHPA/HPA | Prefer served autoscaling/v2, otherwise v2beta2; CREATE/PATCH plus informer LIST/WATCH convert to shared v2 models | External metrics/behavior round-trip, manager tests, served API tests |
| VPA | Use native pods/resize when served; otherwise retain legacy annotation submission | CPU/memory-only patch tests, untouched-resource preservation, creation-time restart-policy checks, real API submission |
| Tide | Align descheduler/autoscaler to the Kubernetes 1.35 line; preserve disabled DRA/CSI snapshot features and scheduler default parallelism | Full controller build and Tide regression |

HPA conversion rejects v2 fields that v2beta2 cannot represent (for example tolerance); it does not silently downgrade to autoscaling/v1. Native resize requires matching restart policies at Pod creation, since those policies cannot be changed during resize. Rejected resize/eviction requests never fall back to ordinary Pod mutation or DELETE.

The SDK fixes do not add ResourcePlugin topology/device processing to the server, PDB eviction history, legacy/native runtime resize semantics, or ORM methods. CPU-only/Memory allocation and OS cgroup application must still be verified independently. Open-source Katalyst scheduler plugins are outside the internal Godel deployment chain.

For the reproducible dual-version lab and evidence, see `skills/katalyst-kind-lab` in the knowledge workspace and task `20261026-katalyst-compat-fix-v1`.
