# SDK 0.35 upgrade branch

This branch preserves the selected Kubernetes-upgrade source baseline and moves
Kubernetes staging dependencies to v0.35.0 (kubernetes to v1.35.0). It includes
the cgroup package split and context-aware PluginManager adaptation needed to
build the agent. The kubelet module remains a fork with ResourcePlugin RPCs;
replacing it with the unmodified community module would remove that interface.

For deployments using ordinary LIST/WATCH, set the client feature before process
startup:

```yaml
env:
- name: KUBE_FEATURE_WatchListClient
  value: "false"
```

For standalone execution:

```bash
KUBE_FEATURE_WatchListClient=false ./katalyst-agent [options]
```

The environment value is a deployment default, not a modification of the
upstream client-go feature registry. Tests of the raw SDK default must unset
this variable explicitly. Disabling WatchList addresses cache initialization
against old servers; it does not fix PodResources protobuf field differences
or provide newer API versions on an old cluster.

Agent compilation and bounded node tests are separate from full controller
migration. Some controller dependencies and the adapter's original enhanced
informer still need API adaptation. CPU/Memory success does not establish device,
multi-NUMA or complete controller compatibility.
