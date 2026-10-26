package compat

import (
	features "k8s.io/client-go/features"
	"k8s.io/klog/v2"
	"os"
	"strconv"
)

type clientFeatureDefaults struct {
	base      features.Gates
	watchList bool
}

func (g clientFeatureDefaults) Enabled(feature features.Feature) bool {
	if feature == features.WatchListClient {
		return g.watchList
	}
	return g.base.Enabled(feature)
}

// ConfigureClientFeatureDefaults also works when k8s.io/kubernetes has replaced
// client-go's environment feature gates with component-base feature gates.
// Invoke once during process startup, before constructing clients/informers.
func ConfigureClientFeatureDefaults() {
	enabled := false
	if value, ok := os.LookupEnv("KUBE_FEATURE_WatchListClient"); ok {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			klog.Warningf("invalid KUBE_FEATURE_WatchListClient=%q; using false", value)
		} else {
			enabled = parsed
		}
	}
	features.ReplaceFeatureGates(clientFeatureDefaults{base: features.FeatureGates(), watchList: enabled})
}
