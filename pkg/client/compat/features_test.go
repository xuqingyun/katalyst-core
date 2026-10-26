package compat

import (
	features "k8s.io/client-go/features"
	"os"
	"testing"
)

type enabledFeatures struct{}

func (enabledFeatures) Enabled(features.Feature) bool { return true }
func TestDefaultsWorkAfterKubernetesReplacesEnvFeatureGates(t *testing.T) {
	old := features.FeatureGates()
	defer features.ReplaceFeatureGates(old)
	original, had := os.LookupEnv("KUBE_FEATURE_WatchListClient")
	defer func() {
		if had {
			os.Setenv("KUBE_FEATURE_WatchListClient", original)
		} else {
			os.Unsetenv("KUBE_FEATURE_WatchListClient")
		}
	}()
	for _, tc := range []struct {
		value               string
		specified, expected bool
	}{{"", false, false}, {"false", true, false}, {"true", true, true}} {
		features.ReplaceFeatureGates(enabledFeatures{})
		if tc.specified {
			os.Setenv("KUBE_FEATURE_WatchListClient", tc.value)
		} else {
			os.Unsetenv("KUBE_FEATURE_WatchListClient")
		}
		ConfigureClientFeatureDefaults()
		if got := features.FeatureGates().Enabled(features.WatchListClient); got != tc.expected {
			t.Fatalf("env %q watchlist=%v", tc.value, got)
		}
		if !features.FeatureGates().Enabled(features.InformerResourceVersion) {
			t.Fatal("unrelated feature changed")
		}
	}
}
