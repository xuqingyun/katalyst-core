package vpa

import (
	"github.com/kubewharf/katalyst-core/pkg/client/compat"
	"os"
	"testing"
)

func TestMain(m *testing.M) { compat.ConfigureClientFeatureDefaults(); os.Exit(m.Run()) }
