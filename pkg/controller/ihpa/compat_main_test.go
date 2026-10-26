package ihpa

import (
	"github.com/kubewharf/katalyst-core/pkg/client/compat"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	time.Local = time.UTC
	compat.ConfigureClientFeatureDefaults()
	os.Exit(m.Run())
}
