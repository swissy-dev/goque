package conformance_test

import (
	"testing"

	"github.com/swissy-dev/goque/backend"
	"github.com/swissy-dev/goque/backendtest"
)

func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) backend.Backend { return newHarness(t) })
}
