package daemon

import (
	"testing"
	"time"
)

func TestDefaultOptionsTargetSLA(t *testing.T) {
	opts := DefaultOptions()
	if opts.InitialRecoveryDelay > time.Second || opts.LeaderRecoveryDelay > time.Second || opts.NodeLeftRecoveryDelay > time.Second {
		t.Fatalf("recovery defaults should schedule within 1s: %+v", opts)
	}
	if opts.LaunchTimeout != 3*time.Second {
		t.Fatalf("expected launch timeout 3s, got %s", opts.LaunchTimeout)
	}
	if opts.RunningGrace != 2*time.Second {
		t.Fatalf("expected running grace 2s, got %s", opts.RunningGrace)
	}
}

func TestDaemonRetryDelayUsesOptions(t *testing.T) {
	w := &daemon{options: normalizeOptions(Options{
		RetryInitialDelay: 100 * time.Millisecond,
		RetryMaxDelay:     250 * time.Millisecond,
		RetryJitterMax:    -1,
	})}

	if got := w.retryDelay(0); got != 100*time.Millisecond {
		t.Fatalf("attempt 0 delay = %s", got)
	}
	if got := w.retryDelay(1); got != 200*time.Millisecond {
		t.Fatalf("attempt 1 delay = %s", got)
	}
	if got := w.retryDelay(3); got != 250*time.Millisecond {
		t.Fatalf("attempt 3 delay = %s", got)
	}
}
