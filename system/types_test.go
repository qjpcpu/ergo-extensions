package system

import (
	"testing"
)

func TestVersion(t *testing.T) {
	v1 := NewVersion()
	v2 := v1.Incr()

	if !v2.GreaterThan(v1) {
		t.Errorf("expected %v > %v", v2, v1)
	}

	if !v2.GreaterThanOrEq(v1) {
		t.Errorf("expected %v >= %v", v2, v1)
	}

	if v1.Equal(v2) {
		t.Errorf("expected %v != %v", v1, v2)
	}

	if !v1.Equal(v1) {
		t.Errorf("expected %v == %v", v1, v1)
	}

	if v1.String() == "" {
		t.Error("String() should not be empty")
	}
}
