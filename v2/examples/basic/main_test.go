package main

import (
	"testing"

	"ergo.services/ergo/gen"
)

func TestEchoReturnsRequest(t *testing.T) {
	actor := &echo{}
	if got, err := actor.HandleCall(gen.PID{}, gen.Ref{}, "hello"); err != nil || got != "hello" {
		t.Fatalf("unexpected echo result: got=%v err=%v", got, err)
	}
}

func TestExampleRunsEndToEnd(t *testing.T) {
	main()
}
