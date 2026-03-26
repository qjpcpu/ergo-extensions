package app

import (
	"errors"
	"strings"
	"testing"

	"ergo.services/ergo/act"
	"ergo.services/ergo/gen"
)

type mockActor struct {
	act.Actor

	panicInit     bool
	panicHandle    bool
	panicHandleCall bool
	panicInspect  bool
	panicLog      bool
	panicEvent    bool

	initCalled     bool
	handleCalled   bool
	handleCallDone bool
	inspectCalled  bool
	logCalled      bool
	eventCalled    bool

	terminateReason error
}

func (m *mockActor) Init(args ...any) error {
	m.initCalled = true
	if m.panicInit {
		panic("panic in Init")
	}
	return nil
}

func (m *mockActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	m.handleCallDone = true
	if m.panicHandleCall {
		panic("panic in HandleCall")
	}
	return "result", nil
}

func (m *mockActor) HandleMessage(from gen.PID, message any) error {
	m.handleCalled = true
	if m.panicHandle {
		panic("panic in HandleMessage")
	}
	return nil
}

func (m *mockActor) HandleInspect(from gen.PID, item ...string) map[string]string {
	m.inspectCalled = true
	if m.panicInspect {
		panic("panic in HandleInspect")
	}
	return map[string]string{"key": "value"}
}

func (m *mockActor) HandleLog(message gen.MessageLog) error {
	m.logCalled = true
	if m.panicLog {
		panic("panic in HandleLog")
	}
	return nil
}

func (m *mockActor) HandleEvent(message gen.MessageEvent) error {
	m.eventCalled = true
	if m.panicEvent {
		panic("panic in HandleEvent")
	}
	return nil
}

func (m *mockActor) Terminate(reason error) {
	m.terminateReason = reason
}

type mockMetaActor struct {
	gen.MetaProcess

	panicInit     bool
	panicStart     bool
	panicHandle    bool
	panicHandleCall bool
	panicInspect  bool

	initCalled    bool
	startCalled   bool
	handleCalled  bool
	handleCallDone bool
	inspectCalled bool

	terminateReason error
}

func (m *mockMetaActor) Init(meta gen.MetaProcess) error {
	m.initCalled = true
	if m.panicInit {
		panic("panic in Init")
	}
	return nil
}

func (m *mockMetaActor) Start() error {
	m.startCalled = true
	if m.panicStart {
		panic("panic in Start")
	}
	return nil
}

func (m *mockMetaActor) HandleMessage(from gen.PID, message any) error {
	m.handleCalled = true
	if m.panicHandle {
		panic("panic in HandleMessage")
	}
	return nil
}

func (m *mockMetaActor) HandleCall(from gen.PID, ref gen.Ref, request any) (any, error) {
	m.handleCallDone = true
	if m.panicHandleCall {
		panic("panic in HandleCall")
	}
	return "result", nil
}

func (m *mockMetaActor) HandleInspect(from gen.PID, item ...string) map[string]string {
	m.inspectCalled = true
	if m.panicInspect {
		panic("panic in HandleInspect")
	}
	return map[string]string{"key": "value"}
}

func (m *mockMetaActor) Terminate(reason error) {
	m.terminateReason = reason
}

func TestPanicAsErrorActor_NoPanic(t *testing.T) {
	mock := &mockActor{}
	wrapped := ProtectActor(mock)

	err := wrapped.Init()
	if err != nil {
		t.Errorf("Init failed: %v", err)
	}
	if !mock.initCalled {
		t.Error("Init was not called")
	}

	_, err = wrapped.HandleCall(gen.PID{}, gen.Ref{}, nil)
	if err != nil {
		t.Errorf("HandleCall failed: %v", err)
	}
	if !mock.handleCallDone {
		t.Error("HandleCall was not called")
	}

	err = wrapped.HandleMessage(gen.PID{}, nil)
	if err != nil {
		t.Errorf("HandleMessage failed: %v", err)
	}
	if !mock.handleCalled {
		t.Error("HandleMessage was not called")
	}

	result := wrapped.HandleInspect(gen.PID{})
	if result == nil {
		t.Error("HandleInspect returned nil")
	}
	if !mock.inspectCalled {
		t.Error("HandleInspect was not called")
	}

	err = wrapped.HandleLog(gen.MessageLog{})
	if err != nil {
		t.Errorf("HandleLog failed: %v", err)
	}
	if !mock.logCalled {
		t.Error("HandleLog was not called")
	}

	err = wrapped.HandleEvent(gen.MessageEvent{})
	if err != nil {
		t.Errorf("HandleEvent failed: %v", err)
	}
	if !mock.eventCalled {
		t.Error("HandleEvent was not called")
	}

	wrapped.Terminate(errors.New("test termination"))
	if mock.terminateReason == nil || mock.terminateReason.Error() != "test termination" {
		t.Error("Terminate was not called correctly")
	}
}

func TestPanicAsErrorActor_PanicInInit(t *testing.T) {
	mock := &mockActor{panicInit: true}
	wrapped := ProtectActor(mock)

	err := wrapped.Init()
	if err == nil {
		t.Error("Expected error from panic in Init")
	}
	var pErr PanicError
	if !errors.As(err, &pErr) {
		t.Errorf("Expected PanicError, got %T", err)
	}
	if !strings.Contains(err.Error(), "panic in Init") {
		t.Errorf("Error message should contain 'panic in Init', got: %v", err.Error())
	}
	if pErr.stack == "" {
		t.Error("Stack trace should not be empty")
	}
}

func TestPanicAsErrorActor_PanicInHandleCall(t *testing.T) {
	mock := &mockActor{panicHandleCall: true}
	wrapped := ProtectActor(mock)

	_, err := wrapped.HandleCall(gen.PID{}, gen.Ref{}, nil)
	if err == nil {
		t.Error("Expected error from panic in HandleCall")
	}
	var pErr PanicError
	if !errors.As(err, &pErr) {
		t.Errorf("Expected PanicError, got %T", err)
	}
	if !strings.Contains(err.Error(), "panic in HandleCall") {
		t.Errorf("Error message should contain 'panic in HandleCall', got: %v", err.Error())
	}
}

func TestPanicAsErrorActorRE_PanicInHandleMessage(t *testing.T) {
	mock := &mockActor{panicHandle: true}
	wrapped := ProtectActor(mock)

	err := wrapped.HandleMessage(gen.PID{}, nil)
	if err == nil {
		t.Error("Expected error from panic in HandleMessage")
	}
	var pErr PanicError
	if !errors.As(err, &pErr) {
		t.Errorf("Expected PanicError, got %T", err)
	}
	if !strings.Contains(err.Error(), "panic in HandleMessage") {
		t.Errorf("Error message should contain 'panic in HandleMessage', got: %v", err.Error())
	}
}

func TestPanicAsErrorActor_PanicInHandleInspect(t *testing.T) {
	mock := &mockActor{panicInspect: true}
	wrapped := ProtectActor(mock)

	result := wrapped.HandleInspect(gen.PID{})
	if result == nil {
		t.Error("Result should not be nil even with panic")
	}
	if _, ok := result["panic"]; !ok {
		t.Error("Result should contain 'panic' key")
	}
	if !strings.Contains(result["panic"], "panic in HandleInspect") {
		t.Errorf("Error message should contain 'panic in HandleInspect', got: %v", result["panic"])
	}
}

func TestPanicAsErrorActor_PanicInHandleLog(t *testing.T) {
	mock := &mockActor{panicLog: true}
	wrapped := ProtectActor(mock)

	err := wrapped.HandleLog(gen.MessageLog{})
	if err == nil {
		t.Error("Expected error from panic in HandleLog")
	}
	var pErr PanicError
	if !errors.As(err, &pErr) {
		t.Errorf("Expected PanicError, got %T", err)
	}
	if !strings.Contains(err.Error(), "panic in HandleLog") {
		t.Errorf("Error message should contain 'panic in HandleLog', got: %v", err.Error())
	}
}

func TestPanicAsErrorActor_PanicInHandleEvent(t *testing.T) {
	mock := &mockActor{panicEvent: true}
	wrapped := ProtectActor(mock)

	err := wrapped.HandleEvent(gen.MessageEvent{})
	if err == nil {
		t.Error("Expected error from panic in HandleEvent")
	}
	var pErr PanicError
	if !errors.As(err, &pErr) {
		t.Errorf("Expected PanicError, got %T", err)
	}
	if !strings.Contains(err.Error(), "panic in HandleEvent") {
		t.Errorf("Error message should contain 'panic in HandleEvent', got: %v", err.Error())
	}
}

func TestPanicAsErrorMetaActor_NoPanic(t *testing.T) {
	mock := &mockMetaActor{}
	wrapped := ProtectMetaActor(mock)

	meta := &mockMetaActor{}
	err := wrapped.Init(meta)
	if err != nil {
		t.Errorf("Init failed: %v", err)
	}
	if !mock.initCalled {
		t.Error("Init was not called")
	}

	err = wrapped.Start()
	if err != nil {
		t.Errorf("Start failed: %v", err)
	}
	if !mock.startCalled {
		t.Error("Start was not called")
	}

	_, err = wrapped.HandleCall(gen.PID{}, gen.Ref{}, nil)
	if err != nil {
		t.Errorf("HandleCall failed: %v", err)
	}
	if !mock.handleCallDone {
		t.Error("HandleCall was not called")
	}

	err = wrapped.HandleMessage(gen.PID{}, nil)
	if err != nil {
		t.Errorf("HandleMessage failed: %v", err)
	}
	if !mock.handleCalled {
		t.Error("HandleMessage was not called")
	}

	result := wrapped.HandleInspect(gen.PID{})
	if result == nil {
		t.Error("HandleInspect returned nil")
	}
	if !mock.inspectCalled {
		t.Error("HandleInspect was not called")
	}

	wrapped.Terminate(errors.New("test termination meta"))
	if mock.terminateReason == nil || mock.terminateReason.Error() != "test termination meta" {
		t.Error("Terminate was not called correctly")
	}
}

func TestPanicAsErrorMetaActor_PanicInInit(t *testing.T) {
	mock := &mockMetaActor{panicInit: true}
	wrapped := ProtectMetaActor(mock)

	meta := &mockMetaActor{}
	err := wrapped.Init(meta)
	if err == nil {
		t.Error("Expected error from panic in Init")
	}
	var pErr PanicError
	if !errors.As(err, &pErr) {
		t.Errorf("Expected PanicError, got %T", err)
	}
	if !strings.Contains(err.Error(), "panic in Init") {
		t.Errorf("Error message should contain 'panic in Init', got: %v", err.Error())
	}
}

func TestPanicAsErrorMetaActor_PanicInStart(t *testing.T) {
	mock := &mockMetaActor{panicStart: true}
	wrapped := ProtectMetaActor(mock)

	err := wrapped.Start()
	if err == nil {
		t.Error("Expected error from panic in Start")
	}
	var pErr PanicError
	if !errors.As(err, &pErr) {
		t.Errorf("Expected PanicError, got %T", err)
	}
	if !strings.Contains(err.Error(), "panic in Start") {
		t.Errorf("Error message should contain 'panic in Start', got: %v", err.Error())
	}
}

func TestPanicAsErrorMetaActor_PanicInHandleCall(t *testing.T) {
	mock := &mockMetaActor{panicHandleCall: true}
	wrapped := ProtectMetaActor(mock)

	_, err := wrapped.HandleCall(gen.PID{}, gen.Ref{}, nil)
	if err == nil {
		t.Error("Expected error from panic in HandleCall")
	}
	var pErr PanicError
	if !errors.As(err, &pErr) {
		t.Errorf("Expected PanicError, got %T", err)
	}
	if !strings.Contains(err.Error(), "panic in HandleCall") {
		t.Errorf("Error message should contain 'panic in HandleCall', got: %v", err.Error())
	}
}

func TestPanicAsErrorMetaActor_PanicInHandleMessage(t *testing.T) {
	mock := &mockMetaActor{panicHandle: true}
	wrapped := ProtectMetaActor(mock)

	err := wrapped.HandleMessage(gen.PID{}, nil)
	if err == nil {
		t.Error("Expected error from panic in HandleMessage")
	}
	var pErr PanicError
	if !errors.As(err, &pErr) {
		t.Errorf("Expected PanicError, got %T", err)
	}
	if !strings.Contains(err.Error(), "panic in HandleMessage") {
		t.Errorf("Error message should contain 'panic in HandleMessage', got: %v", err.Error())
	}
}

func TestPanicAsErrorMetaActor_PanicInRE_HandleInspect(t *testing.T) {
	mock := &mockMetaActor{panicInspect: true}
	wrapped := ProtectMetaActor(mock)

	result := wrapped.HandleInspect(gen.PID{})
	if result == nil {
		t.Error("Result should not be nil even with panic")
	}
	if _, ok := result["panic"]; !ok {
		t.Error("Result should contain 'panic' key")
	}
	if !strings.Contains(result["panic"], "panic in HandleInspect") {
		t.Errorf("Error message should contain 'panic in HandleInspect', got: %v", result["panic"])
	}
}

func TestNewPanicError(t *testing.T) {
	r := "test panic"
	stack := "stack trace"
	err := newPanicError(r, stack)

	if err.Error() != "panic:test panic stack: stack trace" {
		t.Errorf("Unexpected error message: %v", err.Error())
	}
	if err.r != r {
		t.Errorf("Expected r=%v, got %v", r, err.r)
	}
	if err.stack != stack {
		t.Errorf("Expected stack=%v, got %v", stack, err.stack)
	}
}

func TestProtectActor_Nil(t *testing.T) {
	wrapped := ProtectActor(nil)
	if wrapped == nil {
		t.Error("ProtectActor should not return nil")
	}
	
	// Should recover from nil pointer dereference
	err := wrapped.Init()
	if err == nil {
		t.Error("Expected error when calling Init on nil actor")
	}
	if !strings.Contains(err.Error(), "panic:runtime error: invalid memory address or nil pointer dereference") {
		t.Errorf("Expected nil pointer dereference error, got: %v", err)
	}
}

func TestProtectMetaActor_Nil(t *testing.T) {
	wrapped := ProtectMetaActor(nil)
	if wrapped == nil {
		t.Error("ProtectMetaActor should not return nil")
	}
	
	// Should recover from nil pointer dereference
	err := wrapped.Start()
	if err == nil {
		t.Error("Expected error when calling Start on nil meta actor")
	}
	if !strings.Contains(err.Error(), "panic:runtime error: invalid memory address or nil pointer dereference") {
		t.Errorf("Expected nil pointer dereference error, got: %v", err)
	}
}
