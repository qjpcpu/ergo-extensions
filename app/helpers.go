package app

import (
	"ergo.services/ergo/gen"
	"github.com/qjpcpu/ergo-extensions/system"
)

// RegisterToAddressBook asks the local whereis process to refresh a process entry.
func RegisterToAddressBook(process interface {
	Send(to any, message any) error
	PID() gen.PID
}) error {
	return process.Send(system.WhereIsProcess, system.MessageFlushProcess{PID: process.PID()})
}
