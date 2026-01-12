package system

import (
	"ergo.services/ergo/gen"
)

type IAddressBookStorage interface {
	Get(process gen.Atom) (node gen.Atom, version int, err error)
	Set(node gen.Atom, process gen.Atom, version int) error
	Remove(node gen.Atom, process gen.Atom, version int) error
}

type dummyStorage struct{}

func (dummyStorage) Get(process gen.Atom) (node gen.Atom, version int, err error) { return }
func (dummyStorage) Set(node gen.Atom, process gen.Atom, version int) error       { return nil }
func (dummyStorage) Remove(node gen.Atom, process gen.Atom, version int) error    { return nil }

func OmitSystemProcess(s IAddressBookStorage) IAddressBookStorage {
	return &omitSystemProcess{IAddressBookStorage: s}
}

type omitSystemProcess struct {
	IAddressBookStorage
}

func (self *omitSystemProcess) Get(process gen.Atom) (node gen.Atom, version int, err error) {
	if isSystemProc(process) {
		return
	}
	return self.IAddressBookStorage.Get(process)
}

func (self *omitSystemProcess) Set(node gen.Atom, process gen.Atom, version int) error {
	if isSystemProc(process) {
		return nil
	}
	return self.IAddressBookStorage.Set(node, process, version)
}

func (self *omitSystemProcess) Remove(node gen.Atom, process gen.Atom, version int) error {
	if isSystemProc(process) {
		return nil
	}
	return self.IAddressBookStorage.Remove(node, process, version)
}
