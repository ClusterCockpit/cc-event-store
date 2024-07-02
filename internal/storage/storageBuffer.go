package storage2

import (
	"sync"

	lp "github.com/ClusterCockpit/cc-energy-manager/pkg/cc-message"
)

type storageBuffer struct {
	buffer []*lp.CCMessage
	lock   sync.RWMutex
}

type StorageBuffer interface {
	Lock()
	Unlock()
	Add(msg *lp.CCMessage)
	Get() []*lp.CCMessage
	Clear()
	Len() int
}

func (b *storageBuffer) Lock() {
	b.lock.Lock()
}

func (b *storageBuffer) Unlock() {
	b.lock.Unlock()
}

func (b *storageBuffer) Len() int {
	return len(b.buffer)
}

func (b *storageBuffer) Add(msg *lp.CCMessage) {
	b.buffer = append(b.buffer, msg)
}

func (b *storageBuffer) Get() []*lp.CCMessage {
	return b.buffer
}

func (b *storageBuffer) Clear() {
	//b.buffer = b.buffer[:0]
	//clear(b.buffer)
	b.buffer = nil
}

func (b *storageBuffer) LenLocked() int {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.Len()
}

func (b *storageBuffer) AddLocked(msg *lp.CCMessage) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.Add(msg)
}

func (b *storageBuffer) GetLocked() []*lp.CCMessage {
	b.lock.RLock()
	defer b.lock.RUnlock()
	return b.Get()
}

func (b *storageBuffer) ClearLocked() {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.Clear()
}

func NewStorageBuffer(cap int) (StorageBuffer, error) {
	b := new(storageBuffer)
	b.buffer = make([]*lp.CCMessage, 0, cap)
	b.lock = sync.RWMutex{}
	return b, nil
}
