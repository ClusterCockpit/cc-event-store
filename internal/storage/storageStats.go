// Copyright (C) NHR@FAU, University Erlangen-Nuremberg.
// All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
package storage

import "sync"

type storageStats struct {
	Errors map[string]int64 `json:"errors"`
	Stats  map[string]int64 `json:"status"`
	lock   *sync.Mutex
}

type StorageStats interface {
	Init() error
	Close()
	ResetError(key string)
	UpdateError(key string, value int64)
	ResetStats(key string)
	UpdateStats(key string, value int64)
}

func (ss *storageStats) Init() error {
	ss.Errors = make(map[string]int64)
	ss.Stats = make(map[string]int64)
	ss.lock = &sync.Mutex{}
	return nil
}

func (ss *storageStats) Close() {
	clear(ss.Errors)
	clear(ss.Stats)
}

func (ss *storageStats) ResetError(key string) {
	ss.lock.Lock()
	if _, ok := ss.Errors[key]; ok {
		ss.Errors[key] = 0
	}
	ss.lock.Unlock()
}

func (ss *storageStats) ResetStats(key string) {
	ss.lock.Lock()
	if _, ok := ss.Stats[key]; ok {
		ss.Stats[key] = 0
	}
	ss.lock.Unlock()
}

func (ss *storageStats) UpdateError(key string, value int64) {
	ss.lock.Lock()
	if x, ok := ss.Errors[key]; ok {
		ss.Errors[key] = x + value
	} else {
		ss.Errors[key] = value
	}
	ss.lock.Unlock()
}

func (ss *storageStats) UpdateStats(key string, value int64) {
	ss.lock.Lock()
	if x, ok := ss.Stats[key]; ok {
		ss.Stats[key] = x + value
	} else {
		ss.Stats[key] = value
	}
	ss.lock.Unlock()
}
