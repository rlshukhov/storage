// SPDX-License-Identifier: MPL-2.0

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package storage

import (
	"errors"
	"github.com/rlshukhov/nullable"
	"github.com/rlshukhov/storage/badger"
	"github.com/rlshukhov/storage/file"
)

type KeyValueConfig struct {
	Badger nullable.Nullable[badger.Config] `yaml:"badger"`
	File   nullable.Nullable[file.Config]   `yaml:"file"`
}

type KeyValueProvider[K ~string | ~uint64, V any] interface {
	Setup() error
	Shutdown() error

	Store(key K, value V) error
	Get(key K) (V, error)
	Remove(key K) error
	ForEach(fn func(key K, value V) bool) error
	GetMultiple(keys []K) ([]V, error)

	StoreReference(reference K, key K) error
	RemoveReference(reference K) error
	GetByReference(reference K) (V, error)
}

func GetKeyValueProviderFromConfig[K ~string | ~uint64, V any](keyValueConfig KeyValueConfig) (KeyValueProvider[K, V], error) {
	switch true {
	case keyValueConfig.Badger.HasValue():
		return badger.New[K, V](keyValueConfig.Badger.GetValue())

	case keyValueConfig.File.HasValue():
		return file.New[K, V](keyValueConfig.File.GetValue())

	default:
		return nil, errors.New("storage provider is not configured")
	}
}
