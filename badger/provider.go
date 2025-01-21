// SPDX-License-Identifier: MPL-2.0

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package badger

import (
	"bytes"
	"encoding/gob"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/rlshukhov/nullable"
	storageErrors "github.com/rlshukhov/storage/errors"
	"strconv"
)

type Config struct {
	DirectoryPath nullable.Nullable[string] `yaml:"db_path"`
	InMemory      bool                      `yaml:"in_memory,omitempty"`
}

type provider[K any, V any] struct {
	cfg Config
	db  *badger.DB
}

func New[K ~string | ~uint64, V any](cfg Config) (*provider[K, V], error) {
	p := &provider[K, V]{cfg: cfg}
	if err := p.Setup(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *provider[K, V]) Setup() error {
	if !p.cfg.InMemory && p.cfg.DirectoryPath.IsNull() {
		return errors.New("directory path is null")
	}

	var options badger.Options
	if p.cfg.InMemory {
		options = badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)
	} else {
		options = badger.DefaultOptions(p.cfg.DirectoryPath.GetValue()).WithLogger(nil)
	}

	db, err := badger.Open(options)
	if err != nil {
		return err
	}

	p.db = db
	return nil
}

func (p *provider[K, V]) Shutdown() error {
	return p.db.Close()
}

func (p *provider[K, V]) Store(key K, value V) error {
	k, err := p.keyToByte(key)
	if err != nil {
		return err
	}

	v, err := p.encodeToBytes(value)
	if err != nil {
		return err
	}

	return mapError(p.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, v)
	}))
}

func (p *provider[K, V]) GetMultiple(keys []K) ([]V, error) {
	var values []V
	for _, key := range keys {
		v, err := p.Get(key)
		if err != nil {
			return []V{}, err
		}

		values = append(values, v)
	}

	return values, nil
}

func (p *provider[K, V]) Get(key K) (V, error) {
	k, err := p.keyToByte(key)
	if err != nil {
		var v V
		return v, err
	}

	var value V
	err = p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			value, err = p.decodeFromBytes(val)
			return err
		})
	})

	return value, mapError(err)
}

func (p *provider[K, V]) Remove(key K) error {
	k, err := p.keyToByte(key)
	if err != nil {
		return err
	}

	return mapError(p.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	}))
}

func (p *provider[K, V]) ForEach(fn func(key K, value V) bool) error {
	return mapError(p.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			stopIterationErr := errors.New("stop iteration")

			item := it.Item()
			key, err := p.byteToKey(item.Key())
			if err != nil {
				return err
			}

			err = item.Value(func(val []byte) error {
				v, err := p.decodeFromBytes(val)
				if err != nil {
					return err
				}

				if fn(key, v) {
					return nil
				} else {
					return stopIterationErr
				}
			})

			if err != nil {
				if errors.Is(err, stopIterationErr) {
					return nil
				}
				return err
			}
		}
		return nil
	}))
}

func mapError(err error) error {
	if storageErrors.Is(err, badger.ErrKeyNotFound) {
		return storageErrors.NewNotFound(err)
	}

	return err
}

func (p *provider[K, V]) keyToByte(k any) ([]byte, error) {
	switch k.(type) {
	case string:
		return []byte(k.(string)), nil
	case uint64:
		return []byte(strconv.FormatUint(k.(uint64), 10)), nil
	default:
		return nil, errors.New("unknown key type (string, uint64 supported)")
	}
}

func (p *provider[K, V]) byteToKey(b []byte) (K, error) {
	var k K
	switch any(k).(type) {
	case string:
		return any(string(b)).(K), nil
	case uint64:
		intValue, err := strconv.ParseUint(string(b), 10, 64)
		if err != nil {
			var zero K
			return zero, errors.New("failed to convert bytes to uint64")
		}
		return any(intValue).(K), nil
	default:
		var zero K
		return zero, errors.New("unknown key type (string, uint64 supported)")
	}
}

func (p *provider[K, V]) encodeToBytes(data V) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	if err := enc.Encode(data); err != nil {
		return nil, err
	}

	defer buf.Reset()
	return buf.Bytes(), nil
}

func (p *provider[K, V]) decodeFromBytes(data []byte) (V, error) {
	var value V
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	if err := dec.Decode(&value); err != nil {
		return value, err
	}

	return value, nil
}

func (p *provider[K, V]) StoreReference(reference K, key K) error {
	r, err := p.keyToByte(reference)
	if err != nil {
		return err
	}
	k, err := p.keyToByte(key)
	if err != nil {
		return err
	}

	return mapError(p.db.Update(func(txn *badger.Txn) error {
		return txn.Set(r, k)
	}))
}

func (p *provider[K, V]) RemoveReference(reference K) error {
	r, err := p.keyToByte(reference)
	if err != nil {
		return err
	}

	return mapError(p.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(r)
	}))
}

func (p *provider[K, V]) GetByReference(reference K) (V, error) {
	r, err := p.keyToByte(reference)
	if err != nil {
		var v V
		return v, err
	}

	var key K
	err = p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(r)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			key, err = p.byteToKey(val)
			return err
		})
	})
	if err != nil {
		var v V
		return v, mapError(err)
	}

	return p.Get(key)
}
