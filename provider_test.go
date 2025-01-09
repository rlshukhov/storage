// SPDX-License-Identifier: MPL-2.0

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package storage

import (
	"github.com/google/uuid"
	"github.com/rlshukhov/nullable"
	"github.com/rlshukhov/storage/badger"
	"github.com/rlshukhov/storage/errors"
	"github.com/rlshukhov/storage/file"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func performTestsForProviders[K ~string | ~uint64, V any](t *testing.T, test func(t *testing.T, p KeyValueProvider[K, V])) {
	emb, err := GetKeyValueProviderFromConfig[K, V](KeyValueConfig{
		Badger: nullable.FromValue(badger.Config{
			InMemory: true,
		}),
	})
	defer func(emb KeyValueProvider[K, V]) {
		err := emb.Shutdown()
		if err != nil {
			panic(err)
		}
	}(emb)
	if err != nil {
		panic(err)
	}

	fPath := "/tmp/test." + uuid.NewString() + ".yaml"
	f, err := GetKeyValueProviderFromConfig[K, V](KeyValueConfig{
		File: nullable.FromValue(file.Config{
			Path: fPath,
		}),
	})
	if err != nil {
		panic(err)
	}

	defer func() {
		err := f.Shutdown()
		if err != nil {
			panic(err)
		}

		err = os.Remove(fPath)
		if err != nil {
			panic(err)
		}
	}()

	p := []KeyValueProvider[K, V]{
		emb,
		f,
	}

	for _, v := range p {
		test(t, v)
	}
}

func TestProvider_StoreAndGet(t *testing.T) {
	performTestsForProviders[string, string](t, func(t *testing.T, p KeyValueProvider[string, string]) {
		err := p.Store("key", "value")
		require.NoError(t, err)

		val, err := p.Get("key")
		require.NoError(t, err)
		assert.Equal(t, "value", val)
	})
}

func TestProvider_Remove(t *testing.T) {
	performTestsForProviders[string, string](t, func(t *testing.T, p KeyValueProvider[string, string]) {
		err := p.Store("key", "value")
		require.NoError(t, err)

		err = p.Remove("key")
		require.NoError(t, err)

		_, err = p.Get("key")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errors.NotFound))
	})
}

func TestProvider_ForEach(t *testing.T) {
	performTestsForProviders[string, string](t, func(t *testing.T, p KeyValueProvider[string, string]) {
		entries := map[string]string{
			"key1": "value1",
			"key2": "value2",
			"key3": "value3",
		}

		for k, v := range entries {
			err := p.Store(k, v)
			require.NoError(t, err)
		}

		visited := make(map[string]string)
		err := p.ForEach(func(key, value string) bool {
			visited[key] = value
			return true
		})
		require.NoError(t, err)

		assert.Equal(t, entries, visited)
	})
}

type Address struct {
	City    string
	Country string
}

type User struct {
	ID      uint64
	Name    string
	Address Address
	Age     int
}

func TestProviderWithComplexStructure(t *testing.T) {
	performTestsForProviders[uint64, User](t, func(t *testing.T, p KeyValueProvider[uint64, User]) {
		user := User{
			ID:   1,
			Name: "John Doe",
			Address: Address{
				City:    "New York",
				Country: "USA",
			},
			Age: 30,
		}

		err := p.Store(user.ID, user)
		assert.NoError(t, err)

		retrievedUser, err := p.Get(user.ID)
		assert.NoError(t, err)
		assert.Equal(t, user, retrievedUser)

		found := false
		err = p.ForEach(func(key uint64, value User) bool {
			if key == user.ID && value == user {
				found = true
			}
			return true
		})
		assert.NoError(t, err)
		assert.True(t, found)

		err = p.Remove(user.ID)
		assert.NoError(t, err)

		_, err = p.Get(user.ID)
		assert.True(t, errors.Is(err, errors.NotFound))
	})
}

func TestProvider_StoreReferenceAndGetByReference(t *testing.T) {
	performTestsForProviders[string, string](t, func(t *testing.T, p KeyValueProvider[string, string]) {
		err := p.Store("key", "value")
		require.NoError(t, err)

		err = p.StoreReference("ref", "key")
		require.NoError(t, err)

		val, err := p.GetByReference("ref")
		require.NoError(t, err)
		assert.Equal(t, "value", val)
	})
}

func TestProvider_RemoveReference(t *testing.T) {
	performTestsForProviders[string, string](t, func(t *testing.T, p KeyValueProvider[string, string]) {
		err := p.Store("key", "value")
		require.NoError(t, err)

		err = p.StoreReference("ref", "key")
		require.NoError(t, err)

		err = p.RemoveReference("ref")
		require.NoError(t, err)

		_, err = p.GetByReference("ref")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errors.NotFound))
	})
}

func TestProvider_StoreReference_WithNonExistentKey(t *testing.T) {
	performTestsForProviders[string, string](t, func(t *testing.T, p KeyValueProvider[string, string]) {
		err := p.StoreReference("ref", "nonexistent_key")
		assert.NoError(t, err)

		_, err = p.GetByReference("ref")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, errors.NotFound))
	})
}

func TestProvider_StoreReference_Overwrite(t *testing.T) {
	performTestsForProviders[string, string](t, func(t *testing.T, p KeyValueProvider[string, string]) {
		err := p.Store("key1", "value1")
		require.NoError(t, err)
		err = p.Store("key2", "value2")
		require.NoError(t, err)

		err = p.StoreReference("ref", "key1")
		require.NoError(t, err)

		err = p.StoreReference("ref", "key2")
		require.NoError(t, err)

		val, err := p.GetByReference("ref")
		require.NoError(t, err)
		assert.Equal(t, "value2", val)
	})
}
