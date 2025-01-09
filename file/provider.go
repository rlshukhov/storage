// SPDX-License-Identifier: MPL-2.0

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

package file

import (
	"encoding/json"
	baseErrors "errors"
	"github.com/rlshukhov/storage/errors"
	"gopkg.in/yaml.v3"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
)

type Config struct {
	Path    string `yaml:"path"`
	Content string `yaml:"content"`
}

type Type string

const (
	jsn Type = "json"
	yml Type = "yaml"
)

type data[K comparable, V any] struct {
	DataMap    map[K]V `yaml:"data,omitempty" json:"data,omitempty"`
	References map[K]K `yaml:"references,omitempty" json:"references,omitempty"`
}

type provider[K comparable, V any] struct {
	cfg      Config
	data     data[K, V]
	fileType Type
	mu       sync.RWMutex
}

func New[K ~string | ~uint64, V any](cfg Config) (*provider[K, V], error) {
	p := &provider[K, V]{
		cfg: cfg,
		data: data[K, V]{
			DataMap:    map[K]V{},
			References: map[K]K{},
		},
	}

	if cfg.Content != "" {
		err := json.Unmarshal([]byte(cfg.Content), &p.data)
		if err != nil {
			err := yaml.Unmarshal([]byte(cfg.Content), &p.data)
			if err != nil {
				return nil, err
			} else {
				p.fileType = yml
			}
		} else {
			p.fileType = jsn
		}
	} else {
		switch strings.ToLower(filepath.Ext(cfg.Path)) {
		case ".yaml", ".yml":
			p.fileType = yml
		case ".json":
			p.fileType = jsn
		default:
			return nil, baseErrors.New("unsupported file format: only .json, .yaml, and .yml are supported")
		}
	}

	if err := p.Setup(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *provider[K, V]) Setup() error {
	if p.cfg.Content != "" {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if _, err := os.Stat(p.cfg.Path); errors.Is(err, os.ErrNotExist) {
		return os.WriteFile(p.cfg.Path, []byte(""), 0644)
	} else if err != nil {
		return err
	}

	data, err := os.ReadFile(p.cfg.Path)
	if err != nil {
		return err
	}

	switch p.fileType {
	case yml:
		if err := yaml.Unmarshal(data, &p.data); err != nil {
			return err
		}
	case jsn:
		if err := json.Unmarshal(data, &p.data); err != nil {
			return err
		}
	default:
		return baseErrors.New("unsupported file format")
	}

	return nil
}

func (p *provider[K, V]) Shutdown() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.saveToFile()
}

func (p *provider[K, V]) Store(key K, value V) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.data.DataMap[key] = value
	return p.saveToFile()
}

func (p *provider[K, V]) Get(key K) (V, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	value, exists := p.data.DataMap[key]
	if !exists {
		var v V
		return v, errors.NotFound
	}

	return value, nil
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

func (p *provider[K, V]) Remove(key K) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.data.DataMap[key]
	if !exists {
		return errors.NotFound
	}

	delete(p.data.DataMap, key)
	return p.saveToFile()
}

func (p *provider[K, V]) ForEach(fn func(key K, value V) bool) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for k, v := range p.data.DataMap {
		if !fn(k, v) {
			break
		}
	}

	return nil
}

func (p *provider[K, V]) saveToFile() error {
	var data []byte
	var err error

	var k K
	var d any
	if _, ok := any(k).(int); ok {
		d = slices.Collect(maps.Values(p.data.DataMap))
	} else {
		d = p.data
	}

	switch p.fileType {
	case yml:
		data, err = yaml.Marshal(d)
	case jsn:
		data, err = json.MarshalIndent(d, "", "  ")
	default:
		return baseErrors.New("unsupported file format")
	}

	if err != nil {
		return err
	}

	return os.WriteFile(p.cfg.Path, data, 0644)
}

func (p *provider[K, V]) StoreReference(reference K, key K) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.data.References[reference] = key
	return p.saveToFile()
}

func (p *provider[K, V]) RemoveReference(reference K) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, exists := p.data.References[reference]
	if !exists {
		return errors.NotFound
	}

	delete(p.data.References, reference)
	return p.saveToFile()
}

func (p *provider[K, V]) GetByReference(reference K) (V, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	key, exists := p.data.References[reference]
	if !exists {
		var v V
		return v, errors.NotFound
	}

	value, exists := p.data.DataMap[key]
	if !exists {
		var v V
		return v, errors.NotFound
	}

	return value, nil
}
