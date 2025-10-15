package raft

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// Store must finish the durable write before returning nil. One Node owns a Store.
type Store interface {
	Load() (HardState, error)
	Save(HardState) error
}

var ErrEmpty = errors.New("empty_stable_store")

type MemoryStore struct {
	mu     sync.Mutex
	state  HardState
	Fail   bool
	Writes int
}

func (s *MemoryStore) Load() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state.ID == 0 {
		return HardState{}, ErrEmpty
	}
	return clone(s.state), nil
}
func (s *MemoryStore) Save(next HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Fail {
		return ErrStorage
	}
	if err := validTransition(s.state, next); err != nil {
		return err
	}
	s.state = clone(next)
	s.Writes++
	return nil
}

type FileStore struct {
	mu   sync.Mutex
	Path string
}
type envelope struct {
	State  json.RawMessage `json:"state"`
	SHA256 string          `json:"sha256"`
}

func (s *FileStore) Load() (HardState, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.load() }
func (s *FileStore) load() (HardState, error) {
	f, err := os.Open(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return HardState{}, ErrEmpty
	}
	if err != nil {
		return HardState{}, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, 1000001))
	if err != nil {
		return HardState{}, err
	}
	if len(raw) > 1000000 {
		return HardState{}, ErrCorrupt
	}
	var e envelope
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&e) != nil {
		return HardState{}, ErrCorrupt
	}
	var tail any
	if dec.Decode(&tail) != io.EOF {
		return HardState{}, ErrCorrupt
	}
	sum := sha256.Sum256(e.State)
	if hex.EncodeToString(sum[:]) != e.SHA256 {
		return HardState{}, ErrCorrupt
	}
	var state HardState
	dec = json.NewDecoder(bytes.NewReader(e.State))
	dec.DisallowUnknownFields()
	if dec.Decode(&state) != nil {
		return HardState{}, ErrCorrupt
	}
	if err := Validate(state); err != nil {
		return HardState{}, err
	}
	return state, nil
}
func (s *FileStore) Save(next HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.load()
	if err != nil && !errors.Is(err, ErrEmpty) {
		return err
	}
	if err = validTransition(old, next); err != nil {
		return err
	}
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(data)
	raw, err := json.Marshal(envelope{data, hex.EncodeToString(sum[:])})
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.Path)
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".raft-state-*")
	if err != nil {
		return err
	}
	temp := file.Name()
	defer os.Remove(temp)
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(raw)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(temp, s.Path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	if err = directory.Sync(); err != nil {
		return fmt.Errorf("directory sync: %w", err)
	}
	return nil
}
