package raft

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestFileStoreDurableVoteAndLogAcrossNewInstances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node", "state.json")
	n := node(t, 1, &FileStore{Path: path})
	elect(t, n)
	_, _, err := n.Propose("disk-persisted")
	if err != nil {
		t.Fatal(err)
	}
	restarted := node(t, 1, &FileStore{Path: path})
	if restarted.state.Term != 1 || restarted.state.VotedFor != 1 || restarted.state.Log[2].Command != "disk-persisted" {
		t.Fatal(restarted.Stable())
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
}
func TestCorruptFileFailsClosedInsteadOfBootstrapping(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	node(t, 1, &FileStore{Path: path})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]byte{raw[:len(raw)/2], []byte(`{"state":{},"sha256":"invalid"}`), append(raw, []byte(` {"extra":true}`)...)} {
		if err = os.WriteFile(path, bad, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = New(cfg(1), &FileStore{Path: path}); err == nil || errors.Is(err, ErrEmpty) {
			t.Fatal("corruption silently reset", err)
		}
	}
}
func TestStableStoreRejectsRollbackDoubleVoteAndCommittedRewrite(t *testing.T) {
	for _, kind := range []string{"term", "vote", "commit", "entry", "membership"} {
		store := preset(t, 3, 1, []Entry{{1, 2, "fixed", false}})
		old := saved(t, store)
		next := clone(old)
		switch kind {
		case "term":
			next.Term = 2
		case "vote":
			next.VotedFor = 2
			if err := store.Save(next); err != nil {
				t.Fatal(err)
			}
			next.VotedFor = 3
		case "commit":
			next.Commit = 0
		case "entry":
			next.Log[1].Command = "changed"
		case "membership":
			next.Members = []int{1, 2, 3, 4, 5}
		}
		if err := store.Save(next); err == nil {
			t.Fatal("accepted rollback", kind)
		}
	}
}
func TestMemoryStoreCopiesAndConcurrentReaders(t *testing.T) {
	store := &MemoryStore{}
	node(t, 1, store)
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 100; j++ {
				state, err := store.Load()
				if err != nil {
					t.Error(err)
					return
				}
				state.Log[0].Command = "cannot-mutate-store"
			}
		}()
	}
	for term := uint64(1); term <= 50; term++ {
		state := saved(t, store)
		state.Term = term
		if err := store.Save(state); err != nil {
			t.Fatal(err)
		}
	}
	workers.Wait()
	if state := saved(t, store); state.Log[0] != (Entry{}) {
		t.Fatal(state)
	}
}
func TestPersistedMembershipCannotBeReconfigured(t *testing.T) {
	store := &MemoryStore{}
	node(t, 1, store)
	_, err := New(Config{ID: 1, Members: []int{1, 2, 3, 4, 5}, ElectionTicks: 8, HeartbeatTicks: 2}, store)
	if err == nil {
		t.Fatal("membership change accepted")
	}
}
