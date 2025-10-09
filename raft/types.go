// Package raft implements a bounded, single-owner Raft state machine.
package raft

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"unicode/utf8"
)

const MaxEntries = 512
const MaxTerm uint64 = 1000000000
const MaxCommandBytes = 256
const MaxBatch = 32

var (
	ErrNotLeader         = errors.New("not_leader")
	ErrStorage           = errors.New("storage_failure")
	ErrCorrupt           = errors.New("corrupt_stable_state")
	ErrInvalid           = errors.New("invalid_input")
	ErrCommittedConflict = errors.New("committed_log_conflict")
	ErrBudget            = errors.New("resource_budget")
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
	Halted    Role = "halted"
)

type Entry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command string `json:"command"`
	Noop    bool   `json:"noop"`
}
type HardState struct {
	Version  int     `json:"version"`
	ID       int     `json:"id"`
	Members  []int   `json:"members"`
	Term     uint64  `json:"term"`
	VotedFor int     `json:"voted_for"`
	Log      []Entry `json:"log"`
	Commit   uint64  `json:"commit"`
}

func clone(s HardState) HardState {
	s.Members = append([]int(nil), s.Members...)
	s.Log = append([]Entry(nil), s.Log...)
	return s
}
func members(ids []int, id int) ([]int, error) {
	if len(ids) != 3 && len(ids) != 5 {
		return nil, fmt.Errorf("%w: static membership must contain 3 or 5 nodes", ErrInvalid)
	}
	result := append([]int(nil), ids...)
	sort.Ints(result)
	found := false
	for i, v := range result {
		if v < 1 || v > 31 || (i > 0 && result[i-1] == v) {
			return nil, fmt.Errorf("%w: invalid member", ErrInvalid)
		}
		if v == id {
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("%w: local ID absent", ErrInvalid)
	}
	return result, nil
}
func contains(ids []int, id int) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}
func Validate(s HardState) error {
	ids, err := members(s.Members, s.ID)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(ids, s.Members) || s.Version != 1 || s.Term > MaxTerm || !contains(append([]int{0}, ids...), s.VotedFor) || (s.Term == 0 && s.VotedFor != 0) || len(s.Log) < 1 || len(s.Log) > MaxEntries+1 || s.Commit >= uint64(len(s.Log)) {
		return ErrCorrupt
	}
	if s.Log[0] != (Entry{}) {
		return ErrCorrupt
	}
	for i := 1; i < len(s.Log); i++ {
		e := s.Log[i]
		if e.Index != uint64(i) || e.Term == 0 || e.Term > s.Term || e.Term < s.Log[i-1].Term || len(e.Command) > MaxCommandBytes || !utf8.ValidString(e.Command) || (e.Noop && e.Command != "") || (!e.Noop && e.Command == "") {
			return ErrCorrupt
		}
	}
	return nil
}
func validTransition(old, next HardState) error {
	if err := Validate(next); err != nil {
		return err
	}
	if old.ID == 0 {
		return nil
	}
	if old.ID != next.ID || !reflect.DeepEqual(old.Members, next.Members) || next.Term < old.Term || next.Commit < old.Commit ||
		(next.Term == old.Term && old.VotedFor != 0 && old.VotedFor != next.VotedFor) || len(next.Log) <= int(old.Commit) {
		return ErrCorrupt
	}
	for i := uint64(0); i <= old.Commit; i++ {
		if old.Log[i] != next.Log[i] {
			return ErrCommittedConflict
		}
	}
	return nil
}

type Kind string

const (
	RequestVote    Kind = "request_vote"
	VoteResponse   Kind = "vote_response"
	AppendEntries  Kind = "append_entries"
	AppendResponse Kind = "append_response"
)

type Message struct {
	Kind         Kind    `json:"kind"`
	From         int     `json:"from"`
	To           int     `json:"to"`
	Term         uint64  `json:"term"`
	LastIndex    uint64  `json:"last_index,omitempty"`
	LastTerm     uint64  `json:"last_term,omitempty"`
	Granted      bool    `json:"granted,omitempty"`
	PrevIndex    uint64  `json:"prev_index,omitempty"`
	PrevTerm     uint64  `json:"prev_term,omitempty"`
	Entries      []Entry `json:"entries,omitempty"`
	LeaderCommit uint64  `json:"leader_commit,omitempty"`
	Success      bool    `json:"success,omitempty"`
	Match        uint64  `json:"match,omitempty"`
	Hint         uint64  `json:"hint,omitempty"`
	RPC          uint64  `json:"rpc,omitempty"`
}

func (m Message) Copy() Message { m.Entries = append([]Entry(nil), m.Entries...); return m }

type Status struct {
	ID        int    `json:"id"`
	Role      Role   `json:"role"`
	Term      uint64 `json:"term"`
	Vote      int    `json:"vote"`
	Commit    uint64 `json:"commit"`
	LastIndex uint64 `json:"last_index"`
	LeaderID  int    `json:"leader_id"`
}
type Config struct {
	ID             int
	Members        []int
	ElectionTicks  int
	HeartbeatTicks int
	RandomSeed     int64
}
