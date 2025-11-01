package raft

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"unicode/utf8"
)

// Node is single-owner synchronous protocol code; serialize all calls externally.
type Node struct {
	cfg              Config
	state            HardState
	store            Store
	role             Role
	leader           int
	elapsed          int
	heartbeat        int
	votes            map[int]bool
	next             map[int]uint64
	match            map[int]uint64
	sent             map[int]uint64
	acked            map[int]uint64
	rng              *rand.Rand
	electionDeadline int
}

func New(cfg Config, store Store) (*Node, error) {
	ids, err := members(cfg.Members, cfg.ID)
	if err != nil {
		return nil, err
	}
	cfg.Members = ids
	if store == nil || cfg.HeartbeatTicks < 1 || cfg.ElectionTicks <= cfg.HeartbeatTicks || cfg.ElectionTicks > 1000 {
		return nil, ErrInvalid
	}
	s, err := store.Load()
	if errors.Is(err, ErrEmpty) {
		s = HardState{Version: 1, ID: cfg.ID, Members: ids, Log: []Entry{{}}}
		err = store.Save(s)
	}
	if err != nil {
		return nil, err
	}
	if err = Validate(s); err != nil {
		return nil, err
	}
	if s.ID != cfg.ID || !reflect.DeepEqual(s.Members, ids) {
		return nil, fmt.Errorf("%w: persisted identity or membership differs", ErrInvalid)
	}
	node := &Node{cfg: cfg, state: clone(s), store: store, role: Follower, rng: rand.New(rand.NewSource(cfg.RandomSeed + int64(cfg.ID)*1009 + int64(s.Term)*7919))}
	node.resetTimer()
	return node, nil
}
func (n *Node) resetTimer() {
	n.elapsed = 0
	n.electionDeadline = n.cfg.ElectionTicks + n.rng.Intn(n.cfg.ElectionTicks)
}
func (n *Node) Status() Status {
	return Status{n.cfg.ID, n.role, n.state.Term, n.state.VotedFor, n.state.Commit, uint64(len(n.state.Log) - 1), n.leader}
}
func (n *Node) Stable() HardState { return clone(n.state) }
func (n *Node) Applied() []Entry  { return append([]Entry(nil), n.state.Log[1:n.state.Commit+1]...) }
func (n *Node) persist(next HardState) error {
	if err := n.store.Save(next); err != nil {
		n.role = Halted
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	n.state = clone(next)
	return nil
}
func (n *Node) follower(term uint64, leader int) error {
	if term > n.state.Term {
		next := clone(n.state)
		next.Term = term
		next.VotedFor = 0
		if err := n.persist(next); err != nil {
			return err
		}
	}
	n.role = Follower
	n.leader = leader
	n.votes = nil
	return nil
}
func (n *Node) Tick() ([]Message, error) {
	if n.role == Halted {
		return nil, ErrStorage
	}
	n.elapsed++
	if n.role == Leader {
		n.heartbeat++
		if n.heartbeat >= n.cfg.HeartbeatTicks {
			n.heartbeat = 0
			return n.broadcast(), nil
		}
		return nil, nil
	}
	if n.elapsed >= n.electionDeadline {
		return n.Campaign()
	}
	return nil, nil
}
func (n *Node) Campaign() ([]Message, error) {
	if n.role == Halted {
		return nil, ErrStorage
	}
	if n.state.Term >= MaxTerm {
		return nil, ErrBudget
	}
	next := clone(n.state)
	next.Term++
	next.VotedFor = n.cfg.ID
	if err := n.persist(next); err != nil {
		return nil, err
	}
	n.role = Candidate
	n.leader = 0
	n.resetTimer()
	n.votes = map[int]bool{n.cfg.ID: true}
	last := n.state.Log[len(n.state.Log)-1]
	out := []Message{}
	for _, id := range n.cfg.Members {
		if id != n.cfg.ID {
			out = append(out, Message{Kind: RequestVote, From: n.cfg.ID, To: id, Term: n.state.Term, LastIndex: last.Index, LastTerm: last.Term})
		}
	}
	return out, nil
}
func (n *Node) becomeLeader() ([]Message, error) {
	if len(n.state.Log) > MaxEntries {
		return nil, ErrBudget
	}
	next := clone(n.state)
	next.Log = append(next.Log, Entry{Index: uint64(len(next.Log)), Term: next.Term, Noop: true})
	if err := n.persist(next); err != nil {
		return nil, err
	}
	n.role = Leader
	n.leader = n.cfg.ID
	n.heartbeat = 0
	n.next = map[int]uint64{}
	n.match = map[int]uint64{}
	n.sent = map[int]uint64{}
	n.acked = map[int]uint64{}
	for _, id := range n.cfg.Members {
		n.next[id] = uint64(len(n.state.Log))
		n.match[id] = 0
	}
	n.match[n.cfg.ID] = uint64(len(n.state.Log) - 1)
	return n.broadcast(), nil
}
func (n *Node) appendTo(id int) Message {
	next := n.next[id]
	if next < 1 {
		next = 1
	}
	if next > uint64(len(n.state.Log)) {
		next = uint64(len(n.state.Log))
	}
	end := next + MaxBatch
	if end > uint64(len(n.state.Log)) {
		end = uint64(len(n.state.Log))
	}
	n.sent[id]++
	return Message{Kind: AppendEntries, From: n.cfg.ID, To: id, Term: n.state.Term, PrevIndex: next - 1, PrevTerm: n.state.Log[next-1].Term, Entries: append([]Entry(nil), n.state.Log[next:end]...), LeaderCommit: n.state.Commit, RPC: n.sent[id]}
}
func (n *Node) broadcast() []Message {
	out := []Message{}
	for _, id := range n.cfg.Members {
		if id != n.cfg.ID {
			out = append(out, n.appendTo(id))
		}
	}
	return out
}
func (n *Node) Propose(command string) (uint64, []Message, error) {
	if n.role != Leader {
		return 0, nil, ErrNotLeader
	}
	if command == "" || len(command) > MaxCommandBytes || !utf8.ValidString(command) {
		return 0, nil, ErrInvalid
	}
	if len(n.state.Log) > MaxEntries {
		return 0, nil, ErrBudget
	}
	next := clone(n.state)
	index := uint64(len(next.Log))
	next.Log = append(next.Log, Entry{Index: index, Term: next.Term, Command: command})
	if err := n.persist(next); err != nil {
		return 0, nil, err
	}
	n.match[n.cfg.ID] = index
	return index, n.broadcast(), nil
}
func (n *Node) validateMessage(m Message) error {
	if m.To != n.cfg.ID || m.From == m.To || !contains(n.cfg.Members, m.From) || m.Term == 0 || m.Term > MaxTerm || m.LastIndex > MaxEntries || m.LastTerm > m.Term || m.PrevIndex > MaxEntries || m.PrevTerm > m.Term || m.LeaderCommit > MaxEntries || m.Match > MaxEntries || m.Hint > MaxEntries+1 || len(m.Entries) > MaxBatch {
		return ErrInvalid
	}
	switch m.Kind {
	case RequestVote, VoteResponse, AppendEntries, AppendResponse:
	default:
		return ErrInvalid
	}
	if m.Kind != AppendEntries && len(m.Entries) > 0 {
		return ErrInvalid
	}
	if m.Kind == RequestVote && ((m.LastIndex == 0) != (m.LastTerm == 0)) {
		return ErrInvalid
	}
	if m.Kind == AppendEntries && ((m.PrevIndex == 0) != (m.PrevTerm == 0)) {
		return ErrInvalid
	}
	if (m.Kind == AppendEntries || m.Kind == AppendResponse) && m.RPC == 0 {
		return ErrInvalid
	}
	previous := m.PrevTerm
	for i, e := range m.Entries {
		if e.Index != m.PrevIndex+uint64(i)+1 || e.Index > MaxEntries || e.Term == 0 || e.Term > m.Term || e.Term < previous || len(e.Command) > MaxCommandBytes || !utf8.ValidString(e.Command) || (e.Noop && e.Command != "") || (!e.Noop && e.Command == "") {
			return ErrInvalid
		}
		previous = e.Term
	}
	return nil
}
func (n *Node) Step(m Message) ([]Message, error) {
	if n.role == Halted {
		return nil, ErrStorage
	}
	if err := n.validateMessage(m); err != nil {
		return nil, err
	}
	if m.Term > n.state.Term {
		if err := n.follower(m.Term, 0); err != nil {
			return nil, err
		}
	}
	if m.Term < n.state.Term {
		switch m.Kind {
		case RequestVote:
			return []Message{{Kind: VoteResponse, From: n.cfg.ID, To: m.From, Term: n.state.Term}}, nil
		case AppendEntries:
			return []Message{{Kind: AppendResponse, From: n.cfg.ID, To: m.From, Term: n.state.Term, RPC: m.RPC, Hint: uint64(len(n.state.Log))}}, nil
		}
		return nil, nil
	}
	switch m.Kind {
	case RequestVote:
		last := n.state.Log[len(n.state.Log)-1]
		fresh := m.LastTerm > last.Term || (m.LastTerm == last.Term && m.LastIndex >= last.Index)
		grant := (n.state.VotedFor == 0 || n.state.VotedFor == m.From) && fresh
		if grant {
			next := clone(n.state)
			next.VotedFor = m.From
			if err := n.persist(next); err != nil {
				return nil, err
			}
			n.resetTimer()
		}
		return []Message{{Kind: VoteResponse, From: n.cfg.ID, To: m.From, Term: n.state.Term, Granted: grant}}, nil
	case VoteResponse:
		if n.role != Candidate || !m.Granted {
			return nil, nil
		}
		n.votes[m.From] = true
		if len(n.votes) > len(n.cfg.Members)/2 {
			return n.becomeLeader()
		}
		return nil, nil
	case AppendEntries:
		return n.receiveAppend(m)
	case AppendResponse:
		return n.receiveAck(m)
	}
	return nil, ErrInvalid
}
func (n *Node) receiveAppend(m Message) ([]Message, error) {
	if err := n.follower(m.Term, m.From); err != nil {
		return nil, err
	}
	n.resetTimer()
	reject := func(hint uint64) []Message {
		return []Message{{Kind: AppendResponse, From: n.cfg.ID, To: m.From, Term: n.state.Term, RPC: m.RPC, Hint: hint}}
	}
	if m.PrevIndex >= uint64(len(n.state.Log)) {
		return reject(uint64(len(n.state.Log))), nil
	}
	if n.state.Log[m.PrevIndex].Term != m.PrevTerm {
		hint := m.PrevIndex
		term := n.state.Log[hint].Term
		for hint > 1 && n.state.Log[hint-1].Term == term {
			hint--
		}
		if hint == 0 {
			hint = 1
		}
		return reject(hint), nil
	}
	next := clone(n.state)
	changed := false
	for i, e := range m.Entries {
		if e.Index < uint64(len(next.Log)) {
			existing := next.Log[e.Index]
			if existing.Term == e.Term {
				if existing != e {
					return nil, ErrCommittedConflict
				}
				continue
			}
			if e.Index <= next.Commit {
				return nil, ErrCommittedConflict
			}
			next.Log = next.Log[:e.Index]
		}
		next.Log = append(next.Log, m.Entries[i:]...)
		changed = true
		break
	}
	match := m.PrevIndex + uint64(len(m.Entries))
	commit := m.LeaderCommit
	if commit > match {
		commit = match
	}
	if commit > next.Commit {
		next.Commit = commit
		changed = true
	}
	if changed {
		if err := n.persist(next); err != nil {
			return nil, err
		}
	}
	return []Message{{Kind: AppendResponse, From: n.cfg.ID, To: m.From, Term: n.state.Term, RPC: m.RPC, Success: true, Match: match}}, nil
}
func (n *Node) receiveAck(m Message) ([]Message, error) {
	if n.role != Leader || m.RPC == 0 || m.RPC > n.sent[m.From] || m.RPC <= n.acked[m.From] {
		return nil, nil
	}
	n.acked[m.From] = m.RPC
	if !m.Success {
		next := n.next[m.From]
		if next > 1 {
			next--
		}
		if m.Hint > 0 && m.Hint < next {
			next = m.Hint
		}
		if next < n.match[m.From]+1 {
			next = n.match[m.From] + 1
		}
		n.next[m.From] = next
		return []Message{n.appendTo(m.From)}, nil
	}
	if m.Match > uint64(len(n.state.Log)-1) {
		return nil, ErrInvalid
	}
	if m.Match > n.match[m.From] {
		n.match[m.From] = m.Match
	}
	n.next[m.From] = n.match[m.From] + 1
	for index := uint64(len(n.state.Log) - 1); index > n.state.Commit; index-- {
		if n.state.Log[index].Term != n.state.Term {
			continue
		}
		count := 0
		for _, id := range n.cfg.Members {
			if n.match[id] >= index {
				count++
			}
		}
		if count > len(n.cfg.Members)/2 {
			next := clone(n.state)
			next.Commit = index
			if err := n.persist(next); err != nil {
				return nil, err
			}
			return n.broadcast(), nil
		}
	}
	if n.next[m.From] < uint64(len(n.state.Log)) {
		return []Message{n.appendTo(m.From)}, nil
	}
	return nil, nil
}
