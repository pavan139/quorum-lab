package raft

import (
	"errors"
	"reflect"
	"testing"
)

func cfg(id int) Config {
	return Config{ID: id, Members: []int{1, 2, 3}, ElectionTicks: 8, HeartbeatTicks: 2}
}
func node(t *testing.T, id int, store Store) *Node {
	t.Helper()
	n, err := New(cfg(id), store)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func elect(t *testing.T, n *Node) {
	t.Helper()
	if _, err := n.Campaign(); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Step(Message{Kind: VoteResponse, From: 2, To: n.cfg.ID, Term: n.state.Term, Granted: true}); err != nil {
		t.Fatal(err)
	}
	if n.role != Leader {
		t.Fatal("not elected")
	}
}
func saved(t *testing.T, store Store) HardState {
	t.Helper()
	s, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func preset(t *testing.T, term, commit uint64, entries []Entry) *MemoryStore {
	t.Helper()
	s := &MemoryStore{}
	state := HardState{Version: 1, ID: 1, Members: []int{1, 2, 3}, Term: term, Log: append([]Entry{{}}, entries...), Commit: commit}
	if err := s.Save(state); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestVotePersistedBeforeReplyAndRestart(t *testing.T) {
	s := &MemoryStore{}
	n := node(t, 2, s)
	out, err := n.Step(Message{Kind: RequestVote, From: 1, To: 2, Term: 3})
	if err != nil || len(out) != 1 || !out[0].Granted {
		t.Fatal(out, err)
	}
	if state := saved(t, s); state.Term != 3 || state.VotedFor != 1 {
		t.Fatal(state)
	}
	n = node(t, 2, s)
	out, err = n.Step(Message{Kind: RequestVote, From: 3, To: 2, Term: 3})
	if err != nil || out[0].Granted {
		t.Fatal("double vote", out, err)
	}
}
func TestElectionSelfVoteAndTermAreDurable(t *testing.T) {
	s := &MemoryStore{}
	n := node(t, 1, s)
	if _, err := n.Campaign(); err != nil {
		t.Fatal(err)
	}
	n = node(t, 1, s)
	if n.state.Term != 1 || n.state.VotedFor != 1 || n.role != Follower {
		t.Fatal(n.Status())
	}
}
func TestStorageFailureEmitsNoVoteAndHalts(t *testing.T) {
	s := &MemoryStore{}
	n := node(t, 2, s)
	s.Fail = true
	out, err := n.Step(Message{Kind: RequestVote, From: 1, To: 2, Term: 2})
	if !errors.Is(err, ErrStorage) || len(out) != 0 || n.role != Halted {
		t.Fatal(out, err)
	}
	s.Fail = false
	if state := saved(t, s); state.Term != 0 || state.VotedFor != 0 {
		t.Fatal(state)
	}
	if _, err = n.Tick(); !errors.Is(err, ErrStorage) {
		t.Fatal(err)
	}
}
func TestLeaderPersistsNoopAndProposalBeforeTransport(t *testing.T) {
	s := &MemoryStore{}
	n := node(t, 1, s)
	elect(t, n)
	index, out, err := n.Propose("write-a")
	if err != nil || index != 2 || len(out) != 2 {
		t.Fatal(index, out, err)
	}
	state := saved(t, s)
	if !state.Log[1].Noop || state.Log[2].Command != "write-a" {
		t.Fatal(state)
	}
	s.Fail = true
	outIndex, out, err := n.Propose("never-durable")
	if outIndex != 0 || len(out) != 0 || !errors.Is(err, ErrStorage) || n.state.Log[len(n.state.Log)-1].Command != "write-a" {
		t.Fatal(outIndex, out, err)
	}
}
func TestLogFreshnessUsesTermBeforeLength(t *testing.T) {
	s := &MemoryStore{}
	initial := HardState{Version: 1, ID: 2, Members: []int{1, 2, 3}, Term: 4, Log: []Entry{{}, {Index: 1, Term: 3, Command: "existing"}, {Index: 2, Term: 3, Command: "newer"}}}
	if err := s.Save(initial); err != nil {
		t.Fatal(err)
	}
	n := node(t, 2, s)
	for _, m := range []Message{{Kind: RequestVote, From: 1, To: 2, Term: 5, LastIndex: 100, LastTerm: 2}, {Kind: RequestVote, From: 1, To: 2, Term: 5, LastIndex: 1, LastTerm: 3}} {
		out, err := n.Step(m)
		if err != nil || out[0].Granted {
			t.Fatal(out, err)
		}
	}
	out, err := n.Step(Message{Kind: RequestVote, From: 3, To: 2, Term: 5, LastIndex: 2, LastTerm: 3})
	if err != nil || !out[0].Granted {
		t.Fatal(out, err)
	}
}
func TestRejectedHigherTermVoteDoesNotPostponeElection(t *testing.T) {
	s := preset(t, 4, 0, []Entry{{Index: 1, Term: 4, Command: "newer"}})
	n := node(t, 1, s)
	n.elapsed = n.electionDeadline - 1
	before := n.elapsed
	out, err := n.Step(Message{Kind: RequestVote, From: 2, To: 1, Term: 5, LastIndex: 0, LastTerm: 0})
	if err != nil || out[0].Granted || n.elapsed != before {
		t.Fatal("denied vote reset timer", out, err)
	}
	out, err = n.Tick()
	if err != nil || n.role != Candidate || n.state.Term != 6 || len(out) != 2 {
		t.Fatal(n.Status(), err)
	}
}
func TestCurrentTermQuorumRuleWithRealPartialBatches(t *testing.T) {
	entries := []Entry{}
	for i := 1; i <= 40; i++ {
		entries = append(entries, Entry{Index: uint64(i), Term: 1, Command: "old"})
	}
	n := node(t, 1, preset(t, 2, 0, entries))
	elect(t, n)
	term := n.state.Term
	firstRPC := n.sent[2]
	out, err := n.Step(Message{Kind: AppendResponse, From: 2, To: 1, Term: term, RPC: firstRPC, Hint: 1})
	if err != nil || len(out) != 1 || len(out[0].Entries) != MaxBatch {
		t.Fatal(out, err)
	}
	out, err = n.Step(Message{Kind: AppendResponse, From: 2, To: 1, Term: term, RPC: out[0].RPC, Success: true, Match: 32})
	if err != nil || n.state.Commit != 0 || len(out) != 1 {
		t.Fatal("old-term replica count committed", n.Status(), out, err)
	}
	_, err = n.Step(Message{Kind: AppendResponse, From: 2, To: 1, Term: term, RPC: out[0].RPC, Success: true, Match: 41})
	if err != nil || n.state.Commit != 41 {
		t.Fatal(n.Status(), err)
	}
}
func TestStaleFailureDoesNotRegressAcknowledgedPrefix(t *testing.T) {
	n := node(t, 1, &MemoryStore{})
	elect(t, n)
	rpc := n.sent[2]
	out, err := n.Step(Message{Kind: AppendResponse, From: 2, To: 1, Term: n.state.Term, RPC: rpc, Hint: 1})
	if err != nil {
		t.Fatal(err)
	}
	_, err = n.Step(Message{Kind: AppendResponse, From: 2, To: 1, Term: n.state.Term, RPC: out[0].RPC, Success: true, Match: 1})
	if err != nil {
		t.Fatal(err)
	}
	before := n.next[2]
	out, err = n.Step(Message{Kind: AppendResponse, From: 2, To: 1, Term: n.state.Term, RPC: rpc, Hint: 1})
	if err != nil || len(out) != 0 || n.next[2] != before || n.match[2] != 1 {
		t.Fatal(out, err)
	}
}
func TestConflictTruncatesOnlyUncommittedSuffix(t *testing.T) {
	s := preset(t, 3, 1, []Entry{{1, 1, "kept", false}, {2, 2, "divergent", false}, {3, 2, "tail", false}})
	n := node(t, 1, s)
	out, err := n.Step(Message{Kind: AppendEntries, From: 2, To: 1, Term: 4, PrevIndex: 1, PrevTerm: 1, Entries: []Entry{{2, 4, "repaired", false}}, LeaderCommit: 2, RPC: 1})
	if err != nil || !out[0].Success || len(n.state.Log) != 3 || n.state.Commit != 2 {
		t.Fatal(n.Stable(), out, err)
	}
	restarted := node(t, 1, s)
	if restarted.Applied()[1].Command != "repaired" {
		t.Fatal(restarted.Applied())
	}
}
func TestCommittedPrefixCannotBeOverwritten(t *testing.T) {
	s := preset(t, 3, 2, []Entry{{1, 1, "kept", false}, {2, 2, "committed", false}})
	n := node(t, 1, s)
	out, err := n.Step(Message{Kind: AppendEntries, From: 2, To: 1, Term: 4, PrevIndex: 1, PrevTerm: 1, Entries: []Entry{{2, 4, "wrong", false}}, RPC: 1})
	if !errors.Is(err, ErrCommittedConflict) || len(out) != 0 || n.state.Log[2].Command != "committed" {
		t.Fatal(out, err)
	}
}
func TestPartialAppendCannotCommitUnverifiedLocalTail(t *testing.T) {
	entries := []Entry{}
	for i := 1; i <= 40; i++ {
		entries = append(entries, Entry{uint64(i), 1, "old-tail", false})
	}
	n := node(t, 1, preset(t, 1, 0, entries))
	_, err := n.Step(Message{Kind: AppendEntries, From: 2, To: 1, Term: 2, LeaderCommit: 40, RPC: 1})
	if err != nil || n.state.Commit != 0 {
		t.Fatal("heartbeat committed unverified suffix", n.Status(), err)
	}
	_, err = n.Step(Message{Kind: AppendEntries, From: 2, To: 1, Term: 2, Entries: entries[:32], LeaderCommit: 40, RPC: 2})
	if err != nil || n.state.Commit != 32 {
		t.Fatal(n.Status(), err)
	}
	if len(n.state.Log) != 41 {
		t.Fatal("matching heartbeat truncated extra suffix")
	}
}
func TestDuplicateVotesCannotCreateQuorum(t *testing.T) {
	ids := []int{1, 2, 3, 4, 5}
	n, err := New(Config{ID: 1, Members: ids, ElectionTicks: 8, HeartbeatTicks: 2}, &MemoryStore{})
	if err != nil {
		t.Fatal(err)
	}
	n.Campaign()
	for i := 0; i < 3; i++ {
		n.Step(Message{Kind: VoteResponse, From: 2, To: 1, Term: 1, Granted: true})
	}
	if n.role != Candidate {
		t.Fatal("duplicates counted as votes")
	}
	n.Step(Message{Kind: VoteResponse, From: 3, To: 1, Term: 1, Granted: true})
	if n.role != Leader {
		t.Fatal(n.Status())
	}
}
func TestHigherTermResponseStepsLeaderDown(t *testing.T) {
	n := node(t, 1, &MemoryStore{})
	elect(t, n)
	_, err := n.Step(Message{Kind: AppendResponse, From: 2, To: 1, Term: 5, RPC: 1})
	if err != nil || n.role != Follower || n.state.Term != 5 || n.state.VotedFor != 0 {
		t.Fatal(n.Status(), err)
	}
}
func TestMalformedUnknownAndOversizedInputsDoNotMutate(t *testing.T) {
	n := node(t, 1, &MemoryStore{})
	before := n.Stable()
	tests := []Message{{Kind: RequestVote, From: 9, To: 1, Term: 1}, {Kind: "shell", From: 2, To: 1, Term: 1}, {Kind: AppendEntries, From: 2, To: 1, Term: 1, Entries: []Entry{{Index: 2, Term: 1, Command: "bad-index"}}}, {Kind: RequestVote, From: 2, To: 1, Term: MaxTerm + 1}}
	for _, m := range tests {
		if _, err := n.Step(m); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, n.Stable()) {
			t.Fatal("invalid packet mutated stable state")
		}
	}
	if _, err := New(Config{ID: 1, Members: []int{1, 2}, ElectionTicks: 8, HeartbeatTicks: 2}, &MemoryStore{}); err == nil {
		t.Fatal("dynamic/invalid membership accepted")
	}
}

func TestFollowerDoesNotAcknowledgeOrApplyFailedLogWrite(t *testing.T) {
	store := &MemoryStore{}
	n := node(t, 2, store)
	if _, err := n.Step(Message{Kind: RequestVote, From: 1, To: 2, Term: 1}); err != nil {
		t.Fatal(err)
	}
	store.Fail = true
	out, err := n.Step(Message{Kind: AppendEntries, From: 1, To: 2, Term: 1, Entries: []Entry{{Index: 1, Term: 1, Noop: true}}, LeaderCommit: 1, RPC: 1})
	if !errors.Is(err, ErrStorage) || len(out) != 0 || n.role != Halted || n.state.Commit != 0 || len(n.Applied()) != 0 {
		t.Fatal(n.Status(), out, err)
	}
	store.Fail = false
	state := saved(t, store)
	if len(state.Log) != 1 || state.Commit != 0 {
		t.Fatal("unpersisted append escaped", state)
	}
}
func TestLogAndTermBudgetsDoNotFabricateSuccess(t *testing.T) {
	entries := []Entry{}
	for i := 1; i < MaxEntries; i++ {
		entries = append(entries, Entry{uint64(i), 1, "bounded", false})
	}
	n := node(t, 1, preset(t, 1, 0, entries))
	elect(t, n)
	if _, out, err := n.Propose("over-capacity"); !errors.Is(err, ErrBudget) || len(out) != 0 {
		t.Fatal(out, err)
	}
	n = node(t, 1, preset(t, MaxTerm, 0, nil))
	if _, err := n.Campaign(); !errors.Is(err, ErrBudget) || n.state.Term != MaxTerm {
		t.Fatal(n.Status(), err)
	}
}
