package simulator

import (
	"quorumlab/raft"
	"reflect"
	"strings"
	"testing"
)

func TestSeededStaticThreeAndFiveMemberFaultSchedules(t *testing.T) {
	report := Evaluate()
	if report.Passed != 24 || report.Total != 24 {
		for _, result := range report.Cases {
			if !result.Passed {
				t.Errorf("members=%d seed=%d ticks=%d: %s", result.Members, result.Seed, result.Ticks, result.Error)
			}
		}
	}
	if report.SafetyChecks < 1000 {
		t.Fatal("checker did not run after transitions")
	}
	for _, result := range report.Cases {
		if result.Crashes != 1 || result.Restarts != 1 || result.Dropped == 0 || len(result.Commands) != 3 {
			t.Fatal(result)
		}
		for _, command := range result.Commands {
			if strings.Contains(command, "uncommitted-minority") {
				t.Fatal("minority write committed")
			}
		}
		kinds := map[string]bool{}
		for _, event := range result.Milestones {
			kinds[event.Kind] = true
		}
		for _, required := range []string{"partition", "crash", "restart", "heal", "leader_observed"} {
			if !kinds[required] {
				t.Fatal("trace lost milestone", required)
			}
		}
	}
}
func TestReplayIsDeterministic(t *testing.T) {
	first, second := Scenario(3, 7), Scenario(3, 7)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("same seed and schedule diverged")
	}
}
func TestMinorityCannotCommitWithoutQuorum(t *testing.T) {
	for _, size := range []int{3, 5} {
		s, err := New(size, 42)
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Advance(40, 0); err != nil {
			t.Fatal(err)
		}
		leader, err := s.Leader(s.IDs)
		if err != nil {
			t.Fatal(err)
		}
		minority := []int{leader}
		others := []int{}
		for _, id := range s.IDs {
			if id != leader {
				if size == 5 && len(minority) == 1 {
					minority = append(minority, id)
				} else {
					others = append(others, id)
				}
			}
		}
		if err = s.Partition([][]int{minority, others}); err != nil {
			t.Fatal(err)
		}
		if err = s.Propose(leader, "minority-only"); err != nil {
			t.Fatal(err)
		}
		if err = s.Advance(80, 0); err != nil {
			t.Fatal(err)
		}
		for _, command := range s.Commands() {
			if command == "minority-only" {
				t.Fatal("nonquorum committed")
			}
		}
	}
}
func TestCrashRestartPreservesExactStableBytes(t *testing.T) {
	s, err := New(3, 9)
	if err != nil {
		t.Fatal(err)
	}
	s.Advance(40, 0)
	leader, err := s.Leader(s.IDs)
	if err != nil {
		t.Fatal(err)
	}
	s.Propose(leader, "persist-me")
	s.Advance(15, 0)
	before := s.Nodes[leader].Stable()
	if err = s.Crash(leader); err != nil {
		t.Fatal(err)
	}
	if err = s.Restart(leader); err != nil {
		t.Fatal(err)
	}
	after := s.Nodes[leader].Stable()
	if !reflect.DeepEqual(before, after) || s.Nodes[leader].Status().Role != raft.Follower {
		t.Fatal("restart lost stable data or revived leadership")
	}
}
func TestNetworkAndExplorationBoundsAreValidated(t *testing.T) {
	s, _ := New(3, 1)
	for _, groups := range [][][]int{{{1}, {2}}, {{1}, {1, 2, 3}}, {{1, 9}, {2, 3}}} {
		if err := s.Partition(groups); err == nil {
			t.Fatal("invalid partition accepted")
		}
	}
	if len(s.partition) != 0 {
		t.Fatal("invalid partition mutated network")
	}
	if s.Advance(1001, 0) == nil || s.Advance(1, 101) == nil || s.Deliver(0) == nil || s.Drop(0) == nil {
		t.Fatal("unbounded input accepted")
	}
	s.Queue = make([]raft.Message, MaxQueue)
	if s.enqueue([]raft.Message{{}}) == nil {
		t.Fatal("queue bound ignored")
	}
}
func baseStates() map[int]raft.HardState {
	states := map[int]raft.HardState{}
	for id := 1; id <= 3; id++ {
		states[id] = raft.HardState{Version: 1, ID: id, Members: []int{1, 2, 3}, Term: 2, Log: []raft.Entry{{}}}
	}
	return states
}
func TestIndependentCheckerRejectsInjectedSafetyFaults(t *testing.T) {
	for _, fault := range []string{"two-leaders", "divergent-commit", "log-matching", "old-term-commit", "no-quorum", "leader-completeness", "double-vote", "term-rollback"} {
		t.Run(fault, func(t *testing.T) {
			checker := NewChecker()
			states := baseStates()
			status := map[int]raft.Status{}
			entry := raft.Entry{Index: 1, Term: 2, Command: "a"}
			set := func(id int, e raft.Entry, commit uint64) {
				state := states[id]
				state.Log = append([]raft.Entry{{}}, e)
				state.Commit = commit
				states[id] = state
			}
			switch fault {
			case "two-leaders":
				status[1] = raft.Status{ID: 1, Role: raft.Leader}
				status[2] = raft.Status{ID: 2, Role: raft.Leader}
			case "divergent-commit":
				set(1, entry, 1)
				other := entry
				other.Term = 1
				other.Command = "b"
				set(2, other, 1)
			case "log-matching":
				set(1, entry, 0)
				entry.Command = "different"
				set(2, entry, 0)
			case "old-term-commit":
				entry.Term = 1
				for id := 1; id <= 3; id++ {
					set(id, entry, 1)
				}
				status[1] = raft.Status{ID: 1, Role: raft.Leader}
			case "no-quorum":
				set(1, entry, 1)
				status[1] = raft.Status{ID: 1, Role: raft.Leader}
			case "leader-completeness":
				set(1, entry, 1)
				if err := checker.Check(states, status); err != nil {
					t.Fatal(err)
				}
				state := states[2]
				state.Term = 3
				states[2] = state
				status[2] = raft.Status{ID: 2, Role: raft.Leader}
			case "double-vote":
				state := states[1]
				state.VotedFor = 2
				states[1] = state
				if err := checker.Check(states, status); err != nil {
					t.Fatal(err)
				}
				state.VotedFor = 3
				states[1] = state
			case "term-rollback":
				if err := checker.Check(states, status); err != nil {
					t.Fatal(err)
				}
				state := states[1]
				state.Term = 1
				states[1] = state
			}
			if err := checker.Check(states, status); err == nil {
				t.Fatal("checker accepted", fault)
			}
		})
	}
}
