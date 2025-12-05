package simulator

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"quorumlab/raft"
	"reflect"
)

type Result struct {
	Seed             int64          `json:"seed"`
	Members          int            `json:"members"`
	Passed           bool           `json:"passed"`
	Error            string         `json:"error,omitempty"`
	Ticks            int            `json:"ticks"`
	Delivered        int            `json:"messages_delivered"`
	Reordered        int            `json:"out_of_fifo_deliveries"`
	Dropped          int            `json:"messages_dropped"`
	Crashes          int            `json:"crashes"`
	Restarts         int            `json:"restarts"`
	SafetyChecks     int            `json:"safety_observations"`
	Commands         []string       `json:"committed_commands"`
	Statuses         []raft.Status  `json:"final_nodes"`
	Trace            []Event        `json:"trace"`
	Omitted          int            `json:"trace_events_omitted"`
	Milestones       []Event        `json:"milestones"`
	Leaders          map[uint64]int `json:"elected_leaders_by_term"`
	CommittedHistory []raft.Entry   `json:"committed_history"`
}

func Scenario(size int, seed int64) Result {
	result := Result{Seed: seed, Members: size}
	s, err := New(size, seed)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	run := func() error {
		if err := s.Advance(40, 0); err != nil {
			return err
		}
		leader, err := s.Leader(s.IDs)
		if err != nil {
			return err
		}
		pre := fmt.Sprintf("seed-%d-pre", seed)
		majorityCommand := fmt.Sprintf("seed-%d-majority", seed)
		final := fmt.Sprintf("seed-%d-final", seed)
		isolated := fmt.Sprintf("seed-%d-uncommitted-minority", seed)
		if err = s.Propose(leader, pre); err != nil {
			return err
		}
		if err = s.Advance(15, 10); err != nil {
			return err
		}
		minority := []int{leader}
		majority := []int{}
		for _, id := range s.IDs {
			if id != leader {
				if size == 5 && len(minority) == 1 {
					minority = append(minority, id)
				} else {
					majority = append(majority, id)
				}
			}
		}
		if err = s.Partition([][]int{minority, majority}); err != nil {
			return err
		}
		if err = s.Propose(leader, isolated); err != nil {
			return err
		}
		if err = s.Advance(120, 15); err != nil {
			return err
		}
		replacement, err := s.Leader(majority)
		if err != nil {
			return err
		}
		if err = s.Propose(replacement, majorityCommand); err != nil {
			return err
		}
		if err = s.Advance(25, 15); err != nil {
			return err
		}
		if err = s.Crash(replacement); err != nil {
			return err
		}
		if err = s.Advance(10, 10); err != nil {
			return err
		}
		if err = s.Restart(replacement); err != nil {
			return err
		}
		if err = s.Heal(); err != nil {
			return err
		}
		if err = s.Advance(120, 10); err != nil {
			return err
		}
		finalLeader, err := s.Leader(s.IDs)
		if err != nil {
			return err
		}
		if err = s.Propose(finalLeader, final); err != nil {
			return err
		}
		if err = s.Advance(30, 0); err != nil {
			return err
		}
		commands := s.Commands()
		for _, wanted := range []string{pre, majorityCommand, final} {
			found := false
			for _, actual := range commands {
				if wanted == actual {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("expected command did not commit within bounded healing")
			}
		}
		for _, actual := range commands {
			if actual == isolated {
				return fmt.Errorf("minority-only command became committed")
			}
		}
		reference := s.Nodes[finalLeader].Stable()
		for _, id := range s.IDs {
			state := s.Nodes[id].Stable()
			if state.Commit != reference.Commit || !reflect.DeepEqual(state.Log[:state.Commit+1], reference.Log[:reference.Commit+1]) {
				return fmt.Errorf("committed prefixes did not converge")
			}
		}
		return nil
	}
	err = run()
	result.Passed = err == nil
	if err != nil {
		result.Error = err.Error()
	}
	result.Ticks = s.Ticks
	result.Delivered = s.Delivered
	result.Reordered = s.Reordered
	result.Dropped = s.Dropped
	result.Crashes = s.Crashes
	result.Restarts = s.Restarts
	result.SafetyChecks = s.Checker.Checks
	result.Commands = s.Commands()
	result.Trace = s.Trace
	result.Omitted = s.Events - len(s.Trace)
	result.Milestones = s.Milestones
	result.Leaders = s.Checker.Leaders
	for index := uint64(1); index <= uint64(len(s.Checker.Committed)); index++ {
		result.CommittedHistory = append(result.CommittedHistory, s.Checker.Committed[index])
	}
	for _, id := range s.IDs {
		if node := s.Nodes[id]; node != nil {
			result.Statuses = append(result.Statuses, node.Status())
		}
	}
	return result
}

type Evaluation struct {
	Dataset      string   `json:"dataset"`
	SHA256       string   `json:"dataset_sha256"`
	Passed       int      `json:"cases_passed"`
	Total        int      `json:"cases_total"`
	SafetyChecks int      `json:"safety_observations"`
	Runtime      string   `json:"runtime"`
	Cases        []Result `json:"cases"`
	Limitations  []string `json:"limitations"`
	Manifest     any      `json:"manifest"`
}

func Evaluate() Evaluation {
	manifest := struct {
		Version        string  `json:"version"`
		Sizes          []int   `json:"sizes"`
		Seeds          []int64 `json:"seeds"`
		Schedule       string  `json:"schedule"`
		Timing         string  `json:"election_timing"`
		Heartbeat      int     `json:"heartbeat_ticks"`
		DeliveryBudget int     `json:"deliveries_per_tick"`
	}{"raft-partition-restart-v1", []int{3, 5}, nil, "40 stable +15 prewrite +120 partition +25 majority +10 crash +120 heal +30 final; losses 0%,10%,15%,15%,10%,10%,0%", "randomized [10+2*ID, 2*(10+2*ID)); seed+1009*ID+7919*restart_term", 2, 64}
	for seed := int64(1); seed <= 12; seed++ {
		manifest.Seeds = append(manifest.Seeds, seed)
	}
	encoded, _ := json.Marshal(manifest)
	sum := sha256.Sum256(encoded)
	report := Evaluation{Dataset: manifest.Version, SHA256: hex.EncodeToString(sum[:]), Runtime: "deterministic single-process simulator; memory stable stores survive node restart", Limitations: []string{"bounded evidence, not a formal proof", "no real network or OS/power failure simulation", "static 3/5 membership only", "no snapshots, read-index, linearizable read API, or exactly-once clients"}}
	report.Manifest = manifest
	for _, size := range manifest.Sizes {
		for _, seed := range manifest.Seeds {
			result := Scenario(size, seed)
			report.Cases = append(report.Cases, result)
			report.Total++
			report.SafetyChecks += result.SafetyChecks
			if result.Passed {
				report.Passed++
			}
		}
	}
	return report
}
