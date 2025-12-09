// Package simulator supplies an adversarial deterministic transport and an
// independent checker. It does not trust a node's success report.
package simulator

import (
	"fmt"
	"quorumlab/raft"
	"reflect"
)

type Checker struct {
	Checks      int                   `json:"checks"`
	Committed   map[uint64]raft.Entry `json:"-"`
	Leaders     map[uint64]int        `json:"-"`
	priorCommit map[int]uint64
	priorTerm   map[int]uint64
	votes       map[[2]uint64]int
}

func NewChecker() *Checker {
	return &Checker{Committed: map[uint64]raft.Entry{}, Leaders: map[uint64]int{}, priorCommit: map[int]uint64{}, priorTerm: map[int]uint64{}, votes: map[[2]uint64]int{}}
}
func (c *Checker) Check(states map[int]raft.HardState, status map[int]raft.Status) error {
	c.Checks++
	for id, s := range states {
		if s.Term < c.priorTerm[id] || s.Commit < c.priorCommit[id] {
			return fmt.Errorf("durable monotonicity violated at node %d", id)
		}
		if s.VotedFor != 0 {
			key := [2]uint64{uint64(id), s.Term}
			if before := c.votes[key]; before != 0 && before != s.VotedFor {
				return fmt.Errorf("double vote at node %d term %d", id, s.Term)
			}
			c.votes[key] = s.VotedFor
		}
		if st, ok := status[id]; ok && st.Role == raft.Leader {
			if elected := c.Leaders[s.Term]; elected != 0 && elected != id {
				return fmt.Errorf("election safety violated at term %d", s.Term)
			}
			if c.Leaders[s.Term] == 0 {
				for index, entry := range c.Committed {
					if index >= uint64(len(s.Log)) || s.Log[index] != entry {
						return fmt.Errorf("leader completeness violated at node %d index %d", id, index)
					}
				}
				c.Leaders[s.Term] = id
			}
			if s.Commit > c.priorCommit[id] {
				if s.Log[s.Commit].Term != s.Term {
					return fmt.Errorf("leader committed an old-term entry by counting replicas")
				}
				copies := 0
				for _, other := range states {
					if s.Commit < uint64(len(other.Log)) && reflect.DeepEqual(s.Log[:s.Commit+1], other.Log[:s.Commit+1]) {
						copies++
					}
				}
				if copies <= len(states)/2 {
					return fmt.Errorf("commit lacks a durable quorum")
				}
			}
		}
	}
	for a, left := range states {
		for b, right := range states {
			if a >= b {
				continue
			}
			end := len(left.Log)
			if len(right.Log) < end {
				end = len(right.Log)
			}
			for index := 1; index < end; index++ {
				if left.Log[index].Term == right.Log[index].Term && !reflect.DeepEqual(left.Log[:index+1], right.Log[:index+1]) {
					return fmt.Errorf("log matching violated at index %d", index)
				}
			}
		}
	}
	for id, s := range states {
		for index := uint64(1); index <= s.Commit; index++ {
			if previous, ok := c.Committed[index]; ok && previous != s.Log[index] {
				return fmt.Errorf("committed history diverged at index %d", index)
			}
			c.Committed[index] = s.Log[index]
		}
		c.priorCommit[id] = s.Commit
		c.priorTerm[id] = s.Term
	}
	return nil
}
