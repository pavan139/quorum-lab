package simulator

import (
	"fmt"
	"math/rand"
	"quorumlab/raft"
	"sort"
)

const MaxQueue = 4096
const MaxTrace = 256

type Event struct {
	Number  int       `json:"number"`
	Kind    string    `json:"kind"`
	Node    int       `json:"node,omitempty"`
	From    int       `json:"from,omitempty"`
	To      int       `json:"to,omitempty"`
	Term    uint64    `json:"term,omitempty"`
	Message raft.Kind `json:"message,omitempty"`
	Reason  string    `json:"reason,omitempty"`
}
type Simulator struct {
	IDs        []int
	Seed       int64
	Nodes      map[int]*raft.Node
	Stores     map[int]*raft.MemoryStore
	Queue      []raft.Message
	partition  map[[2]int]bool
	rng        *rand.Rand
	Checker    *Checker
	Trace      []Event
	Events     int
	Ticks      int
	Delivered  int
	Reordered  int
	Dropped    int
	Crashes    int
	Restarts   int
	Milestones []Event
}

func New(size int, seed int64) (*Simulator, error) {
	if size != 3 && size != 5 {
		return nil, raft.ErrInvalid
	}
	s := &Simulator{Seed: seed, Nodes: map[int]*raft.Node{}, Stores: map[int]*raft.MemoryStore{}, partition: map[[2]int]bool{}, rng: rand.New(rand.NewSource(seed)), Checker: NewChecker()}
	for id := 1; id <= size; id++ {
		s.IDs = append(s.IDs, id)
		s.Stores[id] = &raft.MemoryStore{}
	}
	for _, id := range s.IDs {
		node, err := raft.New(s.config(id), s.Stores[id])
		if err != nil {
			return nil, err
		}
		s.Nodes[id] = node
	}
	return s, s.observe()
}
func (s *Simulator) config(id int) raft.Config {
	return raft.Config{ID: id, Members: s.IDs, ElectionTicks: 10 + 2*id, HeartbeatTicks: 2, RandomSeed: s.Seed}
}
func (s *Simulator) event(event Event) {
	s.Events++
	event.Number = s.Events
	if len(s.Trace) < MaxTrace {
		s.Trace = append(s.Trace, event)
	}
	if event.Kind != "deliver" && event.Kind != "drop" && len(s.Milestones) < 128 {
		s.Milestones = append(s.Milestones, event)
	}
}
func (s *Simulator) observe() error {
	states := map[int]raft.HardState{}
	status := map[int]raft.Status{}
	for _, id := range s.IDs {
		state, err := s.Stores[id].Load()
		if err != nil {
			return err
		}
		states[id] = state
		if node := s.Nodes[id]; node != nil {
			status[id] = node.Status()
		}
	}
	for _, id := range s.IDs {
		if st, ok := status[id]; ok && st.Role == raft.Leader && s.Checker.Leaders[st.Term] == 0 {
			s.event(Event{Kind: "leader_observed", Node: id, Term: st.Term})
		}
	}
	return s.Checker.Check(states, status)
}
func (s *Simulator) enqueue(messages []raft.Message) error {
	if len(s.Queue)+len(messages) > MaxQueue {
		return raft.ErrBudget
	}
	for _, m := range messages {
		s.Queue = append(s.Queue, m.Copy())
	}
	return nil
}
func (s *Simulator) Partition(groups [][]int) error {
	assignment := map[int]int{}
	for index, group := range groups {
		if len(group) == 0 {
			return raft.ErrInvalid
		}
		for _, id := range group {
			if _, ok := s.Stores[id]; !ok {
				return raft.ErrInvalid
			}
			if _, ok := assignment[id]; ok {
				return raft.ErrInvalid
			}
			assignment[id] = index
		}
	}
	if len(assignment) != len(s.IDs) {
		return raft.ErrInvalid
	}
	next := map[[2]int]bool{}
	for _, a := range s.IDs {
		for _, b := range s.IDs {
			if assignment[a] != assignment[b] {
				next[[2]int{a, b}] = true
			}
		}
	}
	s.partition = next
	s.event(Event{Kind: "partition"})
	return s.observe()
}
func (s *Simulator) Heal() error {
	s.partition = map[[2]int]bool{}
	s.event(Event{Kind: "heal"})
	return s.observe()
}
func (s *Simulator) Crash(id int) error {
	if s.Nodes[id] == nil {
		return raft.ErrInvalid
	}
	s.Nodes[id] = nil
	s.Crashes++
	s.event(Event{Kind: "crash", Node: id})
	return s.observe()
}
func (s *Simulator) Restart(id int) error {
	if _, ok := s.Stores[id]; !ok || s.Nodes[id] != nil {
		return raft.ErrInvalid
	}
	node, err := raft.New(s.config(id), s.Stores[id])
	if err != nil {
		return err
	}
	s.Nodes[id] = node
	s.Restarts++
	s.event(Event{Kind: "restart", Node: id, Term: node.Status().Term})
	return s.observe()
}
func (s *Simulator) Deliver(index int) error {
	if index < 0 || index >= len(s.Queue) {
		return raft.ErrInvalid
	}
	m := s.Queue[index]
	s.Queue = append(s.Queue[:index], s.Queue[index+1:]...)
	if s.Nodes[m.To] == nil || s.partition[[2]int{m.From, m.To}] {
		s.Dropped++
		s.event(Event{Kind: "drop", From: m.From, To: m.To, Term: m.Term, Message: m.Kind, Reason: "partition_or_crashed_destination"})
		return nil
	}
	messages, err := s.Nodes[m.To].Step(m)
	if index != 0 {
		s.Reordered++
	}
	s.Delivered++
	s.event(Event{Kind: "deliver", From: m.From, To: m.To, Term: m.Term, Message: m.Kind})
	if err != nil {
		return err
	}
	if err = s.enqueue(messages); err != nil {
		return err
	}
	return s.observe()
}
func (s *Simulator) Drop(index int) error {
	if index < 0 || index >= len(s.Queue) {
		return raft.ErrInvalid
	}
	m := s.Queue[index]
	s.Queue = append(s.Queue[:index], s.Queue[index+1:]...)
	s.Dropped++
	s.event(Event{Kind: "drop", From: m.From, To: m.To, Term: m.Term, Message: m.Kind, Reason: "seeded_loss"})
	return nil
}
func (s *Simulator) Advance(ticks, loss int) error {
	if ticks < 0 || ticks > 1000 || loss < 0 || loss > 100 || s.Ticks+ticks > 3000 {
		return raft.ErrBudget
	}
	for t := 0; t < ticks; t++ {
		s.Ticks++
		for _, id := range s.IDs {
			if node := s.Nodes[id]; node != nil {
				out, err := node.Tick()
				if err != nil {
					return err
				}
				if err = s.enqueue(out); err != nil {
					return err
				}
				if err = s.observe(); err != nil {
					return err
				}
			}
		}
		for count := 0; count < 64 && len(s.Queue) > 0; count++ {
			index := s.rng.Intn(len(s.Queue))
			var err error
			if s.rng.Intn(100) < loss {
				err = s.Drop(index)
			} else {
				err = s.Deliver(index)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}
func (s *Simulator) Leader(ids []int) (int, error) {
	best := 0
	var term uint64
	for _, id := range ids {
		if node := s.Nodes[id]; node != nil {
			st := node.Status()
			if st.Role == raft.Leader && (best == 0 || st.Term > term) {
				best, term = id, st.Term
			}
		}
	}
	if best == 0 {
		return 0, fmt.Errorf("no leader within bounded schedule")
	}
	return best, nil
}
func (s *Simulator) Propose(id int, command string) error {
	if s.Nodes[id] == nil {
		return raft.ErrNotLeader
	}
	_, out, err := s.Nodes[id].Propose(command)
	if err != nil {
		return err
	}
	s.event(Event{Kind: "propose", Node: id, Term: s.Nodes[id].Status().Term})
	if err = s.enqueue(out); err != nil {
		return err
	}
	return s.observe()
}
func (s *Simulator) Commands() []string {
	indices := []int{}
	for index := range s.Checker.Committed {
		indices = append(indices, int(index))
	}
	sort.Ints(indices)
	result := []string{}
	for _, index := range indices {
		entry := s.Checker.Committed[uint64(index)]
		if !entry.Noop {
			result = append(result, entry.Command)
		}
	}
	return result
}
