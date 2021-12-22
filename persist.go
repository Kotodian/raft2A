package main

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftState []byte
	snapshot  []byte
}

func NewPersister() *Persister {
	return &Persister{}
}

func (p *Persister) Copy() *Persister {
	p.mu.Lock()
	np := NewPersister()
	np.raftState = p.raftState
	np.snapshot = p.snapshot
	p.mu.Unlock()
	return np
}

func (p *Persister) SaveRaftState(state []byte) {
	p.mu.Lock()
	p.raftState = state
	p.mu.Unlock()
}

func (p *Persister) ReadRaftState() []byte {
	p.mu.Lock()
	state := p.raftState
	p.mu.Unlock()
	return state
}

func (p *Persister) RaftStateSize() int {
	p.mu.Lock()
	length := len(p.raftState)
	p.mu.Unlock()
	return length
}

func (p *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	p.mu.Lock()
	p.raftState = state
	p.snapshot = snapshot
	p.mu.Unlock()
}

func (p *Persister) SnapshotSize() int {
	p.mu.Lock()
	length := len(p.snapshot)
	p.mu.Unlock()
	return length
}
