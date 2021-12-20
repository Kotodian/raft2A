package main

import (
	"sync"
)

type Role int

const (
	// Follower 跟随者
	// 		1.回应来自候选人以及领导的rpc(例如心跳)调用
	//		2.如果一段时间未接收到当前的领导者的心跳就变成候选者
	Follower Role = 1
	// Candidate 候选者
	//  	1. 开始竞选领导者(自身任期+1，给自己投票，重置超时时间, 发送投票请求给其他服务器)
	//		2. 只有他能成为领导者
	//		3. 如果收到了新的领导者的请求就变成跟随者
	Candidate Role = 2
	// Leader 领导者
	//		1. todo
	Leader Role = 3
)

type RaftNode struct {
	// mu 锁 变更状态时需上锁
	mu sync.Mutex

	// role 角色 如上
	role Role

	// currentTerm 当前的任期
	currentTerm int

	// votedFor CandidateId
	votedFor int

	// electWaitDuration 竞选等待时间
	electWaitDuration int

	// heartbeatWaitDuration 心跳等待时间
	heartbeatWaitDuration int
}

func NewRaftNode() *RaftNode {
	// 初始化
	raftNode := &RaftNode{
		role:                  Follower,
		votedFor:              -1,
		electWaitDuration:     300,
		heartbeatWaitDuration: 120,
	}
	// 开始运行
	go raftNode.run()

	return raftNode
}

func (r *RaftNode) run() {

}

// BecomeFollower 状态变更为跟随者
func (r *RaftNode) BecomeFollower() {
	r.role = Follower
}

// BecomeCandidate 状态变更为候选者,自身任期+1
func (r *RaftNode) BecomeCandidate() {
	r.currentTerm += 1
	r.role = Candidate
}

// BecomeLeader 状态变更为领导者,只有候选者能成为领导者
func (r *RaftNode) BecomeLeader() {
	if r.role == Candidate {
		r.role = Leader
	} else {
		panic("follower can't be leader")
	}
}
