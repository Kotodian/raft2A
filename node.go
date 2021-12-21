package main

import (
	"github.com/Kotodian/raft2A/utils"
	"log"
	"math/rand"
	"sync"
	"time"
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
	// peers 所有节点
	peers []*utils.ClientEnd
	// me 当前节点的索引值
	me int
	// role 角色 如上
	role Role

	// 当前的任期
	currentTerm int
	// CandidateId
	votedFor int
	// 上次重置竞选时间
	lastResetElectionTimer int64
	// 竞选超时时间
	timeoutElection int64
	// true表示可以开始选举
	timerElectionChan chan bool
	// true表示可以开始发送心跳
	timerHeartbeatChan chan bool
	// 心跳等待时间
	lastResetHeartbeatTimer int64
	// 心跳超时时间
	timeoutHeartbeat int64
}

func NewRaftNode(peers []*utils.ClientEnd, me int) *RaftNode {
	rf := &RaftNode{}
	rf.peers = peers
	rf.me = me
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.timerHeartbeatChan = make(chan bool)
	rf.timerElectionChan = make(chan bool)
	rf.timeoutHeartbeat = 100 // ms
	rf.resetTimerElection()

	DPrintf("Starting raft: %d\n", me)

	go rf.mainLoop()
	go rf.timerElection()
	go rf.timerHeartbeat()

	return rf
}

func (r *RaftNode) mainLoop() {
	for {
		select {
		case <-r.timerElectionChan:
			r.startElection()
		case <-r.timerHeartbeatChan:
			r.broadcastHeartbeat()
		}
	}
}

// startElection 开始选举
func (r *RaftNode) startElection() {
	r.mu.Lock()
	// 变成候选者
	r.BecomeCandidate()
	// 给自己投票
	nVote := 1
	DPrintf("[startElection] raft %d start election | current term: %d | current role: %d\n",
		r.me, r.currentTerm, r.role)
	r.mu.Unlock()
	// 广播消息拉票
	for i := 0; i < len(r.peers); i++ {
		if i == r.me {
			continue
		}
		go func(id int) {
			r.mu.Lock()
			args := RequestVoteReq{
				Term:        r.currentTerm,
				CandidateId: r.me,
			}
			// 请求之前需解锁不然就死锁了
			r.mu.Unlock()
			var reply RequestVoteReply
			if r.sendRequestVote(id, &args, &reply) {
				r.mu.Lock()
				defer r.mu.Unlock()
				if r.currentTerm != args.Term {
					return
				}
				// 拉票成功
				if reply.VoteGranted {
					nVote += 1

					DPrintf("[requestVoteAsync] raft %d get accept vote from %d | current term: %d | current state: %d | reply term: %d | poll %d\n",
						r.me, id, r.currentTerm, r.role, reply.Term, nVote)

					// 如果超过半数且当前角色是候选人时
					if nVote > len(r.peers)/2 && r.role == Candidate {
						// 变成领导者
						r.BecomeLeader()
						DPrintf("[requestVoteAsync] raft %d convert to leader | current term: %d | current state: %d",
							r.me, r.currentTerm, r.role)
						r.mu.Unlock()
						r.broadcastHeartbeat()
						r.mu.Lock()
					}
				} else {
					DPrintf("[requestVoteAsync] raft %d get reject vote from %d | current term: %d | current state: %d",
						r.me, r.currentTerm, r.role)
					// 拉票失败并且存在节点的任期大于当前任期 变成跟随者
					if r.currentTerm < reply.Term {
						r.BecomeFollower()
						r.currentTerm = reply.Term
					}
				}
			} else {
				r.mu.Lock()
				DPrintf("[requestVoteAsync] raft %d RPC to %d failed | current term: %d | current state: %d",
					r.me, id, r.currentTerm, r.role)
				r.mu.Unlock()
			}
		}(i)
	}
}

// timerElection 触发超时选举
func (r *RaftNode) timerElection() {
	for {
		r.mu.Lock()
		if r.role != Leader {
			timeElapsed := (time.Now().UnixNano() - r.lastResetElectionTimer) / time.Hour.Milliseconds()
			if timeElapsed > r.timeoutElection {
				DPrintf("[timerElection] raft %d election timeout | current term: %d | current state: %d\n",
					r.me, r.currentTerm, r.role)
				r.timerElectionChan <- true
			}
		}
		r.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *RaftNode) timerHeartbeat() {
	for {
		r.mu.Lock()
		if r.role == Leader {
			timeElapsed := (time.Now().UnixNano() - r.lastResetElectionTimer) / time.Hour.Milliseconds()
			if timeElapsed > r.timeoutHeartbeat {
				DPrintf("[timerHeartbeat] raft %d heartbeat timeout | current term: %d | current state: %d\n",
					r.me, r.currentTerm, r.role)
				r.timerHeartbeatChan <- true
			}
		}
		r.mu.Unlock()
		time.Sleep(time.Millisecond * 10)
	}
}

func (r *RaftNode) broadcastHeartbeat() {
	r.mu.Lock()
	// 不是领导者没有权限发送心跳RPC
	if r.role != Leader {
		DPrintf("[broadcastHeartbeat] raft %d lost leadership | current term: %d | current state: %d",
			r.me, r.currentTerm, r.role)
		r.mu.Unlock()
		return
	}
	r.resetTimerHeartbeat()
	r.mu.Unlock()
	for i := 0; i < len(r.peers); i++ {
		if i == r.me {
			continue
		}
		go func(id int) {
			r.mu.Lock()
			args := AppendEntriesReq{
				Term: r.currentTerm,
			}
			r.mu.Unlock()
			var reply AppendEntriesReply
			if r.sendAppendEntries(id, &args, &reply) {
				r.mu.Lock()
				defer r.mu.Unlock()

				if r.role != Leader {
					DPrintf("[broadcastHeartbeat] raft %d lost leadership | current term: %d | current state: %d",
						r.me, r.currentTerm, r.role)
					return
				}

				if r.currentTerm != reply.Term {
					DPrintf("[broadcastHeartbeat] raft %d term inconsistency | current term: %d | current state: %d | args term: %d",
						r.me, r.currentTerm, r.role, args.Term)
					return
				}

				if reply.Success {
					DPrintf("[appendEntriesAsync] raft %d append entries to %d accepted | current term: %d | current state: %d",
						r.me, id, r.currentTerm, r.role)
				} else {
					DPrintf("[appendEntriesAsync] raft %d append entries to %d rejected | current term: %d | current state: %d | reply term: %d",
						r.me, id, r.currentTerm, r.role, reply.Term)
					if reply.Term > r.currentTerm {
						r.BecomeFollower()
						r.currentTerm = reply.Term
						return
					}
				}
			} else {
				r.mu.Lock()
				DPrintf("[appendEntriesAsync] raft %d RPC to %d failed | current term: %d | current state: %d",
					r.me, id, r.currentTerm, r.role)
				r.mu.Unlock()
			}
		}(i)
	}

}

func (r *RaftNode) resetTimerElection() {
	rand.Seed(time.Now().UnixNano())
	r.timeoutElection = r.timeoutHeartbeat*5 + rand.Int63n(150)
	r.lastResetElectionTimer = time.Now().UnixNano()
}

func (r *RaftNode) resetTimerHeartbeat() {
	r.lastResetHeartbeatTimer = time.Now().UnixNano()
}

// BecomeFollower 状态变更为跟随者
func (r *RaftNode) BecomeFollower() {
	r.role = Follower
	r.votedFor = -1
}

// BecomeCandidate 状态变更为候选者,自身任期+1
func (r *RaftNode) BecomeCandidate() {
	r.currentTerm += 1
	r.role = Candidate
	r.votedFor = r.me
	r.resetTimerElection()
}

// BecomeLeader 状态变更为领导者,只有候选者能成为领导者
func (r *RaftNode) BecomeLeader() {
	r.role = Leader
	r.resetTimerHeartbeat()
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}
