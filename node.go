package main

import (
	"bytes"
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

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type RaftNode struct {
	// mu 锁 变更状态时需上锁 2A
	mu sync.Mutex
	// peers 所有节点 2A
	peers []*utils.ClientEnd
	// 存储当前节点的持久化状态
	persister *Persister
	// me 当前节点的索引值 2A
	me int
	// role 角色 如上 2A
	role Role

	// 当前的任期 2A
	currentTerm int
	// CandidateId 2A
	votedFor int
	// 上次重置竞选时间 2A
	lastResetElectionTimer int64
	// 竞选超时时间 2A
	timeoutElection int64
	// true表示可以开始选举 2A
	timerElectionChan chan bool
	// true表示可以开始发送心跳 2A
	timerHeartbeatChan chan bool
	// 心跳等待时间 2A
	lastResetHeartbeatTimer int64
	// 心跳超时时间 2A
	timeoutHeartbeat int64
	// 日志条目 2B
	log []LogEntry
	// 已知已提交的最高的日志条目的索引 2B
	commitIndex int
	// 已经被应用到状态机的最高的日志条目的索引 2B
	lastApplied int
	// 领导人的状态
	// 发送到该服务器的下一个日志索引 2B
	nextIndex []int
	// 已知的已经复制到该服务器的最高日志条目的索引 2B
	matchIndex []int

	applyCh chan ApplyMsg
}

func NewRaftNode(peers []*utils.ClientEnd, persister *Persister, me int, applyCh chan ApplyMsg) *RaftNode {
	// 2A
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

	// 2B
	rf.persister = persister
	rf.log = append(rf.log, LogEntry{Term: 0})
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.readPersist(persister.ReadRaftState())

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
	r.persist()
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
						// 在每次竞选成领导者之后重置
						for i := 0; i < len(r.peers); i++ {
							r.nextIndex[i] = len(r.log)
							r.matchIndex[i] = 0
						}
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
						r.persist()
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

// broadcastHeartbeat 广播心跳
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
		retry:
			r.mu.Lock()
			args := AppendEntriesReq{
				Term:         r.currentTerm,
				PrevLogIndex: r.nextIndex[id] - 1,           // 2B
				PrevLogTerm:  r.log[r.nextIndex[id]-1].Term, // 2B
				Entries:      r.log[r.nextIndex[id]:],       // 2B
				LeaderCommit: r.commitIndex,                 // 2B
			}
			r.mu.Unlock()
			// 每次发送前检查下自己是不是leader
			if _, isLeader := r.GetState(); !isLeader {
				return
			}
			var reply AppendEntriesReply
			if r.sendAppendEntries(id, &args, &reply) {
				r.mu.Lock()
				if r.role != Leader {
					DPrintf("[broadcastHeartbeat] raft %d lost leadership | current term: %d | current state: %d",
						r.me, r.currentTerm, r.role)
					r.mu.Unlock()
					return
				}

				if r.currentTerm != reply.Term {
					DPrintf("[broadcastHeartbeat] raft %d term inconsistency | current term: %d | current state: %d | args term: %d",
						r.me, r.currentTerm, r.role, args.Term)
					r.mu.Unlock()
					return
				}

				if reply.Success {
					DPrintf("[appendEntriesAsync] raft %d append entries to %d accepted | current term: %d | current state: %d",
						r.me, id, r.currentTerm, r.role)
					r.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
					r.nextIndex[id] = r.matchIndex[id] + 1
					r.checkN()
					r.mu.Unlock()
				} else {
					DPrintf("[appendEntriesAsync] raft %d append entries to %d rejected | current term: %d | current state: %d | reply term: %d",
						r.me, id, r.currentTerm, r.role, reply.Term)
					if reply.Term > r.currentTerm {
						r.BecomeFollower()
						r.currentTerm = reply.Term
						r.persist()
						r.mu.Unlock()
						return
					}
					r.nextIndex[id] = reply.ConflictIndex
					DPrintf("[appendEntriesAsync] raft %d append entries to %d rejected: decrement next index: %d",
						r.me, id, r.nextIndex[id])
					r.mu.Unlock()
					goto retry
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

// resetTimerElection 重置选举时间
func (r *RaftNode) resetTimerElection() {
	rand.Seed(time.Now().UnixNano())
	r.timeoutElection = r.timeoutHeartbeat*5 + rand.Int63n(150)
	r.lastResetElectionTimer = time.Now().UnixNano()
}

// resetTimerHeartbeat 重置心跳时间 2A
func (r *RaftNode) resetTimerHeartbeat() {
	r.lastResetHeartbeatTimer = time.Now().UnixNano()
}

// GetState 获取当前的状态(任期以及是否是领导者)
func (r *RaftNode) GetState() (term int, isLeader bool) {
	r.mu.Lock()
	term = r.currentTerm
	isLeader = r.role == Leader
	r.mu.Unlock()
	return
}

// BecomeFollower 状态变更为跟随者 2A
func (r *RaftNode) BecomeFollower() {
	r.role = Follower
	r.votedFor = -1
}

// BecomeCandidate 状态变更为候选者,自身任期+1 2A
func (r *RaftNode) BecomeCandidate() {
	r.currentTerm += 1
	r.role = Candidate
	r.votedFor = r.me
	r.resetTimerElection()
}

// BecomeLeader 状态变更为领导者,只有候选者能成为领导者 2A
func (r *RaftNode) BecomeLeader() {
	r.role = Leader
	r.resetTimerHeartbeat()
}

// persist 持久化状态 2B
func (r *RaftNode) persist() {
	w := new(bytes.Buffer)
	e := utils.NewEncoder(w)
	e.Encode(r.currentTerm)
	e.Encode(r.votedFor)
	e.Encode(r.log)
	data := w.Bytes()
	r.persister.SaveRaftState(data)
	DPrintf("[persist] raft: %d || currentTerm: %d || votedFor: %d || log len: %d\n",
		r.me, r.currentTerm, r.votedFor, len(r.log))
}

// readPersist 从持久化中读取当前状态
func (r *RaftNode) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	b := bytes.NewBuffer(data)
	d := utils.NewDecoder(b)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		DPrintf("[readPersist] error\n")
	} else {
		r.currentTerm = currentTerm
		r.votedFor = votedFor
		r.log = log
	}
}

func (r *RaftNode) applyEntries() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      r.log[i].Command,
			CommandIndex: i,
		}
		r.applyCh <- applyMsg
		r.lastApplied += 1
		DPrintf("[applyEntries] raft %d applied entry | last Applied: %d | commitIndex: %d\n",
			r.me, r.lastApplied, r.commitIndex)
	}
}

// checkN 领导者使用
// 如果超过半数的节点已经复制,确定好之后更新提交索引,然后应用到状态机
func (r *RaftNode) checkN() {
	for n := len(r.log) - 1; n > r.commitIndex; n-- {
		nReplicated := 0
		for i := 0; i < len(r.peers); i++ {
			if r.matchIndex[i] >= n && r.log[n].Term == r.currentTerm {
				nReplicated += 1
			}

			if nReplicated > len(r.peers)/2 {
				r.commitIndex = n
				go r.applyEntries()
				break
			}
		}
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	log.Printf(format, a...)
	return
}
