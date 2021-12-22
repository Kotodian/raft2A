package main

import (
	"github.com/Kotodian/raft2A/utils"
)

// RequestVoteReq 投票RPC
type RequestVoteReq struct {
	Term         int // 2A 候选人的任期
	CandidateId  int // 2A 候选人Id
	LastLogIndex int // 2B 候选人最后日志的索引值
	LastLogTerm  int // 2B 候选人最后日志的任期
}

// RequestVoteReply 投票RPC回复
type RequestVoteReply struct {
	Term        int  // 2A 当前节点的任期
	VoteGranted bool // 2A 是否同意拉票
}

// AppendEntriesReq 复制日志以及心跳RPC
type AppendEntriesReq struct {
	Term         int        // 2A 领导者的任期
	LeaderId     int        // 2A 领导者Id
	PrevLogIndex int        // 2B 紧邻新日志之前的那个日志条目的索引
	PrevLogTerm  int        // 2B 紧邻新日之前的那个日志条目的任期
	Entries      []LogEntry // 2B 需要被保存的日志条目(心跳RPC时为空)
	LeaderCommit int        // 2B 领导者已知的提交的最高的日志条目索引
}

type AppendEntriesReply struct {
	Term    int  // 2A 当前节点任期
	Success bool // 2A 调用结果

	ConflictIndex int // 扩展
}

// RequestVote 拉票服务端逻辑
func (r *RaftNode) RequestVote(req *RequestVoteReq, reply *RequestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 如果候选人的任期大于当前的任期,当前节点就直接变成跟随者
	if req.Term > r.currentTerm {
		r.BecomeFollower()
		r.currentTerm = req.Term
		r.persist()
	}

	// 如果候选人的任期小于当前任期, 拒绝投票
	if req.Term < r.currentTerm || (r.votedFor != -1 && r.votedFor != req.CandidateId) {
		DPrintf("[RequestVote] raft %d reject vote for %d | current term: %d | current state: %d | received term",
			r.me, req.CandidateId, r.currentTerm, r.role, req.Term)
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		return
	}

	// 返回结果并重置选举定时 2A
	if r.votedFor == -1 || r.votedFor == req.CandidateId {
		// 如果自己的日志比对方的新就拒绝投票
		lastLogIndex := len(r.log) - 1
		if r.log[lastLogIndex].Term > req.LastLogTerm ||
			(r.log[lastLogIndex].Term == req.LastLogTerm && req.LastLogIndex < lastLogIndex) {
			reply.Term = r.currentTerm
			reply.VoteGranted = false
			return
		}
		DPrintf("[RequestVote] raft %d accept vote for %d | current term: %d | current state: %d | received term",
			r.me, req.CandidateId, r.currentTerm, r.role, req.Term)
		reply.Term = r.currentTerm
		reply.VoteGranted = true
		r.votedFor = req.CandidateId
		r.persist()
		r.resetTimerElection()
	}
}

// sendRequestVote 客户端调用拉票rpc
func (r *RaftNode) sendRequestVote(server int, req *RequestVoteReq, reply *RequestVoteReply) bool {
	ok := r.peers[server].Call("Raft.RequestVote", req, reply)
	return ok
}

// AppendEntries 日志复制/心跳rpc服务端逻辑
func (r *RaftNode) AppendEntries(req *AppendEntriesReq, reply *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 如果领导人的任期小于接收者的任期 2A
	if req.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		DPrintf("[AppendEntries] raft %d reject append entries | current term: %d | current state: %d | received term: %d",
			r.me, r.currentTerm, r.role, req.Term)
		return
	}
	// 收到心跳包,更新选举超时时间 2A
	r.resetTimerElection()

	// 如果当前的任期小于请求的任期 那么自己必然是跟随者 2A
	if req.Term > r.currentTerm {
		r.BecomeFollower()
		r.currentTerm = req.Term
		DPrintf("[AppendEntries] raft %d update term | current term: %d | current state: %d | received term: %d",
			r.me, r.currentTerm, r.role, req.Term)
	} else if r.role == Candidate {
		r.BecomeFollower()
		DPrintf("[AppendEntries] raft %d update state | current term: %d | current state: %d | received term: %d",
			r.me, r.currentTerm, r.role, req.Term)
	}

	// 如果接受者中的日志没有包含条目或者条目的任期与请求者的任期对应上 2B
	if len(r.log) <= req.PrevLogIndex || r.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		DPrintf("[AppendEntries] raft %d reject append entry | log len: %d | received prev log index: %d | received prev log term: %d",
			r.me, len(r.log), req.PrevLogIndex, req.PrevLogTerm)
		reply.Term = r.currentTerm
		reply.Success = false
		if len(r.log) <= req.PrevLogIndex {
			reply.ConflictIndex = len(r.log)
		} else {
			for i := req.PrevLogIndex; i > 0; i-- {
				if r.log[i].Term != r.log[i-1].Term {
					reply.ConflictIndex = i
					break
				}
			}
		}
	} else {
		isMatch := true
		nextIndex := req.PrevLogIndex + 1
		conflictIndex := 0
		logLen := len(r.log)
		entLen := len(req.Entries)
		for i := 0; isMatch && i < entLen; i++ {
			// 如果一个已经存在的条目和新条目发生了冲突(索引相同,任期不同) 记录下这个索引并删除它以及之后的所有条目
			if (logLen-1 < nextIndex+1) || (r.log[nextIndex+i].Term != req.Entries[i].Term) {
				isMatch = false
				conflictIndex = i
				break
			}
		}
		// 删除它以及之后的所有条目
		if !isMatch {
			r.log = append(r.log[:nextIndex+conflictIndex], req.Entries[conflictIndex:]...)
			r.persist()
			DPrintf("[AppendEntries] raft %d appended entries from leader | log length: %d\n", r.me, len(r.log))
		}

		// 更新本地提交记录
		lastNewEntryIndex := req.PrevLogIndex + entLen
		// 如果领导人已知已提交的记录的索引大于接受者的已知已提交的最高日志条目的索引
		// 则把接受者的已知已经提交的最高的日志条目的索引重置为已知已经提交的最高的日志条目的索引或者是上一个新条目的索引取两者的最小值
		if req.LeaderCommit > r.commitIndex {
			r.commitIndex = utils.Min(req.LeaderCommit, lastNewEntryIndex)
			// 应用日志到状态机
			go r.applyEntries()
		}

		reply.Term = r.currentTerm
		reply.Success = true
	}

}

// sendAppendEntries 客户端调用心跳/日志复制rpc
func (r *RaftNode) sendAppendEntries(server int, req *AppendEntriesReq, reply *AppendEntriesReply) bool {
	ok := r.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}
