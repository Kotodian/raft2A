package main

// RequestVoteReq 投票RPC
type RequestVoteReq struct {
	Term        int
	CandidateId int

	// 以下的暂时不用

	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply 投票RPC回复
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesReq 复制日志以及心跳RPC
type AppendEntriesReq struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote 拉票服务端逻辑
func (r *RaftNode) RequestVote(req *RequestVoteReq, reply *RequestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.Term > r.currentTerm {
		r.BecomeFollower()
		r.currentTerm = req.Term
	}

	if req.Term < r.currentTerm || (r.votedFor != -1 && r.votedFor != req.CandidateId) {
		DPrintf("[RequestVote] raft %d reject vote for %d | current term: %d | current state: %d | received term",
			r.me, req.CandidateId, r.currentTerm, r.role, req.Term)
		reply.Term = r.currentTerm
		reply.VoteGranted = false
		return
	}

	if r.votedFor == -1 || r.votedFor == req.CandidateId {
		DPrintf("[RequestVote] raft %d accept vote for %d | current term: %d | current state: %d | received term",
			r.me, req.CandidateId, r.currentTerm, r.role, req.Term)
		reply.Term = r.currentTerm
		reply.VoteGranted = true
		r.votedFor = req.CandidateId
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

	if req.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		DPrintf("[AppendEntries] raft %d reject append entries | current term: %d | current state: %d | args term: %d",
			r.me, r.currentTerm, r.role, req.Term)
		return
	}
	// 收到心跳包,更新选举超时时间
	r.resetTimerElection()

	// 如果当前的任期小于请求的任期 那么自己必然是跟随者
	if req.Term > r.currentTerm || r.role != Follower {
		r.BecomeFollower()
		r.currentTerm = req.Term
		DPrintf("[AppendEntries] raft %d update term or state | current term: %d | current state: %d | args term: %d",
			r.me, r.currentTerm, r.role, req.Term)
	}
	reply.Term = r.currentTerm
	reply.Success = true
}

// sendAppendEntries 客户端调用心跳/日志复制rpc
func (r *RaftNode) sendAppendEntries(server int, req *AppendEntriesReq, reply *AppendEntriesReply) bool {
	ok := r.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}
