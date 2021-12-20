package main

import (
	"bytes"
	"log"
	"reflect"
	"strings"
	"sync"
)

// RequestVote 投票RPC
type RequestVote struct {
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

// AppendEntries 复制日志以及心跳RPC
type AppendEntries struct {
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

type reqMsg struct {
	endName  interface{}
	svcMeth  string
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

type ClientEnd struct {
	endName interface{}
	ch      chan reqMsg
	done    chan struct{}
}

func (c *ClientEnd) Call(svcMeth string, args interface{}, reply interface{}) bool {
	req := reqMsg{}
	req.endName = c.endName
	req.svcMeth = svcMeth
	req.argsType = reflect.TypeOf(args)
	req.replyCh = make(chan replyMsg)

	select {
	case c.ch <- req:
	case <-c.done:
		return false
	}

	rep := <-req.replyCh
	if rep.ok {
		rb := bytes.NewBuffer(rep.reply)
		rd := NewDecoder(rb)
		if err := rd.Decode(reply); err != nil {
			log.Fatalf("ClientEnd.Call(); decode reply: %v\n", err)
		}
		return true
	} else {
		return false
	}
}

type Network struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool
	longReordering bool
	ends           map[interface{}]*ClientEnd
	enabled        map[interface{}]bool
	connections    map[interface{}]interface{}
	endCh          chan reqMsg
}

type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int
}

func MakeServer() *Server {
	rs := &Server{}
	rs.services = map[string]*Service{}
	return rs
}

func (s *Server) AddService(svc *Service) {
	s.mu.Lock()
	s.services[svc.name] = svc
	s.mu.Unlock()
}

func (s *Server) dispatch(req reqMsg) replyMsg {
	s.mu.Lock()

	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]
	methodName := req.svcMeth[dot+1:]

	service, ok := s.services[serviceName]

	s.mu.Unlock()

	if ok {
		return service.dispatch(methodName, req)
	} else {
		choices := []string{}
		for k, _ := range s.services {
			choices = append(choices, k)
		}
		return replyMsg{}
	}
}

type Service struct {
	name    string
	rcvr    reflect.Value
	typ     reflect.Type
	methods map[string]reflect.Method
}

func MakeService(rcvr interface{}) *Service {
	svc := &Service{}
	svc.typ = reflect.TypeOf(rcvr)
	svc.rcvr = reflect.ValueOf(rcvr)
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()

	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		mtype := method.Type
		mname := method.Name

		if method.PkgPath != "" ||
			mtype.NumIn() != 3 ||
			mtype.In(2).Kind() != reflect.Ptr ||
			mtype.NumOut() != 0 {

		} else {
			svc.methods[mname] = method
		}
	}
	return svc
}

func (svc *Service) dispatch(methname string, req reqMsg) replyMsg {
	if method, ok := svc.methods[methname]; ok {
		args := reflect.New(req.argsType)

		ab := bytes.NewBuffer(req.args)
		ad := NewDecoder(ab)
		ad.Decode(args.Interface())

		replyType := method.Type.In(2)
		replyType = replyType.Elem()
		replyv := reflect.New(replyType)

		function := method.Func
		function.Call([]reflect.Value{svc.rcvr, args.Elem(), replyv})

		rb := new(bytes.Buffer)
		re := NewEncoder(rb)
		re.EncodeValue(replyv)

		return replyMsg{true, rb.Bytes()}
	} else {
		choices := make([]string, 0)
		for k, _ := range svc.methods {
			choices = append(choices, k)
		}
		log.Fatalf("unknown method %v in %v; expecting one of %v\n",
			methname, req.svcMeth, choices)
		return replyMsg{}
	}
}
