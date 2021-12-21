package utils

import (
	"bytes"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

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
	servers        map[interface{}]*Server
	connections    map[interface{}]interface{}
	endCh          chan reqMsg
	done           chan struct{}
	count          int32
	bytes          int64
}

func (rn *Network) MakeEnd(endName interface{}) *ClientEnd {
	rn.mu.Lock()

	if _, ok := rn.ends[endName]; ok {
		log.Fatalf("MakeEnd: %v already exists\n", endName)
	}

	e := &ClientEnd{}
	e.endName = endName
	e.ch = rn.endCh
	e.done = rn.done
	rn.ends[endName] = e
	rn.enabled[endName] = false
	rn.connections[endName] = nil

	return e
}

func (rn *Network) AddServer(serverName interface{}, server *Server) {
	rn.mu.Lock()
	rn.servers[serverName] = server
	rn.mu.Unlock()
}

func (rn *Network) DeleteServer(serverName interface{}) {
	rn.mu.Lock()
	rn.servers[serverName] = nil
	rn.mu.Unlock()
}

func (rn *Network) Connect(endName interface{}, serverName interface{}) {
	rn.mu.Lock()
	rn.connections[endName] = serverName
	rn.mu.Unlock()
}

func (rn *Network) Enable(endName interface{}, enabled bool) {
	rn.mu.Lock()
	rn.enabled[endName] = enabled
	rn.mu.Unlock()
}

func (rn *Network) Cleanup() {
	close(rn.done)
}

func (rn *Network) Reliable(yes bool) {
	rn.mu.Lock()
	rn.reliable = yes
	rn.mu.Unlock()
}

func (rn *Network) LongReordering(yes bool) {
	rn.mu.Lock()
	rn.longReordering = yes
	rn.mu.Unlock()
}

func (rn *Network) LongDelays(yes bool) {
	rn.mu.Lock()
	rn.longReordering = yes
	rn.mu.Unlock()
}

func (rn *Network) readEndNameInfo(endName interface{}) (enabled, reliable, longReordering bool, server *Server, serverName interface{}) {
	rn.mu.Lock()
	enabled = rn.enabled[endName]
	serverName = rn.connections[endName]
	if serverName != nil {
		server = rn.servers[serverName]
	}
	reliable = rn.reliable
	longReordering = rn.longReordering
	rn.mu.Unlock()
	return
}

func (rn *Network) isServerDead(endName interface{}, serverName interface{}, server *Server) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if !rn.enabled[endName] || rn.servers[serverName] != server {
		return true
	}
	return false
}

func (rn *Network) processReq(req reqMsg) {
	enabled, reliable, longReordering, server, servername := rn.readEndNameInfo(req.endName)

	if enabled && servername != nil && server != nil {
		if !reliable {
			ms := rand.Int() % 27
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else if !reliable && rand.Int()%1000 < 100 {
			req.replyCh <- replyMsg{false, nil}
			return
		}

		ech := make(chan replyMsg)
		// execute the request
		go func() {
			r := server.dispatch(req)
			ech <- r
		}()

		var reply replyMsg
		replyOK := false
		serverDead := false

		for !replyOK && !serverDead {
			select {
			case reply = <-ech:
				replyOK = true
			case <-time.After(100 * time.Millisecond):
				serverDead = rn.isServerDead(req.endName, servername, server)
				if serverDead {
					go func() {
						<-ech
					}()
				}
			}
		}

		serverDead = rn.isServerDead(req.endName, servername, server)

		if !replyOK || serverDead {
			req.replyCh <- replyMsg{false, nil}
		} else if !reliable && rand.Int()%1000 < 100 {
			req.replyCh <- replyMsg{false, nil}
		} else if longReordering && rand.Intn(900) < 600 {
			ms := 200 + rand.Intn(1+rand.Intn(2000))
			time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
				atomic.AddInt64(&rn.bytes, int64(len(reply.reply)))
				req.replyCh <- reply
			})
		} else {
			atomic.AddInt64(&rn.bytes, int64(len(reply.reply)))
			req.replyCh <- reply
		}
	} else {
		ms := 0
		if rn.longDelays {
			ms = rand.Int() % 7000
		} else {
			ms = rand.Int() % 100
		}
		time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			req.replyCh <- replyMsg{false, nil}
		})
	}
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
		for k := range s.services {
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
