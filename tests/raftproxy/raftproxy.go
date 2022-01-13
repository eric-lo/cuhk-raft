package raftproxy

/*
*	AUTHOR:
*			Shimin Wang <wyudun@gmail.com>
*
*	DESCRIPTION:
*			Runner for launching new raft instance
 */

import (
	"context"
	"cuhk/tests/raft"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type logger struct {
	RequestVoteSchema, AppendEntriesSchema map[string]int32
	expectedEvents                         []*raft.Event
	events                                 []*raft.Event
	newEvent                               chan raft.Event
}

type proxy struct {
	srv    raft.RaftNodeClient
	srvID  int32
	logger *logger
}

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)

var INFINITY_TIME = 100 * time.Minute

var keyFrenquencyMap = map[string]int{}
var keyFrenquencyMapLock = &sync.Mutex{}

/**
* Proxy validates the requests going into a RaftNode and the responses coming out of it.  * It logs errors that occurs during a test.
 */
func NewProxy(nodePort, myPort int, srvId int) (raft.RaftNodeServer, error) {
	p := new(proxy)
	p.logger = &logger{
		expectedEvents: []*raft.Event{},
		events:         []*raft.Event{},
		newEvent:       make(chan raft.Event, 1000),
	}
	p.srvID = int32(srvId)

	// Start server
	//l, err := net.Listen("tcp", fmt.Sprintf(":%d", myPort))
	//if err != nil {
	//	LOGE.Println("Failed to listen:", err)
	//	return nil, err
	//}

	// Create RPC connection to raft node.
	//srv, err := rpc.DialHTTP("tcp", fmt.Sprintf("localhost:%d", nodePort))
	//if err != nil {
	//	LOGE.Println("Failed to dial node %d", nodePort)
	//	return nil, err
	//}
	//p.srv = srv
	//
	//// register RPC
	//rpc.RegisterName("RaftNode", raftproxyrpc.Wrap(p))
	//rpc.HandleHTTP()
	//go http.Serve(l, nil)

	l, err := net.Listen("tcp", fmt.Sprintf(":%d", myPort))

	if err != nil {
		log.Print("Fail to listen port", err)
		os.Exit(1)
	}

	s := grpc.NewServer()
	raft.RegisterRaftNodeServer(s, p)

	go s.Serve(l)

	conn, err := grpc.Dial(fmt.Sprintf("localhost:%d", nodePort), grpc.WithInsecure())
	if err != nil {
		log.Print("Proxy fail to connect self raft node", err)
		os.Exit(1)
	}
	//defer conn.Close()
	p.srv = raft.NewRaftNodeClient(conn)

	fmt.Printf("node %d Proxy started\n", srvId)
	return p, nil
}

func (p *proxy) getExpectedEventsForThisNode(expectedEvents []*raft.Event) []*raft.Event {
	res := []*raft.Event{}
	for _, event := range expectedEvents {
		if event.To == -1 {
			event.To = p.srvID
		}
		if event.From == -1 {
			event.From = p.srvID
		}

		if !event.IsResponse {
			if event.To == p.srvID && event.From != event.To {
				res = append(res, event)
			}
		} else {
			if event.From == p.srvID && event.From != event.To {
				res = append(res, event)
			}
		}
	}
	return res
}

func (p *proxy) Reset(args *raft.CheckEventsArgs) {
	p.logger = &logger{
		RequestVoteSchema:   args.RequestVoteSchema,
		AppendEntriesSchema: args.AppendEntriesSchema,
		expectedEvents:      p.getExpectedEventsForThisNode(args.ExpectedEvents),
		events:              []*raft.Event{},
		newEvent:            make(chan raft.Event, 1000),
	}
}

func getEventsString(events []*raft.Event) string {
	res := ""
	for _, event := range events {
		res += eventToString(event) + "\n"
	}
	return res
}

func (l *logger) checkLogger() (bool, string) {
	sortedEvents := []string{}
	sortedExpectedEvents := []string{}

	for i := 0; i < len(l.events); i++ {
		sortedEvents = append(sortedEvents, eventToString(l.events[i]))
		sortedExpectedEvents = append(sortedExpectedEvents, eventToString(l.expectedEvents[i]))
	}

	sort.Strings(sortedEvents)
	sort.Strings(sortedExpectedEvents)

	if strings.Join(sortedEvents, "") != strings.Join(sortedExpectedEvents, "") {
		return false, fmt.Sprintf("\nExpected:\n%sBut get:\n%s",
			getEventsString(l.expectedEvents),
			getEventsString(l.events))
	}

	return true, ""
}

func (p *proxy) getDelayDuration(from, to, term int, isRequestVote, isReply bool) (bool, time.Duration) {
	newlogger := p.logger
	var key string
	var phase string
	if isRequestVote {
		phase = "rv "
	} else {
		phase = "ae "
	}

	if isReply {
		key = fmt.Sprintf("t%d:%d<-%d", term, to, from)
	} else {
		key = fmt.Sprintf("t%d:%d->%d", term, from, to)
	}

	phasekey := phase + key

	keyFrenquencyMapLock.Lock()
	if _, exist := keyFrenquencyMap[phasekey]; !exist {
		keyFrenquencyMap[phasekey] = 1
	} else {
		keyFrenquencyMap[phasekey]++
	}
	key = fmt.Sprintf("%s #%d", key, keyFrenquencyMap[phasekey])
	keyFrenquencyMapLock.Unlock()

	var delay int32 = 0
	if isRequestVote {
		if v, exist := newlogger.RequestVoteSchema[key]; exist {
			delay = v
		}
	} else {
		if v, exist := newlogger.AppendEntriesSchema[key]; exist {
			delay = v
		}
	}

	if delay < 0 {
		return true, time.Millisecond
	} else {
		return false, time.Duration(delay) * time.Millisecond
	}
}

func (p *proxy) Propose(ctx context.Context, args *raft.ProposeArgs) (*raft.ProposeReply, error) {
	return p.srv.Propose(context.Background(), args)
}

//func (p *proxy) GetValue(args *raftproxyrpc.GetValueArgs, reply *raftproxyrpc.GetValueReply) error {
//	err := p.srv.Call("RaftNode.GetValue", args, reply)
//	return err
//}
func (p *proxy) GetValue(ctx context.Context, args *raft.GetValueArgs) (*raft.GetValueReply, error) {
	return p.srv.GetValue(context.Background(), args)
}

//func (p *proxy) SetElectionTimeout(args *raftproxyrpc.SetElectionTimeoutArgs, reply *raftproxyrpc.SetElectionTimeoutReply) error {
//	err := p.srv.Call("RaftNode.SetElectionTimeout", args, reply)
//	return err
//}

func (p *proxy) SetElectionTimeout(ctx context.Context, args *raft.SetElectionTimeoutArgs) (*raft.SetElectionTimeoutReply, error) {
	return p.srv.SetElectionTimeout(context.Background(), args)
}

//func (p *proxy) SetHeartBeatInterval(args *raftproxyrpc.SetHeartBeatIntervalArgs, reply *raftproxyrpc.SetHeartBeatIntervalReply) error {
//	err := p.srv.Call("RaftNode.SetHeartBeatInterval", args, reply)
//	return err
//}

func (p *proxy) SetHeartBeatInterval(ctx context.Context, args *raft.SetHeartBeatIntervalArgs) (*raft.SetHeartBeatIntervalReply, error) {
	return p.srv.SetHeartBeatInterval(context.Background(), args)
}

//func (p *proxy) RequestVote(args *raftproxyrpc.RequestVoteArgs, reply *raftproxyrpc.RequestVoteReply) error {
//	dropped, delay := p.getDelayDuration(args.From, args.To, args.Term, true, false)
//	if dropped {
//		fmt.Printf("node %d: dropped RequestVote from %d\n", args.To, args.From)
//		time.Sleep(INFINITY_TIME)
//		return nil
//	}
//	time.Sleep(delay)
//
//	requestVoteEvent := raftproxyrpc.Event{
//		From:         args.From,
//		To:           args.To,
//		Term:         args.Term,
//		Msg:          raftproxyrpc.RequestVote,
//		CandidateId:  args.From,
//		LastLogIndex: args.LastLogIndex,
//		LastLogTerm:  args.LastLogTerm,
//		IsResponse:   false,
//	}
//	p.logger.newEvent <- requestVoteEvent
//
//	err := p.srv.Call("RaftNode.RequestVote", args, reply)
//
//	dropped, delay = p.getDelayDuration(reply.From, reply.To, reply.Term, true, true)
//	if dropped {
//		fmt.Printf("node %d: dropped RequestVoteResponse to %d\n", reply.From, reply.To)
//		time.Sleep(INFINITY_TIME)
//		return nil
//	}
//	time.Sleep(delay)
//
//	requestVoteResponseEvent := raftproxyrpc.Event{
//		From:        reply.From,
//		To:          reply.To,
//		Term:        reply.Term,
//		Msg:         raftproxyrpc.RequestVote,
//		VoteGranted: reply.VoteGranted,
//		IsResponse:  true,
//	}
//
//	p.logger.newEvent <- requestVoteResponseEvent
//	return err
//}

func (p *proxy) RequestVote(ctx context.Context, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	dropped, delay := p.getDelayDuration(int(args.From), int(args.To), int(args.Term), true, false)
	if dropped {
		fmt.Printf("node %d: dropped RequestVote from %d\n", args.To, args.From)
		time.Sleep(INFINITY_TIME)
		return nil, nil
	}
	time.Sleep(delay)

	requestVoteEvent := raft.Event{
		From:         args.From,
		To:           args.To,
		Term:         args.Term,
		Msg:          raft.MsgName_RequestVote,
		CandidateId:  args.From,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
		IsResponse:   false,
	}
	p.logger.newEvent <- requestVoteEvent

	reply, err := p.srv.RequestVote(context.Background(), args)

	dropped, delay = p.getDelayDuration(int(reply.From), int(reply.To), int(reply.Term), true, true)
	if dropped {
		fmt.Printf("node %d: dropped RequestVoteResponse to %d\n", reply.From, reply.To)
		time.Sleep(INFINITY_TIME)
		return nil, nil
	}
	time.Sleep(delay)

	requestVoteResponseEvent := raft.Event{
		From:        reply.From,
		To:          reply.To,
		Term:        reply.Term,
		Msg:         raft.MsgName_RequestVote,
		VoteGranted: reply.VoteGranted,
		IsResponse:  true,
	}

	p.logger.newEvent <- requestVoteResponseEvent
	return reply, err
}

//func (p *proxy) AppendEntries(args *raftproxyrpc.AppendEntriesArgs, reply *raftproxyrpc.AppendEntriesReply) error {
//	dropped, delay := p.getDelayDuration(args.From, args.To, args.Term, false, false)
//	if dropped {
//		fmt.Printf("node %d: dropped AppendEntries from %d\n", args.To, args.From)
//		time.Sleep(INFINITY_TIME)
//		return nil
//	}
//	time.Sleep(delay)
//
//	appendEntriesEvent := raftproxyrpc.Event{
//		From:         args.From,
//		To:           args.To,
//		Term:         args.Term,
//		Msg:          raftproxyrpc.AppendEntries,
//		LeaderId:     args.LeaderId,
//		PrevLogIndex: args.PrevLogIndex,
//		PrevLogTerm:  args.PrevLogTerm,
//		Entries:      args.Entries,
//		LeaderCommit: args.LeaderCommit,
//		IsResponse:   false,
//	}
//	p.logger.newEvent <- appendEntriesEvent
//
//	err := p.srv.Call("RaftNode.AppendEntries", args, reply)
//
//	dropped, delay = p.getDelayDuration(reply.From, reply.To, reply.Term, false, true)
//	if dropped {
//		fmt.Printf("node %d: dropped AppendEntriesResponse to %d\n", reply.From, reply.To)
//		time.Sleep(INFINITY_TIME)
//		return nil
//	}
//	time.Sleep(delay)
//
//	appendEntriesResponseEvent := raftproxyrpc.Event{
//		From:       reply.From,
//		To:         reply.To,
//		Term:       reply.Term,
//		Msg:        raftproxyrpc.AppendEntries,
//		Success:    reply.Success,
//		MatchIndex: reply.MatchIndex,
//		IsResponse: true,
//	}
//
//	p.logger.newEvent <- appendEntriesResponseEvent
//	return err
//}

func (p *proxy) AppendEntries(ctx context.Context, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	dropped, delay := p.getDelayDuration(int(args.From), int(args.To), int(args.Term), false, false)
	if dropped {
		fmt.Printf("node %d: dropped AppendEntries from %d\n", args.To, args.From)
		time.Sleep(INFINITY_TIME)
		return nil, nil
	}
	time.Sleep(delay)

	appendEntriesEvent := raft.Event{
		From:         args.From,
		To:           args.To,
		Term:         args.Term,
		Msg:          raft.MsgName_AppendEntries,
		LeaderId:     args.LeaderId,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      args.Entries,
		LeaderCommit: args.LeaderCommit,
		IsResponse:   false,
	}
	p.logger.newEvent <- appendEntriesEvent

	reply, err := p.srv.AppendEntries(context.Background(), args)

	dropped, delay = p.getDelayDuration(int(reply.From), int(reply.To), int(reply.Term), false, true)
	if dropped {
		fmt.Printf("node %d: dropped AppendEntriesResponse to %d\n", reply.From, reply.To)
		time.Sleep(INFINITY_TIME)
		return nil, nil
	}
	time.Sleep(delay)

	appendEntriesResponseEvent := raft.Event{
		From:       reply.From,
		To:         reply.To,
		Term:       reply.Term,
		Msg:        raft.MsgName_AppendEntries,
		Success:    reply.Success,
		MatchIndex: reply.MatchIndex,
		IsResponse: true,
	}

	p.logger.newEvent <- appendEntriesResponseEvent
	return reply, err
}

//func (p *proxy) CheckEvents(args *raftproxyrpc.CheckEventsArgs, reply *raftproxyrpc.CheckEventsReply) error {
//	p.Reset(args)
//	newlogger := p.logger
//
//	if len(newlogger.expectedEvents) == 0 {
//		reply.Success = true
//		return nil
//	}
//
//	for {
//		select {
//		case newEvent := <-newlogger.newEvent:
//
//			// If you want to see the detail log trace for each test, please uncomment this line
//			//fmt.Printf("%s\n",  newEvent.ToString())
//
//			newlogger.events = append(newlogger.events, newEvent)
//			if len(newlogger.events) == len(newlogger.expectedEvents) {
//				reply.Success, reply.ErrMsg = newlogger.checkLogger()
//				return nil
//			}
//		}
//	}
//
//	return nil
//}
func (p *proxy) CheckEvents(ctx context.Context, args *raft.CheckEventsArgs) (*raft.CheckEventsReply, error) {
	p.Reset(args)
	newlogger := p.logger

	reply := new(raft.CheckEventsReply)

	if len(newlogger.expectedEvents) == 0 {
		reply.Success = true
		return reply, nil
	}

	for {
		select {
		case newEvent := <-newlogger.newEvent:

			// If you want to see the detail log trace for each test, please uncomment this line
			fmt.Printf("%s\n", eventToString(&newEvent))

			newlogger.events = append(newlogger.events, &newEvent)
			if len(newlogger.events) == len(newlogger.expectedEvents) {
				reply.Success, reply.ErrMsg = newlogger.checkLogger()
				return reply, nil
			}
		}
	}
}

func eventToString(e *raft.Event) string {
	var res = ""
	if e.IsResponse {
		res += fmt.Sprintf("node %d -> %d: ", e.From, e.To)
	} else {
		res += fmt.Sprintf("node %d <- %d: ", e.To, e.From)
	}
	switch e.Msg {
	case raft.MsgName_RequestVote:
		if e.IsResponse {
			if e.VoteGranted {
				res += fmt.Sprintf("granted, term: %d", e.Term)
			} else {
				res += fmt.Sprintf("reject, term: %d", e.Term)
			}
		} else {
			res += fmt.Sprintf("RequestVote -- term: %d, candidateId: %d, lastLogIdx: %d, lastLogTerm: %d",
				e.Term, e.CandidateId, e.LastLogIndex, e.LastLogTerm)
		}
	case raft.MsgName_AppendEntries:
		if e.IsResponse {
			if e.Success {
				res += fmt.Sprintf("success, term: %d, matchIdx: %d", e.Term, e.MatchIndex)
			} else {
				res += fmt.Sprintf("fail, term: %d, matchIdx: %d", e.Term, e.MatchIndex)
			}
		} else {
			res += fmt.Sprintf("AppendEntries -- term: %d, leaderId: %d, prevLogIdx: %d, prevLogTerm: %d, "+
				"entries: %s, leaderCommit: %d", e.Term, e.LeaderId, e.PrevLogIndex, e.PrevLogTerm,
				EntriesToString(e.Entries), e.LeaderCommit)
		}
	}
	return res
}

func EntriesToString(l []*raft.LogEntry) string {
	s := []string{}
	for _, le := range l {
		s = append(s, logEntryToString(le))
	}
	return fmt.Sprintf("[%s]", strings.Join(s, ","))
}

func logEntryToString(l *raft.LogEntry) string {
	var op string
	switch l.Op {
	case raft.Operation_Put:
		op = "put"
	case raft.Operation_Delete:
		op = "del"
	}
	return fmt.Sprintf("t%d:%s<%s,%v>", l.Term, op, l.Key, l.Value)
}
