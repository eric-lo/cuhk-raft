package main

/*
*	AUTHOR:
*			Shimin Wang <wyudun@gmail.com>
*
*	DESCRIPTION:
*			Tests for log replication
 */

import (
	crand "crypto/rand"
	"cuhk/tests/raft"
	"math"
	"math/big"
	"math/rand"
	"time"
)

// all the tests here will start from electing node 0 as the leader
var electionNode0Events = []*raft.Event{
	// 0 requestVote to all
	{Term: 1, From: 0, To: -1, Msg: raft.MsgName_RequestVote,
		CandidateId: 0, LastLogIndex: 0, LastLogTerm: 0, IsResponse: false},
	// all vote for 0
	{Term: 1, From: -1, To: 0, Msg: raft.MsgName_RequestVote,
		VoteGranted: true, IsResponse: true},
	// 0 appendEntry for all
	{Term: 1, From: 0, To: -1, Msg: raft.MsgName_AppendEntries,
		LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{},
		LeaderCommit: 0, IsResponse: false},
	// all reply success
	{Term: 1, From: -1, To: 0, Msg: raft.MsgName_AppendEntries,
		Success: true, MatchIndex: 0, IsResponse: true},
}

/**
* Tests simply put 1 key
 */
func testOneSimplePut(doneChan chan bool) {
	key := "test"
	randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
	value := rand.Int31()

	// following is the delayed schema for each message for RequestVote and AppendEntries
	requestVoteSchema := map[string]int32{}
	appendEntriesSchema := map[string]int32{}

	le := raft.LogEntry{
		Term:  1,
		Op:    raft.Operation_Put,
		Key:   key,
		Value: value,
	}
	// following is the expected global events stream for this test
	events := append(electionNode0Events, []*raft.Event{
		// 0 appendEntry for all
		{Term: 1, From: 0, To: -1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le},
			LeaderCommit: 0, IsResponse: false},
		// all reply success
		{Term: 1, From: -1, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 1, IsResponse: true},
		// 0 appendEntry for all
		{Term: 1, From: 0, To: -1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []*raft.LogEntry{},
			LeaderCommit: 1, IsResponse: false},
		// all reply success
		{Term: 1, From: -1, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 1, IsResponse: true},
	}...)

	roundChan := make(chan error)
	pt.RunAllRaftRound(requestVoteSchema, appendEntriesSchema, events, roundChan)
	time.Sleep(2 * time.Second)

	pt.SetAllElectTimeout([]int{1000, 2000, 2000, 2000, 2000})
	go pt.SetHeartBeatInterval(1000, 0)

	time.Sleep(1500 * time.Millisecond)

	proposeRes := make(chan ProposeResult, pt.numNodes)
	pt.ProposeAll(raft.Operation_Put, key, value, proposeRes)

	for i := 0; i < pt.numNodes; i++ {
		err := <-roundChan
		if err != nil {
			printFailErr("testOneSimplePut", err)
			doneChan <- false
			return
		}
	}

	expectedProposeRes := []ProposeResult{
		{reply: &raft.ProposeReply{Status: raft.Status_OK}},
		{reply: &raft.ProposeReply{CurrentLeader: 0, Status: raft.Status_WrongNode}},
		{reply: &raft.ProposeReply{CurrentLeader: 0, Status: raft.Status_WrongNode}},
		{reply: &raft.ProposeReply{CurrentLeader: 0, Status: raft.Status_WrongNode}},
		{reply: &raft.ProposeReply{CurrentLeader: 0, Status: raft.Status_WrongNode}},
	}
	for i := 0; i < pt.numNodes; i++ {
		res := <-proposeRes
		if !checkProposeRes(res, expectedProposeRes[res.nodeId], res.nodeId) {
			doneChan <- false
			return
		}
	}

	if !checkGetValueAll(key, []raft.Status{
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
	}, []int32{value, value, value, value, value}) {
		doneChan <- false
		return
	}

	doneChan <- true
}

/*
* Tests put 1 key and then update it
 */
func testOneSimpleUpdate(doneChan chan bool) {
	key := "test"
	randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
	value1 := rand.Int31()
	value2 := rand.Int31()

	// following is the delayed schema for each message for RequestVote and AppendEntries
	requestVoteSchema := map[string]int32{}
	appendEntriesSchema := map[string]int32{
		"t1:0->3 #2": -1,
		"t1:0->4 #2": -1,
	}

	le1 := raft.LogEntry{
		Term:  1,
		Op:    raft.Operation_Put,
		Key:   key,
		Value: value1,
	}
	le2 := raft.LogEntry{
		Term:  1,
		Op:    raft.Operation_Put,
		Key:   key,
		Value: value2,
	}

	// following is the expected global events stream for this test
	events := append(electionNode0Events, []*raft.Event{
		// 0 appendEntry to 1, 2; appendEntry to 3,4 dropped
		{Term: 1, From: 0, To: 1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1},
			LeaderCommit: 0, IsResponse: false},
		{Term: 1, From: 0, To: 2, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1},
			LeaderCommit: 0, IsResponse: false},
		// 1, 2 reply success
		{Term: 1, From: 1, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 1, IsResponse: true},
		{Term: 1, From: 2, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 1, IsResponse: true},

		// 0 response to propose <k, v1> and then new propose <k, v2> comes in

		// 0 appendEntry to all
		{Term: 1, From: 0, To: 1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []*raft.LogEntry{&le2},
			LeaderCommit: 1, IsResponse: false},
		{Term: 1, From: 0, To: 2, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []*raft.LogEntry{&le2},
			LeaderCommit: 1, IsResponse: false},
		{Term: 1, From: 0, To: 3, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1, &le2},
			LeaderCommit: 1, IsResponse: false},
		{Term: 1, From: 0, To: 4, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1, &le2},
			LeaderCommit: 1, IsResponse: false},

		// all reply success
		{Term: 1, From: 1, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},
		{Term: 1, From: 2, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},
		{Term: 1, From: 3, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},
		{Term: 1, From: 4, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},

		// 0 appendEntry for all
		{Term: 1, From: 0, To: -1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []*raft.LogEntry{},
			LeaderCommit: 2, IsResponse: false},
		// all reply success
		{Term: 1, From: -1, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},
	}...)

	roundChan := make(chan error)
	pt.RunAllRaftRound(requestVoteSchema, appendEntriesSchema, events, roundChan)
	time.Sleep(2 * time.Second)

	pt.SetAllElectTimeout([]int{1000, 4000, 4000, 4000, 4000})
	go pt.SetHeartBeatInterval(1000, 0)

	time.Sleep(1500 * time.Millisecond)

	// propose new <k, v1>
	proposeRes1 := make(chan ProposeResult)
	go pt.Propose(raft.Operation_Put, key, value1, 0, proposeRes1)
	res1 := <-proposeRes1
	if !checkProposeRes(res1, ProposeResult{
		reply: &raft.ProposeReply{Status: raft.Status_OK},
	}, 0) {
		doneChan <- false
		return
	}

	if !checkGetValueAll(key, []raft.Status{
		raft.Status_KeyFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
	}, []int32{value1, 0, 0, 0, 0}) {
		doneChan <- false
		return
	}

	// update <k, v2>
	proposeRes2 := make(chan ProposeResult)
	go pt.Propose(raft.Operation_Put, key, value2, 0, proposeRes2)

	for i := 0; i < pt.numNodes; i++ {
		err := <-roundChan
		if err != nil {
			printFailErr("testOneSimpleUpdate", err)
			doneChan <- false
			return
		}
	}

	res2 := <-proposeRes2
	if !checkProposeRes(res2, ProposeResult{
		reply: &raft.ProposeReply{Status: raft.Status_OK},
	}, 0) {
		doneChan <- false
		return
	}
	if !checkGetValueAll(key, []raft.Status{
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
	}, []int32{value2, value2, value2, value2, value2}) {
		doneChan <- false
		return
	}

	doneChan <- true
}

/*
* Tests put 1 key and then delete it
 */
func testOneSimpleDelete(doneChan chan bool) {
	key := "test"
	randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
	value1 := rand.Int31()

	// following is the delayed schema for each message for RequestVote and AppendEntries
	requestVoteSchema := map[string]int32{}
	appendEntriesSchema := map[string]int32{
		"t1:0->1 #2": -1,
		"t1:0->3 #2": -1,
		"t1:0->4 #2": -1,

		"t1:0->1 #3": -1,
		"t1:0->2 #3": -1,
		"t1:0->4 #3": -1,
	}

	le1 := raft.LogEntry{
		Term:  1,
		Op:    raft.Operation_Put,
		Key:   key,
		Value: value1,
	}
	le2 := raft.LogEntry{
		Term: 1,
		Op:   raft.Operation_Delete,
		Key:  key,
	}

	// following is the expected global events stream for this test
	events := append(electionNode0Events, []*raft.Event{
		// 0 appendEntry to 2; appendEntry to 1,3,4 dropped
		{Term: 1, From: 0, To: 2, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1},
			LeaderCommit: 0, IsResponse: false},
		// 1, 2 reply success
		{Term: 1, From: 2, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 1, IsResponse: true},

		// new propose del<k> comes in

		// 0 appendEntry to 3; appendEntry to 1,2,4 dropped
		{Term: 1, From: 0, To: 3, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1, &le2},
			LeaderCommit: 0, IsResponse: false},
		// 1,3 reply success
		{Term: 1, From: 3, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},

		// 0 commit put<k, v1>

		// 0 appendEntry for all
		{Term: 1, From: 0, To: 1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1, &le2},
			LeaderCommit: 1, IsResponse: false},
		{Term: 1, From: 0, To: 2, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 1, PrevLogTerm: 1, Entries: []*raft.LogEntry{&le2},
			LeaderCommit: 1, IsResponse: false},
		{Term: 1, From: 0, To: 3, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []*raft.LogEntry{},
			LeaderCommit: 1, IsResponse: false},
		{Term: 1, From: 0, To: 4, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1, &le2},
			LeaderCommit: 1, IsResponse: false},
		// all reply success
		{Term: 1, From: -1, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},

		// 0 commit del<k>
		// 0 appendEntry for all
		{Term: 1, From: 0, To: -1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []*raft.LogEntry{},
			LeaderCommit: 2, IsResponse: false},
		// all reply success
		{Term: 1, From: -1, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},
		// all commit del<k>
	}...)

	roundChan := make(chan error)
	pt.RunAllRaftRound(requestVoteSchema, appendEntriesSchema, events, roundChan)
	time.Sleep(2 * time.Second)

	pt.SetAllElectTimeout([]int{1000, 4000, 4000, 4000, 4000})
	go pt.SetHeartBeatInterval(1000, 0)

	time.Sleep(1500 * time.Millisecond)

	// propose new <k, v1>
	proposeRes1 := make(chan ProposeResult)
	go pt.Propose(raft.Operation_Put, key, value1, 0, proposeRes1)

	time.Sleep(1000 * time.Millisecond)

	// delete <k>
	proposeRes2 := make(chan ProposeResult)
	go pt.Propose(raft.Operation_Delete, key, 0, 0, proposeRes2)

	time.Sleep(1000 * time.Millisecond)
	if !checkGetValueAll(key, []raft.Status{
		raft.Status_KeyFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
	}, []int32{value1, 0, 0, 0, 0}) {
		doneChan <- false
		return
	}

	time.Sleep(1000 * time.Millisecond)
	if !checkGetValueAll(key, []raft.Status{
		raft.Status_KeyNotFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
	}, []int32{0, value1, value1, value1, value1}) {
		doneChan <- false
		return
	}

	for i := 0; i < pt.numNodes; i++ {
		err := <-roundChan
		if err != nil {
			printFailErr("testOneSimpleDelete", err)
			doneChan <- false
			return
		}
	}

	res1 := <-proposeRes1
	if !checkProposeRes(res1, ProposeResult{
		reply: &raft.ProposeReply{Status: raft.Status_OK},
	}, 0) {
		doneChan <- false
		return
	}

	res2 := <-proposeRes2
	if !checkProposeRes(res2, ProposeResult{
		reply: &raft.ProposeReply{Status: raft.Status_OK},
	}, 0) {
		doneChan <- false
		return
	}

	if !checkGetValueAll(key, []raft.Status{
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
	}, []int32{0, 0, 0, 0, 0}) {
		doneChan <- false
		return
	}

	doneChan <- true
}

/**
* Tests simply put 1 key
 */
func testDeleteNonExistKey(doneChan chan bool) {
	key := "test"
	key2 := "test2"
	randint, _ := crand.Int(crand.Reader, big.NewInt(math.MaxInt64))
	rand.Seed(randint.Int64())
	value1 := rand.Int31()

	// following is the delayed schema for each message for RequestVote and AppendEntries
	requestVoteSchema := map[string]int32{}
	appendEntriesSchema := map[string]int32{
		"t1:0<-1 #2": -1,
		"t1:0->2 #2": -1,
		"t1:0->3 #2": -1,
		"t1:0<-4 #2": -1,

		"t1:0<-1 #3": -1,
		"t1:0->2 #3": -1,
		"t1:0<-3 #2": -1,
		"t1:0->4 #3": -1,
	}

	le1 := raft.LogEntry{
		Term:  1,
		Op:    raft.Operation_Put,
		Key:   key,
		Value: value1,
	}
	le2 := raft.LogEntry{
		Term: 1,
		Op:   raft.Operation_Delete,
		Key:  key2,
	}

	// following is the expected global events stream for this test
	events := append(electionNode0Events, []*raft.Event{
		// 0 appendEntry to 1,4; appendEntry to 2,3 dropped
		{Term: 1, From: 0, To: 1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1},
			LeaderCommit: 0, IsResponse: false},
		{Term: 1, From: 0, To: 4, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1},
			LeaderCommit: 0, IsResponse: false},
		// 1,4 reply dropped

		// new propose del<k2> comes in

		// 0 appendEntry to 1,3; appendEntry to 2,4 dropped
		{Term: 1, From: 0, To: 1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1, &le2},
			LeaderCommit: 0, IsResponse: false},
		{Term: 1, From: 0, To: 3, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1, &le2},
			LeaderCommit: 0, IsResponse: false},
		// 1,3 reply dropped

		// 0 appendEntry for all
		{Term: 1, From: 0, To: -1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 0, PrevLogTerm: 0, Entries: []*raft.LogEntry{&le1, &le2},
			LeaderCommit: 0, IsResponse: false},
		// all reply success
		{Term: 1, From: -1, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},

		// 0 commit put<k,v1> and del<k>
		// 0 appendEntry for all
		{Term: 1, From: 0, To: -1, Msg: raft.MsgName_AppendEntries,
			LeaderId: 0, PrevLogIndex: 2, PrevLogTerm: 1, Entries: []*raft.LogEntry{},
			LeaderCommit: 2, IsResponse: false},
		// all reply success
		{Term: 1, From: -1, To: 0, Msg: raft.MsgName_AppendEntries,
			Success: true, MatchIndex: 2, IsResponse: true},
		// all commit del<k>
	}...)

	roundChan := make(chan error)
	pt.RunAllRaftRound(requestVoteSchema, appendEntriesSchema, events, roundChan)
	time.Sleep(2 * time.Second)

	pt.SetAllElectTimeout([]int{1000, 4000, 4000, 4000, 4000})
	go pt.SetHeartBeatInterval(1000, 0)

	time.Sleep(1500 * time.Millisecond)

	// propose new <k, v1>
	proposeRes1 := make(chan ProposeResult)
	go pt.Propose(raft.Operation_Put, key, value1, 0, proposeRes1)

	time.Sleep(1000 * time.Millisecond)

	// delete <k2>
	proposeRes2 := make(chan ProposeResult)
	go pt.Propose(raft.Operation_Delete, key2, 0, 0, proposeRes2)

	time.Sleep(1000 * time.Millisecond)
	if !checkGetValueAll(key, []raft.Status{
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
	}, []int32{0, 0, 0, 0, 0}) {
		doneChan <- false
		return
	}

	time.Sleep(1000 * time.Millisecond)

	res1 := <-proposeRes1
	if !checkProposeRes(res1, ProposeResult{
		reply: &raft.ProposeReply{Status: raft.Status_OK},
	}, 0) {
		doneChan <- false
		return
	}

	res2 := <-proposeRes2
	if !checkProposeRes(res2, ProposeResult{
		reply: &raft.ProposeReply{Status: raft.Status_KeyNotFound},
	}, 0) {
		doneChan <- false
		return
	}

	if !checkGetValueAll(key, []raft.Status{
		raft.Status_KeyFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
		raft.Status_KeyNotFound,
	}, []int32{value1, 0, 0, 0, 0}) {
		doneChan <- false
		return
	}

	for i := 0; i < pt.numNodes; i++ {
		err := <-roundChan
		if err != nil {
			printFailErr("testDeleteNonExistKey", err)
			doneChan <- false
			return
		}
	}

	if !checkGetValueAll(key, []raft.Status{
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
		raft.Status_KeyFound,
	}, []int32{value1, value1, value1, value1, value1}) {
		doneChan <- false
		return
	}

	doneChan <- true
}
