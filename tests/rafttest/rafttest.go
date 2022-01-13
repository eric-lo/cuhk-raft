package main

/*
*							A RAFT TESTING FRAMEWORK
*
*	AUTHOR:
*			Shimin Wang <wyudun@gmail.com>
*
*	DESCRIPTION:
*	This is a testing framework for Raft <https://raft.github.io>.
*
*	The basic idea of this test framework is to set up a proxy sever upon each raft node, and do some
*	bookkeeping in the proxy server to check if all the message are passed as expected. To ensure the
*	message are sent in expected order, we need to predefined the delay of each message, I store this
*	information in requestVoteSchema and appendEntriesSchema. Both of them are map from a message
*	description to an integer value indicating the delay of this message. The format of the message
*	description is:
*
*		t<term>:<node1><messagedirection><node2> #<order number of this in this term>
*
*	If this message is a response message, the <messagedirection> will be <-, otherwise, it is either
*	requestVote or appendEntries, the <messagedirection> will be ->. And if the value is negative, it
*	indicates we drop this message.
*
*	For example, in appendEntriesSchema, entry "t1:0->4 #1": 600 indiates the 1st appendEntries
*	message sent from node0 to node4 in term 1 will be delayed by 600ms; in requestVoteSchema, entry
*	"t1:0<-1 #2": -1 indiates the 2nd requestVote response sent from node 1 to node 0 in term 1 will
*	be dropped.
*
*	All undeclared messages will have no delay and will be be dropped.
*
*	To add tests to this framework, you need to:
*	1. construct the delay scenario in requestVoteSchema and appendEntriesSchema;
*	2. add expected events to the events array in each test;
*	3. broadcast this scenario to each proxy server by calling:
*			pt.RunAllRaftRound(requestVoteSchema, appendEntriesSchema, events, roundChan)
*	4. setup the desired election timeout and heartbeat interval for each node
*	5. using time.Sleep and pt.Propose to send out desired propose properly;
*	6. using checkGetValueAll, checkProposeRes and listen to the roundChan channel for the test
*	result from all nodes to check if there is something unexpceted.
*
*	Currently this testing framework contains 10 tests for leaderElection and 4 tests for log
*	replication.
*
*	Please feel free to contact me if you have any questions regarding to this framework
*
**/

import (
	"context"
	"cuhk/tests/raft"
	"errors"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"os"
	"regexp"
	"strings"
	"time"
)

type raftTester struct {
	myPort   int
	numNodes int
	cliMap   map[int]raft.RaftNodeClient
	px       []raft.RaftNodeClient
}

type testFunc struct {
	name string
	f    func(chan bool)
}

var (
	numNodes   = flag.Int("N", 1, "number of raft nodes in the ring")
	proxyPorts = flag.String("proxyports", "", "proxy ports of all nodes")
	testRegex  = flag.String("t", "", "test to run")
	passCount  int
	failCount  int
	timeout    = 20
	pt         *raftTester
)

type ProposeResult struct {
	nodeId int
	reply  *raft.ProposeReply
	err    error
}

var LOGE = log.New(os.Stderr, "", log.Lshortfile|log.Lmicroseconds)
var LOGF *os.File

func initRaftTester(allProxyPorts string, numNodes int) (*raftTester, error) {
	tester := new(raftTester)
	tester.numNodes = numNodes

	proxyPortList := strings.Split(allProxyPorts, ",")

	// Create RPC connection to raft nodes.
	cliMap := make(map[int]raft.RaftNodeClient)
	for i, p := range proxyPortList {
		//cli, err := rpc.DialHTTP("tcp", "localhost:"+p)
		//if err != nil {
		//	return nil, fmt.Errorf("could not connect to node %d", i)
		//}
		conn, err := grpc.Dial(fmt.Sprintf("localhost:%s", p), grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Print("Proxy fail to connect self raft node", err)
			os.Exit(1)
		}
		//defer conn.Close()
		cliMap[i] = raft.NewRaftNodeClient(conn)
	}

	tester.cliMap = cliMap
	return tester, nil
}

func (pt *raftTester) GetValue(key string, nodeID int) (*raft.GetValueReply, error) {
	args := &raft.GetValueArgs{Key: key}
	return pt.cliMap[nodeID].GetValue(context.Background(), args)
}

func (pt *raftTester) ProposeAll(op raft.Operation, key string, value int32,
	proposeRes chan ProposeResult) {
	for i := 0; i < pt.numNodes; i++ {
		go pt.Propose(op, key, value, i, proposeRes)
	}
}

func (pt *raftTester) Propose(op raft.Operation, key string, value int32, nodeID int,
	proposeRes chan ProposeResult) {
	args := &raft.ProposeArgs{Op: op, Key: key, V: value}
	reply, err := pt.cliMap[nodeID].Propose(context.Background(), args)
	proposeRes <- ProposeResult{
		nodeId: nodeID,
		reply:  reply,
		err:    err,
	}
}

func (pt *raftTester) SetAllElectTimeout(timeouts []int) {
	for idx, timeout := range timeouts {
		go pt.SetElectTimeout(timeout, idx)
	}
}

func (pt *raftTester) SetElectTimeout(timeout, nodeID int) {
	args := &raft.SetElectionTimeoutArgs{
		Timeout: int32(timeout),
	}
	pt.cliMap[nodeID].SetElectionTimeout(context.Background(), args)
}

func (pt *raftTester) SetHeartBeatInterval(timeout, nodeID int) {
	args := &raft.SetHeartBeatIntervalArgs{
		Interval: int32(timeout),
	}
	pt.cliMap[nodeID].SetHeartBeatInterval(context.Background(), args)
}

/**
 * staff node-specific functions
 */
func (pt *raftTester) RunAllRaftRound(requestVoteSchema, appendEntriesSchema map[string]int32,
	events []*raft.Event, roundChan chan error) {
	for i := 0; i < pt.numNodes; i++ {
		go pt.RunRaftRound(requestVoteSchema, appendEntriesSchema, events, i, roundChan)
	}
}

func (pt *raftTester) RunRaftRound(requestVoteSchema, appendEntriesSchema map[string]int32,
	events []*raft.Event, nodeID int, roundChan chan error) {

	args := &raft.CheckEventsArgs{RequestVoteSchema: requestVoteSchema,
		AppendEntriesSchema: appendEntriesSchema,
		ExpectedEvents:      events}

	reply, err := pt.cliMap[nodeID].CheckEvents(context.Background(), args)
	if err != nil {
		roundChan <- err
		return
	} else {
		if reply.Success {
			roundChan <- nil
		} else {
			roundChan <- errors.New(reply.ErrMsg)
		}
	}
}

func checkProposeRes(res, expected ProposeResult, nodeId int) bool {
	if res.err != nil {
		return false
	}
	if res.reply.Status == expected.reply.Status {
		if res.reply.Status == raft.Status_WrongNode &&
			res.reply.CurrentLeader != expected.reply.CurrentLeader {
			LOGE.Printf("FAIL: incorrect current Leader from Propose to node %d. Got %v, expected %v.\n",
				nodeId, res.reply.CurrentLeader, expected.reply.CurrentLeader)
			return false
		}
	} else {
		LOGE.Printf("FAIL: incorrect Status from Propose to node %d. Got %v, expected %v.\n",
			nodeId, res.reply.Status, expected.reply.Status)
		return false
	}

	return true
}

func checkGetValueAll(key string, status []raft.Status, values []int32) bool {
	for id, _ := range pt.cliMap {
		r, err := pt.GetValue(key, id)
		if err != nil {
			printFailErr("GetValue", err)
			return false
		}

		if r.Status != status[id] {
			LOGE.Printf("FAIL: Node %d: incorrect status from GetValue on key %s. Got %v, expected %v.\n",
				id, key, r.Status, status[id])
			return false
		} else {
			if r.Status == raft.Status_KeyFound && int32(r.V) != values[id] {
				LOGE.Printf("FAIL: Node %d: incorrect value from GetValue on key %s. Got %d, expected %d.\n",
					id, key, int32(r.V), values[id])
				return false
			}
		}
	}
	return true
}

func runTest(t testFunc, doneChan chan bool) {
	go t.f(doneChan)

	var pass bool
	select {
	case <-time.After(time.Duration(timeout) * time.Second):
		LOGE.Println("FAIL: test timed out")
		pass = false
	case pass = <-doneChan:
	}

	var res string
	if pass {
		res = t.name + " PASS"
		fmt.Println(res)
		log.Println(res)
		passCount++
	} else {
		res = t.name + " FAIL"
		fmt.Println(res)
		log.Println(res)
		failCount++
	}
}

func printFailErr(fname string, err error) {
	LOGE.Printf("FAIL: error on %s - %s", fname, err)
}

func printFailHint(hint string) {
	LOGE.Printf("\nHint: %s\n", hint)
}

func main() {
	LOGF, err := os.OpenFile("rafttest.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Errorf("error opening file: %v", err)
	}
	defer LOGF.Close()

	log.SetOutput(LOGF)

	btests := []testFunc{
		// tests for leader election
		{"testOneCandidateOneRoundElection", testOneCandidateOneRoundElection},
		{"testOneCandidateStartTwoElection", testOneCandidateStartTwoElection},
		{"testTwoCandidateForElection", testTwoCandidateForElection},
		{"testSplitVote", testSplitVote},
		{"testAllForElection", testAllForElection},
		{"testLeaderRevertToFollower", testLeaderRevertToFollower},

		// tests for log replication
		{"testOneSimplePut", testOneSimplePut},
		{"testOneSimpleUpdate", testOneSimpleUpdate},
		{"testOneSimpleDelete", testOneSimpleDelete},
		{"testDeleteNonExistKey", testDeleteNonExistKey},
	}

	flag.Parse()

	// Run the tests with a single tester
	raftTester, err := initRaftTester(*proxyPorts, *numNodes)
	if err != nil {
		LOGE.Fatalln("Failed to initialize test:", err)
	}
	pt = raftTester

	doneChan := make(chan bool)
	for _, t := range btests {
		if b, err := regexp.MatchString(*testRegex, t.name); b && err == nil {
			fmt.Printf("Running %s:\n", t.name)
			runTest(t, doneChan)
		}
	}
}
