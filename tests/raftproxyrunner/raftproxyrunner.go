package main

/*
*	AUTHOR:
*			Shimin Wang <wyudun@gmail.com>
*
*	DESCRIPTION:
*			Runner for launching new proxy upon a raft node
 */
import (
	"cuhk/tests/raftproxy"
	"flag"
	"fmt"
	"log"
	"time"
)

var (
	raftPorts  = flag.Int("raftport", 9000, "real ports of this node")
	proxyPort  = flag.Int("proxyport", 9001, "proxy ports of this node")
	nodeID     = flag.Int("id", 0, "node ID must match index of this node's port in the ports list")
	numRetries = flag.Int("retries", 5, "number of times a node should retry dialing another node")
)

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)
}

func main() {
	flag.Parse()

	// set up the proxy
	for i := 0; i <= *numRetries; i++ {
		if i == *numRetries {
			fmt.Errorf("could not instantiate proxy on port %d", *proxyPort)
		}
		_, err := raftproxy.NewProxy(*raftPorts, *proxyPort, *nodeID)
		if err != nil {
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}

	// Run the paxos node forever.
	select {}
}
