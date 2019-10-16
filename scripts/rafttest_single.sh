#!/usr/bin/env bash

#!/bin/bash

#if [ -z $GOPATH ]; then
#    echo "FAIL: GOPATH environment variable is not set"
#    exit 1
#fi
BASE_PATH=$(cd `dirname $0`/.. && pwd)
BIN_PATH=$BASE_PATH/bin

if [ $# -le 1 ]; then
    # Pick distinct random ports between [10000, 20000).
    while ((i<11))
    do
       N=$(((RANDOM % 10000) + 10000))
       echo "${A[*]}" | grep $N && continue # if number already in the array
       A[$i]=$N
       ((i++))
    done

    NODE_PORT0=${A[0]}
    NODE_PORT1=${A[1]}
    NODE_PORT2=${A[2]}
    NODE_PORT3=${A[3]}
    NODE_PORT4=${A[4]}
    PROXY_NODE_PORT0=${A[5]}
    PROXY_NODE_PORT1=${A[6]}
    PROXY_NODE_PORT2=${A[7]}
    PROXY_NODE_PORT3=${A[8]}
    PROXY_NODE_PORT4=${A[9]}
    TESTER_PORT=${A[10]}
else
    NODE_PORT0=$2
    NODE_PORT1=$3
    NODE_PORT2=$4
    NODE_PORT3=$5
    NODE_PORT4=$6
    PROXY_NODE_PORT0=$7
    PROXY_NODE_PORT1=$8
    PROXY_NODE_PORT2=$9
    PROXY_NODE_PORT3=${10}
    PROXY_NODE_PORT4=${11}
    TESTER_PORT=${12}
fi

RAFT_TEST=$BASE_PATH/bin/rafttest
RAFT_NODE=$BASE_PATH/bin/raftrunner
PROXY_NODE=$BASE_PATH/bin/raftproxyrunner
ALL_PORTS="${NODE_PORT0},${NODE_PORT1},${NODE_PORT2},${NODE_PORT3},${NODE_PORT4}"
ALL_PROXY_PORTS="${PROXY_NODE_PORT0},${PROXY_NODE_PORT1},${PROXY_NODE_PORT2},${PROXY_NODE_PORT3},${PROXY_NODE_PORT4}"
##################################################

if [ $# -le 1 ]; then
    echo "All real ports:" ${ALL_PORTS}
    echo "All proxy ports:" ${ALL_PROXY_PORTS}
fi

# start all the proxy upon each students' nodes
${PROXY_NODE} -raftport=${NODE_PORT0} -proxyport=${PROXY_NODE_PORT0} -id=0 &
PROXY_NODE_PID0=$!


${PROXY_NODE} -raftport=${NODE_PORT1} -proxyport=${PROXY_NODE_PORT1} -id=1 &
PROXY_NODE_PID1=$!


${PROXY_NODE} -raftport=${NODE_PORT2} -proxyport=${PROXY_NODE_PORT2} -id=2 &
PROXY_NODE_PID2=$!


${PROXY_NODE} -raftport=${NODE_PORT3} -proxyport=${PROXY_NODE_PORT3} -id=3 &
PROXY_NODE_PID3=$!


${PROXY_NODE} -raftport=${NODE_PORT4} -proxyport=${PROXY_NODE_PORT4} -id=4 &
PROXY_NODE_PID4=$!

# Start 5 student raft nodes.
${RAFT_NODE} ${NODE_PORT0} ${ALL_PROXY_PORTS} 0 10000 10000 & # 2> /dev/null &
RAFT_NODE_PID0=$!


${RAFT_NODE} ${NODE_PORT1} ${ALL_PROXY_PORTS} 1 10000 10000 & # 2> /dev/null &
RAFT_NODE_PID1=$!


${RAFT_NODE} ${NODE_PORT2} ${ALL_PROXY_PORTS} 2 10000 10000 & # 2> /dev/null &
RAFT_NODE_PID2=$!


${RAFT_NODE} ${NODE_PORT3} ${ALL_PROXY_PORTS} 3 10000 10000 & # 2> /dev/null &
RAFT_NODE_PID3=$!


${RAFT_NODE} ${NODE_PORT4} ${ALL_PROXY_PORTS} 4 10000 10000 & # 2> /dev/null &
RAFT_NODE_PID4=$!

## Start rafttest.
${RAFT_TEST} -proxyports=${ALL_PROXY_PORTS} -N=5 -t $1

# Kill raft node.
kill -9 ${RAFT_NODE_PID0} 2> /dev/null
kill -9 ${RAFT_NODE_PID1} 2> /dev/null
kill -9 ${RAFT_NODE_PID2} 2> /dev/null
kill -9 ${RAFT_NODE_PID3} 2> /dev/null
kill -9 ${RAFT_NODE_PID4} 2> /dev/null

# Kill proxy
kill -9 ${PROXY_NODE_PID0} 2> /dev/null
kill -9 ${PROXY_NODE_PID1} 2> /dev/null
kill -9 ${PROXY_NODE_PID2} 2> /dev/null
kill -9 ${PROXY_NODE_PID3} 2> /dev/null
kill -9 ${PROXY_NODE_PID4} 2> /dev/null


wait ${RAFT_NODE_PID0} 2> /dev/null
wait ${RAFT_NODE_PID1} 2> /dev/null
wait ${RAFT_NODE_PID2} 2> /dev/null
wait ${RAFT_NODE_PID3} 2> /dev/null
wait ${RAFT_NODE_PID4} 2> /dev/null


wait ${PROXY_NODE_PID0} 2> /dev/null
wait ${PROXY_NODE_PID1} 2> /dev/null
wait ${PROXY_NODE_PID2} 2> /dev/null
wait ${PROXY_NODE_PID3} 2> /dev/null
wait ${PROXY_NODE_PID4} 2> /dev/null