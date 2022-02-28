package cuhk.asgn;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import raft.Raft.AppendEntriesArgs;
import raft.Raft.AppendEntriesReply;
import raft.Raft.CheckEventsArgs;
import raft.Raft.CheckEventsReply;
import raft.Raft.GetValueArgs;
import raft.Raft.GetValueReply;
import raft.Raft.LogEntry;
import raft.Raft.ProposeArgs;
import raft.Raft.ProposeReply;
import raft.Raft.RequestVoteArgs;
import raft.Raft.RequestVoteReply;
import raft.Raft.SetElectionTimeoutArgs;
import raft.Raft.SetElectionTimeoutReply;
import raft.Raft.SetHeartBeatIntervalArgs;
import raft.Raft.SetHeartBeatIntervalReply;
import raft.RaftNodeGrpc;
import raft.RaftNodeGrpc.RaftNodeBlockingStub;

public class RaftRunner {

  public static void main(String[] args) throws Exception {
    String ports = args[1];
    int myport = Integer.parseInt(args[0]);
    int nodeID = Integer.parseInt(args[2]);
    int heartBeatInterval = Integer.parseInt(args[3]);
    int electionTimeout = Integer.parseInt(args[4]);

    String[] portStrings = ports.split(",");

    // A map where
    // 		the key is the node id
    //		the value is the {hostname:port}
    Map<Integer, Integer> hostmap = new HashMap<>();
    for (int x = 0; x < portStrings.length; x++) {
      hostmap.put(x, Integer.valueOf(portStrings[x]));
    }

    RaftNode node = NewRaftNode(myport, hostmap, nodeID, heartBeatInterval, electionTimeout);

    final Server server = node.getGrpcServer();
    //Stop the server
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        server.shutdown();
        System.err.println("*** server shut down");
      }
    });

    server.awaitTermination();

  }

  // Desc:
  // NewRaftNode creates a new RaftNode. This function should return only when
  // all nodes have joined the ring, and should return a non-nil error if this node
  // could not be started in spite of dialing any other nodes.
  //
  // Params:
  // myport: the port of this new node. We use tcp in this project.
  //			   	Note: Please listen to this port rather than nodeidPortMap[nodeId]
  // nodeidPortMap: a map from all node IDs to their ports.
  // nodeId: the id of this node
  // heartBeatInterval: the Heart Beat Interval when this node becomes leader. In millisecond.
  // electionTimeout: The election timeout for this node. In millisecond.
  public static RaftNode NewRaftNode(int myPort, Map<Integer, Integer> nodeidPortMap, int nodeId, int heartBeatInterval,
    int electionTimeout) throws IOException {
        //TODO: implement this !
        int numNodes = nodeidPortMap.size();

        nodeidPortMap.remove(nodeId);

        Map<Integer, RaftNodeBlockingStub> hostConnectionMap = new HashMap<>();

        RaftNode raftNode = new RaftNode();

        Server server = ServerBuilder.forPort(myPort).addService(raftNode).build();
        raftNode.server = server;
        server.start();

        //crate channel to other RaftNode
        for (Map.Entry<Integer, Integer> entry : nodeidPortMap.entrySet()) {
          int id = entry.getValue();
          Channel channel = ManagedChannelBuilder.forAddress("127.0.0.1", id)
              .usePlaintext() // disable TLS
              .build();

          hostConnectionMap.put(
              id,
              RaftNodeGrpc.newBlockingStub(channel)
          );
        }

        System.out.println("Successfully connect all nodes");

        //TODO: kick off leader election here !

        return raftNode;
    }



  public static class RaftNode extends RaftNodeGrpc.RaftNodeImplBase {

    public Server server;
    List<LogEntry> log;
    //TODO: add class attributes you needed

    public RaftNode(){
      //TODO: Implement this!
    }

    // Desc:
    // Propose initializes proposing a new operation, and replies with the
    // result of committing this operation. Propose should not return until
    // this operation has been committed, or this node is not leader now.
    //
    // If the we put a new <k, v> pair or deleted an existing <k, v> pair
    // successfully, it should return OK; If it tries to delete an non-existing
    // key, a KeyNotFound should be returned; If this node is not leader now,
    // it should return WrongNode as well as the currentLeader id.
    //
    // Params:
    // args: the operation to propose
    // reply: as specified in Desc
    @Override
    public void propose(ProposeArgs request, StreamObserver<ProposeReply> responseObserver) {
      // TODO: Implement this!
    }

    // Desc:GetValue
    // GetValue looks up the value for a key, and replies with the value or with
    // the Status KeyNotFound.
    //
    // Params:
    // args: the key to check
    // reply: the value and status for this lookup of the given key
    @Override
    public void getValue(GetValueArgs request, StreamObserver<GetValueReply> responseObserver) {
      // TODO: Implement this!
    }

    // Desc:
    // Receive a RecvRequestVote message from another Raft Node. Check the paper for more details.
    //
    // Params:
    // args: the RequestVote Message, you must include From(src node id) and To(dst node id) when
    // you call this API
    // reply: the RequestVote Reply Message
    @Override
    public void requestVote(RequestVoteArgs request,
        StreamObserver<RequestVoteReply> responseObserver) {
      // TODO: Implement this!
    }

    // Desc:
    // Receive a RecvAppendEntries message from another Raft Node. Check the paper for more details.
    //
    // Params:
    // args: the AppendEntries Message, you must include From(src node id) and To(dst node id) when
    // you call this API
    // reply: the AppendEntries Reply Message
    @Override
    public void appendEntries(AppendEntriesArgs request,
        StreamObserver<AppendEntriesReply> responseObserver) {
      // TODO: Implement this!
    }

    // Desc:
    // Set electionTimeOut as args.Timeout milliseconds.
    // You also need to stop current ticker and reset it to fire every args.Timeout milliseconds.
    //
    // Params:
    // args: the heartbeat duration
    // reply: no use
    @Override
    public void setElectionTimeout(SetElectionTimeoutArgs request,
        StreamObserver<SetElectionTimeoutReply> responseObserver) {
      // TODO: Implement this!
    }

    // Desc:
    // Set heartBeatInterval as args.Interval milliseconds.
    // You also need to stop current ticker and reset it to fire every args.Interval milliseconds.
    //
    // Params:
    // args: the heartbeat duration
    // reply: no use
    @Override
    public void setHeartBeatInterval(SetHeartBeatIntervalArgs request,
        StreamObserver<SetHeartBeatIntervalReply> responseObserver) {
      // TODO: Implement this!
    }

    //NO NEED TO TOUCH THIS FUNCTION
    @Override
    public void checkEvents(CheckEventsArgs request,
        StreamObserver<CheckEventsReply> responseObserver) {
    }

    public Server getGrpcServer(){
      return this.server;
    }
  }
}
