#pragma once

#include <mutex>
#include <atomic>
#include <vector>
#include <map>
#include <memory>
#include <thread>
#include <condition_variable>
#include <functional>

// Internal dependencies
#include "storage/rocksdb_store.h"
#include "rpc/client.h"
#include "client.pb.h"
#include "raft.pb.h"
#include <grpcpp/grpcpp.h>

// Configuration for the node
struct ClusterConfig {
    int my_id;
    int tcp_port;  // Port for Client Gateway (8080)
    int rpc_port;  // Port for Raft Peers (50051)
    
    // List of peers: {id, "ip:port"} 
    // e.g., { {2, "127.0.0.1:50052"}, {3, "127.0.0.1:50053"} }
    std::vector<std::pair<int, std::string>> peers;
};

class RaftServiceImpl;

class RaftNode {
public:
    // Constructor: Inject config and storage
    RaftNode(ClusterConfig config, std::shared_ptr<RocksDBStore> storage);
    ~RaftNode();

    // Lifecycle
    void start(); // Starts background threads (Election, RPC Server)
    void stop();  // Clean shutdown

    // --- CLIENT INTERFACE (Gateway calls this) ---
    // The Callback is: void(ClientResponse)
    void processClientRequest(const client::ClientRequest& req, 
                              std::function<void(client::ClientResponse)> callback);

    // --- RPC INTERFACE (gRPC Service calls these) ---
    raft::RequestVoteReply handleRequestVote(const raft::RequestVoteArgs& args);
    raft::AppendEntriesReply handleAppendEntries(const raft::AppendEntriesArgs& args);
    raft::InstallSnapshotReply handleInstallSnapshot(const raft::InstallSnapshotArgs& args);
    int getCurrentTerm() const { return currentTerm_; }

private:
    // --- INTERNAL LOGIC ---
    void runElectionTimer();       // Thread: Checks if leader is dead
    void runReplicationManager();  // Thread: Leader sends heartbeats/logs
    void runStateMachineExecutor();// Thread: Applies committed logs to DB
    
    void startElection();
    void becomeFollower(int term);
    void becomeLeader();

    void replicateToPeer(int peerId);

    // --- STATE ---
    std::mutex mutex_; // Protects ALL state below
    ClusterConfig config_;
    std::shared_ptr<RocksDBStore> storage_;

    // Network Stubs (Peer ID -> Client Wrapper)
    std::map<int, std::unique_ptr<PeerClient>> peer_stubs_;

    // --- RAFT STATE (Loaded from Storage on boot) ---
    int currentTerm_ = 0;   
    int votedFor_ = -1;     
    int lastLogIndex_ = 0;  
    int lastLogTerm_ = 0;
    
    // Volatile State
    enum Role { FOLLOWER, CANDIDATE, LEADER };
    std::atomic<Role> state_{FOLLOWER};
    
    int leaderId_ = -1; // To redirect clients
    std::atomic<bool> running_{false}; // For stopping threads

    // Raft Volatile State
    int commitIndex_ = 0;
    int lastApplied_ = 0; // Cached from storage for speed
    
    // Election Timer State
    std::chrono::steady_clock::time_point last_heartbeat_time_;

    // Leader Volatile State (Re-initialized on election)
    std::map<int, int> nextIndex_;
    std::map<int, int> matchIndex_;

    // Pending Client Callbacks (RequestID -> Function)
    // We notify these when the log is committed
    std::map<std::string, std::function<void(client::ClientResponse)>> pending_callbacks_;
    
    // Condition Variable to wake up the Executor thread
    std::condition_variable commit_cv_;


    // --- RPC SERVER MANAGEMENT ---
    std::unique_ptr<RaftServiceImpl> rpc_service_; // The "Ears"
    std::unique_ptr<grpc::Server> rpc_server_;     // The gRPC Engine
    std::thread rpc_server_thread_;                // The thread it runs on

    std::thread election_thread_;
    std::thread replication_thread_;
    std::thread execution_thread_;
    
    // Helper to launch the server
    void startRpcServer();
    void stopRpcServer();
};
