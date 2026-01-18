#include "raft/node.h"
#include "raft/service.h"

#include "utils/globals.h"
#include "spdlog/spdlog.h"

#include <iostream>

RaftNode::RaftNode(ClusterConfig config, std::shared_ptr<RocksDBStore> storage)
    : config_(config), storage_(storage) {

    // We pull the data from RocksDB into our fast RAM variables.
    auto hardState = storage_->loadHardState();
    currentTerm_ = hardState.first; 
    votedFor_ = hardState.second;   
    
    // Get Log Info
    lastLogIndex_ = storage_->getLastLogIndex();
    if (lastLogIndex_ > 0) {
        lastLogTerm_ = storage_->getLogEntry(lastLogIndex_).term();
    } else {
        lastLogTerm_ = 0;
    }
    
    // 2. Initialize Volatile State
    state_ = FOLLOWER;
    leaderId_ = -1;
    commitIndex_ = 0;
    lastApplied_ = storage_->getLastAppliedIndex();
    
    // 3. Setup Networking
    for (const auto& peer : config.peers) {
        if (peer.first != config.my_id) {
            if (DevFlags::LOG_NETWORK) {
                spdlog::info("[Node {}] Connecting to peer {} at {}...", 
                    config.my_id, peer.first, peer.second);
            }
            peer_stubs_[peer.first] = std::make_unique<PeerClient>(peer.second);
        }
    }
    
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    
    rpc_service_ = std::make_unique<RaftServiceImpl>(*this);


    spdlog::info("[Node {}] Online. Term: {}, Log Index: {}", config.my_id, currentTerm_, lastLogIndex_);
}

RaftNode::~RaftNode() {
    stop();
}

void RaftNode::start() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (running_) return;
    running_ = true;

    // Launch threads (Functions defined in other files)
    election_thread_ = std::thread([this]() { this->runElectionTimer(); });
    replication_thread_ = std::thread([this]() { this->runReplicationManager(); });
    execution_thread_ = std::thread([this]() { this->runStateMachineExecutor(); });
    rpc_server_thread_ = std::thread([this]() {
        this->startRpcServer();
    });
}

void RaftNode::stop() {
    bool expected = true;
    // Only run the stop logic ONCE
    if (running_.compare_exchange_strong(expected, false)) {
        spdlog::info("[Node {}] Stopping logic...", config_.my_id);
        
        // 1. Wake up background threads
        commit_cv_.notify_all();
        
        // 2. Stop RPC Server (Unblocks rpc_server_thread_)
        stopRpcServer();
        
        // 3. JOIN ALL THREADS (Wait for them to finish safely)
        if (election_thread_.joinable()) election_thread_.join();
        if (replication_thread_.joinable()) replication_thread_.join();
        if (execution_thread_.joinable()) execution_thread_.join();
        if (rpc_server_thread_.joinable()) rpc_server_thread_.join();

    } else {
        // Just ensure flag is false and notify just in case
        running_ = false;
        commit_cv_.notify_all();
    }
}

void RaftNode::startRpcServer() {
    std::string server_address = "0.0.0.0:" + std::to_string(config_.rpc_port);
    
    grpc::ServerBuilder builder;
    // Listen on the port defined in config
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    
    // Register the service we created in the constructor
    builder.RegisterService(rpc_service_.get());
    
    // Construct the server
    rpc_server_ = builder.BuildAndStart();
    spdlog::info("[Node {}] RPC Server listening on {}", config_.my_id, server_address);
    
    // Blocking call - this thread will stay here until Shutdown() is called
    if (rpc_server_) {
        rpc_server_->Wait();
    }
}

void RaftNode::stopRpcServer() {
    if (rpc_server_) {
        spdlog::info("[Node {}] Shutting down RPC Server...", config_.my_id);
        // This creates a deadline for current calls to finish, then forces close
        rpc_server_->Shutdown(); 
    }
}