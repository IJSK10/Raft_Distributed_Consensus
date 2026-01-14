#pragma once

#include <grpcpp/grpcpp.h>
#include <string>
#include <memory>
#include <iostream>

// Include the generated gRPC header
#include "raft.grpc.pb.h" 

class PeerClient {
public:
    // Connects to "127.0.0.1:50052"
    PeerClient(std::string target_address) {
        // Create a channel (connection)
        auto channel = grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials());
        
        // Create the Stub (the object used to call methods)
        stub_ = raft::RaftService::NewStub(channel);
    }

    // --- 1. RequestVote RPC ---
    // Returns true if RPC succeeded (network is okay), false if failed.
    bool sendRequestVote(const raft::RequestVoteArgs& args, raft::RequestVoteReply* reply) {
        grpc::ClientContext context;
        // Set a short timeout (e.g., 200ms) so elections don't hang if a peer is dead
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));

        grpc::Status status = stub_->RequestVote(&context, args, reply);
        
        if (!status.ok()) {
            // Optional: Log failure for debugging
            // std::cerr << "RPC Failed: " << status.error_message() << std::endl;
            return false;
        }
        return true;
    }

    // --- 2. AppendEntries RPC ---
    bool sendAppendEntries(const raft::AppendEntriesArgs& args, raft::AppendEntriesReply* reply) {
        grpc::ClientContext context;
        // Shorter timeout for heartbeats/logs (e.g., 100ms) to keep system responsive
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));

        grpc::Status status = stub_->AppendEntries(&context, args, reply);
        return status.ok();
    }

    // --- 3. InstallSnapshot RPC ---
    bool sendInstallSnapshot(const raft::InstallSnapshotArgs& args, raft::InstallSnapshotReply* reply) {
        grpc::ClientContext context;
        // Longer timeout because snapshots are large files (e.g., 5 seconds)
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));

        grpc::Status status = stub_->InstallSnapshot(&context, args, reply);
        return status.ok();
    }

private:
    std::unique_ptr<raft::RaftService::Stub> stub_;
};