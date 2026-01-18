#pragma once

#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"
#include "raft/node.h"

// This class implements the methods defined in the .proto file
class RaftServiceImpl final : public raft::RaftService::Service {
public:
    // We pass the RaftNode by reference so we can forward calls to it
    explicit RaftServiceImpl(RaftNode& node) : node_(node) {}

    // 1. Inbound RequestVote (Another node wants to be leader)
    grpc::Status RequestVote(grpc::ServerContext* context, 
                             const raft::RequestVoteArgs* request, 
                             raft::RequestVoteReply* reply) override;

    // 2. Inbound AppendEntries (Leader is sending logs/heartbeat)
    grpc::Status AppendEntries(grpc::ServerContext* context, 
                               const raft::AppendEntriesArgs* request, 
                               raft::AppendEntriesReply* reply) override;

    // 3. Inbound InstallSnapshot (Leader is sending a huge backup)
    grpc::Status InstallSnapshot(grpc::ServerContext* context, 
                                 const raft::InstallSnapshotArgs* request, 
                                 raft::InstallSnapshotReply* reply) override;

private:
    RaftNode& node_;
};