#include "rpc/client.h"

PeerClient::PeerClient(std::string target_address) {
    auto channel = grpc::CreateChannel(target_address, grpc::InsecureChannelCredentials());
    stub_ = raft::RaftService::NewStub(channel);
}

bool PeerClient::sendRequestVote(const raft::RequestVoteArgs& args, raft::RequestVoteReply* reply) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200));
    grpc::Status status = stub_->RequestVote(&context, args, reply);
    return status.ok();
}

bool PeerClient::sendAppendEntries(const raft::AppendEntriesArgs& args, raft::AppendEntriesReply* reply) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
    grpc::Status status = stub_->AppendEntries(&context, args, reply);
    return status.ok();
}

bool PeerClient::sendInstallSnapshot(const raft::InstallSnapshotArgs& args, raft::InstallSnapshotReply* reply) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    grpc::Status status = stub_->InstallSnapshot(&context, args, reply);
    return status.ok();
}