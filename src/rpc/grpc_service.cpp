#include "raft/service.h" 


grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext* context, 
                                          const raft::RequestVoteArgs* request, 
                                          raft::RequestVoteReply* reply) {
    *reply = node_.handleRequestVote(*request);
    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext* context, 
                                            const raft::AppendEntriesArgs* request, 
                                            raft::AppendEntriesReply* reply) {
    *reply = node_.handleAppendEntries(*request);
    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::InstallSnapshot(grpc::ServerContext* context, 
                                              const raft::InstallSnapshotArgs* request, 
                                              raft::InstallSnapshotReply* reply) {
    reply->set_term(node_.getCurrentTerm());
    return grpc::Status::OK;
}