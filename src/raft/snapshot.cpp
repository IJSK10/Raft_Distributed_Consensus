#include "raft/node.h"
#include "utils/globals.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <iostream>

void RaftNode::sendSnapshot(int peerId) {
    // 1. Load Snapshot (Heavy I/O)
    auto snap = storage_->createSnapshot();
    
    // 2. Prepare RPC
    raft::InstallSnapshotArgs args;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        args.set_term(currentTerm_);
        args.set_leaderid(config_.my_id);
    }

    args.set_lastincludedindex(snap.last_index);
    args.set_lastincludedterm(snap.last_term);
    args.set_data(snap.data); // The entire RocksDB state dump

    args.set_offset(0);  // We are sending the whole file at once
    args.set_done(true); // No chunks, we are done immediately

    // 3. Send over Network
    raft::InstallSnapshotReply reply;
    bool success = peer_stubs_[peerId]->sendInstallSnapshot(args, &reply);

    // 4. Update State
    if (success) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (reply.term() > currentTerm_) {
            becomeFollower(reply.term());
            return;
        }
        
        // If successful, the peer is now at least at the snapshot index
        // We update matchIndex and nextIndex so we can resume AppendEntries next time
        if (state_ == LEADER) {
            matchIndex_[peerId] = std::max(matchIndex_[peerId], snap.last_index);
            nextIndex_[peerId] = matchIndex_[peerId] + 1;
            if (DevFlags::LOG_SNAPSHOT) {
                spdlog::info("[Snapshot] Successfully sent to Peer {}. Updated matchIndex to {}", peerId, matchIndex_[peerId]);
            }
        }
    }
}


raft::InstallSnapshotReply RaftNode::handleInstallSnapshot(const raft::InstallSnapshotArgs& args) {
    std::lock_guard<std::mutex> lock(mutex_);
    raft::InstallSnapshotReply reply;
    
    reply.set_term(currentTerm_);
    
    if (args.term() < currentTerm_) {
        return reply;
    }

    if (args.term() > currentTerm_) {
        becomeFollower(args.term());
    }
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    leaderId_ = args.leaderid();

    // 1. Check if we already have this data
    if (args.lastincludedindex() <= lastApplied_) {
        return reply; // We are already ahead
    }

    if (args.offset() != 0 || !args.done()) {
        spdlog::warn("[Snapshot] Received chunked snapshot (Offset: {}). This simple implementation only supports full snapshots.", args.offset());
        // In a real system, we would buffer chunks here. 
        // For now, we assume offset=0 and done=true.
    }

    if (DevFlags::LOG_SNAPSHOT){
        spdlog::info("[Snapshot] Received Snapshot from Leader (Index: {}). applying...", args.lastincludedindex());
    }

    // 2. Apply to Storage (Wipes DB and restores)
    storage_->applySnapshot(args.data(), args.lastincludedindex(), args.lastincludedterm());

    // 3. Update RAM State
    lastLogIndex_ = args.lastincludedindex();
    lastLogTerm_ = args.lastincludedterm();
    lastApplied_ = args.lastincludedindex();
    commitIndex_ = args.lastincludedindex();

    // 4. Discard older logs (We just replaced the state machine, so history before this is irrelevant)
    storage_->truncateLog(args.lastincludedindex() + 1);

    // We must ensure that getLogEntry(snapshot_index) returns the correct term
    // so that the NEXT AppendEntries (which uses this as prevLogIndex) succeeds.
    raft::LogEntry dummy_entry;
    dummy_entry.set_index(args.lastincludedindex());
    dummy_entry.set_term(args.lastincludedterm());

    // Create a harmless "No-Op" command
    auto* cmd = dummy_entry.mutable_command();
    cmd->set_request_id("SNAPSHOT_ANCHOR");       // Unique ID marking it as internal
    cmd->set_type(client::ClientRequest::GET_BALANCE); // Read-only is safest
    cmd->set_sender_id("SYSTEM_ANCHOR");          // Fake user that won't interfere with real accounts

    // Force write this single log entry to RocksDB
    storage_->appendLog(dummy_entry);

    reply.set_term(currentTerm_);
    return reply;
}

