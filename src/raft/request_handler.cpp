#include "raft/node.h"
#include "utils/globals.h" 
#include "spdlog/spdlog.h"
#include <iostream>

void RaftNode::processClientRequest(const client::ClientRequest& req, 
                                    std::function<void(client::ClientResponse)> callback) {

    // 1. LEADERSHIP CHECK
    // If I am not the leader, I cannot process writes (or linearizable reads).
    // I must redirect the client to the correct leader.
    if (state_ != LEADER) {
        client::ClientResponse resp;
        resp.set_success(false);
        resp.set_leader_hint(leaderId_); 
        resp.set_result("Not Leader");
        callback(resp);
        return;
    }

    // 2. IDEMPOTENCY CHECK (Deduplication)
    // Check if this request was ALREADY executed in the past.
    // This happens if the client didn't get the ack and retried.
    std::string existing_result = storage_->getRequestResult(req.request_id());
    if (!existing_result.empty()) {
        if (DevFlags::LOG_CLIENT) {
            spdlog::info("[Node {}] Duplicate Request {} found in history.", config_.my_id, req.request_id());
        }
        
        client::ClientResponse resp;
        resp.set_success(true);
        resp.set_result(existing_result);
        resp.set_leader_hint(leaderId_); // We assume we are leader if we got here
        callback(resp);
        return;
    }

    // 3. IN-FLIGHT CHECK
    // Check if we are currently working on this request (but not done yet).
    int assigned_index = 0;
    
    {
        std::lock_guard<std::mutex> lock(mutex_);

        // 1. Re-Verify Leadership (State might have changed while we were reading disk)
        if (state_ != LEADER) {
            client::ClientResponse resp;
            resp.set_success(false);
            resp.set_leader_hint(leaderId_);
            resp.set_result("Not Leader");
            
            // We unlock before calling callback to avoid blocking
            mutex_.unlock(); 
            callback(resp);
            return;
        }

        // 2. In-Flight Check (RAM Check - Fast)
        if (pending_callbacks_.count(req.request_id())) {
            return; // Already processing
        }

        // 3. Assign Index & Create Entry
        assigned_index = lastLogIndex_ + 1;
        
        raft::LogEntry entry;
        entry.set_term(currentTerm_);
        entry.set_index(assigned_index);
        *entry.mutable_command() = req;

        // 4. Append to Disk 
        // We do this inside the lock to prevent "Log Holes" 
        // (e.g., Log 101 writing before Log 100 finishes).
        storage_->appendLog(entry);
        
        // 5. Update Memory State
        lastLogIndex_ = assigned_index;
        lastLogTerm_ = currentTerm_;

        // 6. Register Callback
        pending_callbacks_[req.request_id()] = callback;
        
        // Lock releases here automatically
    }

    if (DevFlags::LOG_CLIENT) {
        spdlog::info("[Node {}] New Log Created at Index {} (Type: {})", 
            config_.my_id, lastLogIndex_, static_cast<int>(req.type()));
    }

    // The Replication Thread (replication.cpp) picks this up automatically 
    // in its next 50ms cycle because 'lastLogIndex_' has increased.
}