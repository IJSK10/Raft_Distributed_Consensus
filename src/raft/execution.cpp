#include "raft/node.h"
#include "utils/globals.h"
#include "spdlog/spdlog.h"
#include <iostream>

void RaftNode::runStateMachineExecutor() {
    while (running_) {
        std::vector<raft::LogEntry> logs_to_apply;

        // 1. Wait for Commit
        {
            std::unique_lock<std::mutex> lock(mutex_);
            commit_cv_.wait(lock, [this] {
                return !running_ || (commitIndex_ > lastApplied_);
            });

            if (!running_) break;

            // Fetch logs while holding lock
            int batch_limit = 100; 
            for (int i = lastApplied_ + 1; i <= commitIndex_ && logs_to_apply.size() < batch_limit; ++i) {
                logs_to_apply.push_back(storage_->getLogEntry(i));
            }
        } 

        // 2. Execute Logic (No Lock)
        for (const auto& entry : logs_to_apply) {
            std::string result_msg = "Success";
            int sender_new_bal = -1;   // -1 indicates "No Change"
            int receiver_new_bal = -1; // -1 indicates "No Change"

            const auto& cmd = entry.command();
            
            if (cmd.type() == client::ClientRequest::TRANSFER) {
                // A. READ PHASE
                int sender_bal = storage_->getBalance(cmd.sender_id());
                int receiver_bal = storage_->getBalance(cmd.receiver_id());
                
                // B. LOGIC PHASE
                if (sender_bal >= cmd.amount()) {
                    sender_new_bal = sender_bal - cmd.amount();
                    receiver_new_bal = receiver_bal + cmd.amount(); 
                    if (DevFlags::LOG_EXECUTION) {
                        spdlog::info("[Executor] Transfer: {} -> {} (${})", 
                            cmd.sender_id(), cmd.receiver_id(), cmd.amount());
                    }
                } else {
                    result_msg = "Insufficient Funds";
                    if (DevFlags::LOG_EXECUTION) {
                        spdlog::warn("[Executor] Failed Transfer: Insufficient Funds (Sender: {})", cmd.sender_id());
                    }
                }
            } else {
                int current_bal = storage_->getBalance(cmd.sender_id());
                result_msg = "Balance: " + std::to_string(current_bal);
            }

            // C. WRITE PHASE
            // We pass the calculated values. If they are -1, the storage layer ignores them.
            storage_->applyTransaction(entry, sender_new_bal, receiver_new_bal, result_msg);

            // 3. Notify Waiting Client
            {
                std::lock_guard<std::mutex> lock(mutex_);
                lastApplied_ = entry.index(); 
                
                auto it = pending_callbacks_.find(cmd.request_id());
                if (it != pending_callbacks_.end()) {
                    client::ClientResponse resp;
                    bool is_success = (result_msg == "Success" || result_msg.find("Balance") != std::string::npos);
                    resp.set_success(is_success);
                    resp.set_result(result_msg);
                    resp.set_leader_hint(leaderId_);
                    
                    it->second(resp);
                    pending_callbacks_.erase(it);
                }
            }

            if (lastApplied_ % 100 == 0) {
                
                // SAFETY BUFFER: Keep the last 10 logs on disk.
                // This allows followers who are slightly behind to catch up 
                // without needing a full snapshot transfer.
                int compact_index = lastApplied_ - 50;

                if (compact_index > 0) {
                    if (DevFlags::LOG_SNAPSHOT) {
                    spdlog::info("[Snapshot] Taking snapshot. Compacting logs up to Index {} (Current Head: {})...", 
                        compact_index, lastApplied_);
                    }
                    
                    // A. Create Snapshot (In RocksDB, state is already saved)
                    
                    // B. Compact Logs (Delete Log entries 0 to compact_index)
                    storage_->compactLog(compact_index);
                    
                    if (DevFlags::LOG_EXECUTION) {
                        spdlog::info("[Snapshot] Logs compacted successfully.");
                    }
                }
            }
        }
    }
}