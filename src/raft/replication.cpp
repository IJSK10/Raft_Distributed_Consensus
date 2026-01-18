#include "raft/node.h"
#include "utils/globals.h"
#include "spdlog/spdlog.h"
#include <algorithm>
#include <iostream>

void RaftNode::runReplicationManager() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // Step 1: Get list of peers and check leadership (Quick Lock)
        std::vector<int> target_peers;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_ != LEADER) continue;
            
            for (auto& peer : peer_stubs_) {
                target_peers.push_back(peer.first);
            }
        } // Lock RELEASED here. Leader is free to handle new clients!

        // Step 2: Spawn parallel tasks
        // Each thread manages its own locking and disk I/O independently.
        for (int peerId : target_peers) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (replication_active_[peerId]) {
                    continue; // Skip this peer, previous thread is still working
                }
                replication_active_[peerId] = true; // Mark as busy
            }

            std::thread([this, peerId]() {
                
                // Do the work (Snapshot or AppendEntries)
                this->replicateToPeer(peerId);

                // C. MARK AS DONE
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    replication_active_[peerId] = false;
                }
                
            }).detach();
        }
    }
}


void RaftNode::replicateToPeer(int peerId) {
    // PHASE 1: SNAPSHOT STATE (Short Lock)
    // We only lock to grab the numbers we need.
    int nextIdx;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ != LEADER) return;
        nextIdx = nextIndex_[peerId];
    }

    int prevLogIndex = nextIdx - 1;
    if (prevLogIndex > 0) {
        // RocksDBStore returns term 0 if the log is missing/compacted
        if (storage_->getLogEntry(prevLogIndex).term() == 0) {
            spdlog::info("[Replication] Peer {} is too far behind (Next: {}, My Low: {}). Sending Snapshot...", 
                peerId, nextIdx, storage_->getLastLogIndex());
            
            sendSnapshot(peerId);
            return; // Done for this cycle
        }
    }

    int current_term_snapshot;
    int commit_index_snapshot;
    int my_id;
    int last_log_index_snapshot;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ != LEADER) return;
        
        nextIdx = nextIndex_[peerId];
        current_term_snapshot = currentTerm_;
        commit_index_snapshot = commitIndex_;
        last_log_index_snapshot = lastLogIndex_;
        my_id = config_.my_id;
    } // Unlock!
    // PHASE 2: PREPARE DATA (Heavy Disk I/O - No Lock)
    // We read RocksDB here. Since we don't hold the main mutex,
    // the Leader can still process client requests in parallel.
    
    int prevLogTerm = 0;
    if (prevLogIndex > 0) {
        prevLogTerm = storage_->getLogEntry(prevLogIndex).term();
    }

    std::vector<raft::LogEntry> entries_to_send;
    // Cap batch size to prevent network congestion
    int batch_limit = 100; 
    
    if (nextIdx <= last_log_index_snapshot) {
        for (int i = nextIdx; i <= last_log_index_snapshot && entries_to_send.size() < batch_limit; ++i) {
            entries_to_send.push_back(storage_->getLogEntry(i));
        }
    }

    // Prepare Arguments
    raft::AppendEntriesArgs args;
    args.set_term(current_term_snapshot);
    args.set_leaderid(my_id);
    args.set_prevlogindex(prevLogIndex);
    args.set_prevlogterm(prevLogTerm);
    args.set_leadercommit(commit_index_snapshot);
    
    for (const auto& entry : entries_to_send) {
        *args.add_entries() = entry;
    }

    // PHASE 3: NETWORK CALL (Slow - No Lock)
    raft::AppendEntriesReply reply;
    // Note: peer_stubs_ is thread-safe (read-only after init)
    bool success = peer_stubs_[peerId]->sendAppendEntries(args, &reply);

    // PHASE 4: UPDATE STATE (Short Lock)
    if (success) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        if (state_ != LEADER || currentTerm_ != current_term_snapshot) return;

        if (reply.term() > currentTerm_) {
            becomeFollower(reply.term());
            return;
        }

        if (reply.success()) {
            // Success Logic
            if (!entries_to_send.empty()) {
                int last_sent_index = entries_to_send.back().index();
                
                // Maximize in case out-of-order replies arrive
                matchIndex_[peerId] = std::max(matchIndex_[peerId], last_sent_index);
                nextIndex_[peerId] = matchIndex_[peerId] + 1;
                
                // Commit Logic (Check for majority)
                // Optimized: Start checking from the index we just updated
                for (int N = lastLogIndex_; N > commitIndex_; N--) {
                    if (storage_->getLogEntry(N).term() != currentTerm_) continue;

                    int count = 1; 
                    for (const auto& p : config_.peers) {
                        if (p.first != config_.my_id && matchIndex_[p.first] >= N) count++;
                    }
                    
                    if (count > (config_.peers.size() + 1) / 2) {
                        commitIndex_ = N;
                        if (DevFlags::LOG_REPLICATION) {
                            spdlog::info("[Leader] Committed Log {}", commitIndex_);
                        }
                        commit_cv_.notify_all(); 
                        break; 
                    }
                }
            }
        } else {
            // Failure Logic (Backtrack)
            // If they rejected index X, try X-1 next time.
            if (nextIndex_[peerId] > 1) {
                nextIndex_[peerId]--;
            }
        }
    }
}


// 3. HANDLE APPEND ENTRIES (Follower Logic)
// (This part remains the same as before - it's already optimal)
raft::AppendEntriesReply RaftNode::handleAppendEntries(const raft::AppendEntriesArgs& args) {
    std::lock_guard<std::mutex> lock(mutex_);
    raft::AppendEntriesReply reply;
    
    // 1. Term Check
    if (args.term() < currentTerm_) {
        reply.set_term(currentTerm_);
        reply.set_success(false);
        return reply;
    }

    // 2. Leader is Valid
    last_heartbeat_time_ = std::chrono::steady_clock::now();
    leaderId_ = args.leaderid();
    
    if (args.term() > currentTerm_) {
        becomeFollower(args.term());
    } else if (state_ != FOLLOWER) {
        becomeFollower(args.term());
    }

    // 3. Log Consistency
    if (args.prevlogindex() > 0) {
        if (args.prevlogindex() > lastLogIndex_) {
            spdlog::warn("[Replication] Rejecting: Leader PrevLogIndex {} > My LastLogIndex {}", 
                args.prevlogindex(), lastLogIndex_);
            reply.set_term(currentTerm_);
            reply.set_success(false);
            return reply;
        }

        int myTerm;
        
        // If the leader is asking about our exact Last Log, use our RAM state.
        // This is crucial immediately after a snapshot, where the log might not exist on disk yet.
        if (args.prevlogindex() == lastLogIndex_) {
            myTerm = lastLogTerm_; 
        } else {
            // Otherwise, read from disk
            myTerm = storage_->getLogEntry(args.prevlogindex()).term();
        }
        // -------------------------------------

        if (myTerm != args.prevlogterm()) {
            spdlog::warn("[Replication] Rejecting: Index {} Term Mismatch. Mine: {} Leader: {}", 
                args.prevlogindex(), myTerm, args.prevlogterm());
            reply.set_term(currentTerm_);
            reply.set_success(false);
            return reply;
        }
    }

    // 4. Append Entries
    int currentIndex = args.prevlogindex();
    for (const auto& entry : args.entries()) {
        currentIndex++;
        if (currentIndex <= lastLogIndex_) {
            if (storage_->getLogEntry(currentIndex).term() != entry.term()) {
                storage_->truncateLog(currentIndex);
                lastLogIndex_ = currentIndex - 1;
                storage_->appendLog(entry);
                lastLogIndex_ = currentIndex;
                lastLogTerm_ = entry.term();
            }
        } else {
            storage_->appendLog(entry);
            lastLogIndex_ = currentIndex;
            lastLogTerm_ = entry.term();
        }
    }

    // 5. Update Commit Index
    if (args.leadercommit() > commitIndex_) {
        commitIndex_ = std::min(args.leadercommit(), lastLogIndex_);
        commit_cv_.notify_all(); 
    }

    reply.set_term(currentTerm_);
    reply.set_success(true);
    return reply;
}