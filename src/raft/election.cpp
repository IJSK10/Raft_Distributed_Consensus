#include "raft/node.h"
#include <random>
#include <chrono>
#include <algorithm>
#include <vector>

#include "utils/globals.h"
#include "spdlog/spdlog.h"

// Random timeout between 150ms and 300ms
int getRandomTimeout() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(150, 300);
    return dis(gen);
}

// Sleeps for random time. If no heartbeat received, starts election.
void RaftNode::runElectionTimer() {
    while (running_) {
        // 1. Sleep for random timeout
        int timeout = getRandomTimeout();
        std::this_thread::sleep_for(std::chrono::milliseconds(timeout));

        bool start_new_election = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (state_ == LEADER) continue;

            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - last_heartbeat_time_).count();

            if (elapsed >= timeout) {
                start_new_election = true;
            }
        }

        if (start_new_election) {
            if (DevFlags::LOG_ELECTION) {
                spdlog::info("[Node {}] Election Timeout! Starting...", config_.my_id);
            }
            startElection();
        }
    }
}

// 2. START ELECTION (Candidate Logic)
void RaftNode::startElection() {
    
    int saved_term;
    int saved_last_log_index;
    int saved_last_log_term;
    int my_id;
    std::vector<int> target_peers;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        
        // Update State
        state_ = CANDIDATE;
        currentTerm_++;
        votedFor_ = config_.my_id;
        last_heartbeat_time_ = std::chrono::steady_clock::now();
        
        // Snapshot values for the RPC arguments
        saved_term = currentTerm_;
        saved_last_log_index = lastLogIndex_;
        saved_last_log_term = lastLogTerm_;
        my_id = config_.my_id;

        // Collect peers safely
        for (auto& peer : peer_stubs_) {
            target_peers.push_back(peer.first);
        }
    }

    // PERSIST STATE (Safety)
    storage_->saveHardState(currentTerm_, votedFor_);
    
    if (DevFlags::LOG_ELECTION) {
        spdlog::info("[Election] Node {} became CANDIDATE for Term {}", config_.my_id, currentTerm_);
    }

    // Prepare RequestVote Arguments
    raft::RequestVoteArgs args;
    args.set_term(saved_term);
    args.set_candidateid(my_id);
    args.set_lastlogindex(saved_last_log_index);
    args.set_lastlogterm(saved_last_log_term);

    auto votesReceived = std::make_shared<std::atomic<int>>(1); // Start with 1 (Myself)
    int votesNeeded = (config_.peers.size() + 1) / 2 + 1;

    for (int peerId : target_peers) {
        std::thread([this, peerId, args, votesReceived, votesNeeded, saved_term]() {
            raft::RequestVoteReply reply;
            
            // Network Call
            bool success = peer_stubs_[peerId]->sendRequestVote(args, &reply);
            
            if (success) {
                std::lock_guard<std::mutex> lock(mutex_);
                
                // 1. Validation: Am I still a candidate in the same term?
                if (state_ != CANDIDATE || currentTerm_ != saved_term) return;

                // 2. Peer has higher term? Step down.
                if (reply.term() > currentTerm_) {
                    becomeFollower(reply.term());
                    return;
                }

                // 3. Vote Granted?
                if (reply.votegranted()) {
                    (*votesReceived)++;
                    if (*votesReceived == votesNeeded) {
                        becomeLeader();
                    }
                }
            }
        }).detach();
    }
}


raft::RequestVoteReply RaftNode::handleRequestVote(const raft::RequestVoteArgs& args) {
    std::lock_guard<std::mutex> lock(mutex_);
    raft::RequestVoteReply reply;
    
    // Rule 1: Reject old terms
    if (args.term() < currentTerm_) {
        reply.set_term(currentTerm_);
        reply.set_votegranted(false);
        return reply;
    }

    // Rule 2: If new term seen, update myself
    if (args.term() > currentTerm_) {
        becomeFollower(args.term());
    }

    // Rule 3: Check if I already voted for someone else
    bool canVote = (votedFor_ == -1 || votedFor_ == args.candidateid());

    // Rule 4: LOG MATCHING PROPERTY (Is candidate up-to-date?)
    // A candidate is up-to-date if:
    // 1. Their last log term is higher than mine
    // 2. OR terms are equal, but their log is longer (higher index)
    bool isLogUpToDate = false;
    if (args.lastlogterm() > lastLogTerm_) {
        isLogUpToDate = true;
    } else if (args.lastlogterm() == lastLogTerm_ && args.lastlogindex() >= lastLogIndex_) {
        isLogUpToDate = true;
    }

    if (canVote && isLogUpToDate) {
        // GRANT VOTE
        votedFor_ = args.candidateid();
        storage_->saveHardState(currentTerm_, votedFor_); // Persist!
        
        reply.set_votegranted(true);
        last_heartbeat_time_ = std::chrono::steady_clock::now(); // Reset timer so I don't start election
        if (DevFlags::LOG_ELECTION) {
            spdlog::info("[Election] Voted YES for Node {} in Term {}", args.candidateid(), currentTerm_);
        }
    } else {
        // DENY VOTE
        reply.set_votegranted(false);
    }
    
    reply.set_term(currentTerm_);
    return reply;
}

void RaftNode::becomeFollower(int term) {
    // Note: Mutex is already locked by caller
    state_ = FOLLOWER;
    currentTerm_ = term;
    votedFor_ = -1;
    storage_->saveHardState(currentTerm_, votedFor_);
    
    if (DevFlags::LOG_ELECTION) {
        spdlog::info("[Node {}] Stepping down to FOLLOWER (Term {})", config_.my_id, currentTerm_);
    }
}

void RaftNode::becomeLeader() {
    // Note: Mutex is already locked by caller
    if (state_ != CANDIDATE) return; // Race condition check

    state_ = LEADER;
    leaderId_ = config_.my_id;
    
    if (DevFlags::LOG_ELECTION) {
        spdlog::warn("[Node {}] !!! BECAME LEADER !!! Term {}", config_.my_id, currentTerm_);
    }

    // Leader Initialization:
    // Initialize nextIndex for all peers to (lastLogIndex + 1)
    for (const auto& peer : config_.peers) {
        nextIndex_[peer.first] = lastLogIndex_ + 1;
        matchIndex_[peer.first] = 0;
    }

    // Immediately send heartbeats (handled by Replication Thread)
    // We can signal the replication thread to wake up immediately
    // For now, it just waits for its next 50ms cycle.
}