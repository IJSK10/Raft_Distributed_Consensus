#pragma once

#include <rocksdb/db.h>
#include <string>
#include <vector>
#include <memory>
#include <mutex>
#include "raft.pb.h"
#include "client.pb.h"

class RocksDBStore {
public:
    // Opens the DB at 'db_path' (creates it if missing)
    explicit RocksDBStore(const std::string& db_path);
    ~RocksDBStore();

    // --- 1. RAFT HARD STATE (Term & Vote) ---
    // Returns {term, votedFor}. If missing, returns {0, -1}.
    std::pair<int, int> loadHardState();
    void saveHardState(int term, int voted_for);

    // --- 2. THE LOG (Append-Only History) ---
    int getLastLogIndex();
    raft::LogEntry getLogEntry(int index);
    
    // Deletes everything from start_index onwards (used during conflicts)
    void truncateLog(int start_index);
    
    // Appends a new entry. Returns the new index.
    int appendLog(const raft::LogEntry& entry);
    
    void initializeGenesisData();

    // Batch append (optimization for leader)
    void appendLogs(const std::vector<raft::LogEntry>& entries);

    // --- 3. STATE MACHINE (Bank Accounts) ---
    // Returns balance. If account doesn't exist, returns 0.
    int getBalance(const std::string& account_id);
    
    // ATOMIC EXECUTION:
    // 1. Updates Balance (Transfer money)
    // 2. Saves Request Result (for deduplication)
    // 3. Updates 'lastApplied' index
    // All in one atomic RocksDB batch.
    void applyTransaction(const raft::LogEntry& entry, int sender_new_bal, int receiver_new_bal, std::string result_msg);

    // --- 4. IDEMPOTENCY (Duplicate Detection) ---
    // Returns 0 if request not found.
    int getRequestIndex(const std::string& request_id);
    
    // Returns the cached result string (e.g. "Success")
    std::string getRequestResult(const std::string& request_id);



    //SNAPSHOT

    struct SnapshotData {
        int last_index;
        int last_term;
        std::string data;
    };
    SnapshotData createSnapshot();

    // Wipes 'acct:' and 'req:', restores from data, and updates metadata.
    void applySnapshot(const std::string& snapshot_data, int last_index, int last_term);

    // Deletes logs from index 0 up to 'compact_index'.
    // Used after taking a snapshot to free disk space.
    void compactLog(int compact_index);

private:
    std::unique_ptr<rocksdb::DB> db_;
    std::mutex db_mutex_; // Thread safety for complex operations
    
    // Caches the last index so we don't have to scan the DB every time
    int last_log_index_cache_ = 0;
};