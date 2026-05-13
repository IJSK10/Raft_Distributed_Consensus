#include "storage/rocksdb_store.h"
#include "utils/globals.h"
#include "spdlog/spdlog.h"
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/iterator.h>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <cstring>

// Helper: Format keys consistently (e.g., "log:0000000001")
std::string formatLogKey(int index) {
    std::ostringstream oss;
    oss << "log:" << std::setw(10) << std::setfill('0') << index;
    return oss.str();
}

RocksDBStore::RocksDBStore(const std::string& db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    
    rocksdb::Status status = rocksdb::DB::Open(options, db_path, &db_);
    if (!status.ok()) {
        spdlog::error("CRITICAL: Failed to open RocksDB at {}", db_path);
        exit(1);
    }

    // Load Cache
    std::string value;
    if (db_->Get(rocksdb::ReadOptions(), "sys:lastLogIndex", &value).ok()) {
        last_log_index_cache_ = std::stoi(value);
    } else {
        last_log_index_cache_ = 0;
    }
    
    if (DevFlags::LOG_STORAGE) {
        spdlog::info("[Storage] DB Opened. Last Log Index: {}", last_log_index_cache_);
    }
}

RocksDBStore::~RocksDBStore() {
    // db_ closes automatically
}

// --- 1. GENESIS (Users 1-9000) ---
void RocksDBStore::initializeGenesisData() {
    std::lock_guard<std::mutex> lock(db_mutex_);

    // Check if Account 1 exists. If so, we are already initialized.
    std::string val;
    if (db_->Get(rocksdb::ReadOptions(), "acct:1", &val).ok()) {
        return;
    }

    if (DevFlags::LOG_STORAGE)
    {
        spdlog::info("[Storage] Genesis: Creating 9000 accounts with Balance $100000...");
    }
    
    rocksdb::WriteBatch batch;
    for (int i = 1; i <= 9000; ++i) {
        batch.Put("acct:" + std::to_string(i), "100000");
    }
    
    db_->Write(rocksdb::WriteOptions(), &batch);

    if (DevFlags::LOG_STORAGE)
    {
        spdlog::info("[Storage] Genesis Complete.");
    }
}

// --- 2. RAFT HARD STATE ---
std::pair<int, int> RocksDBStore::loadHardState() {
    int term = 0;
    int votedFor = -1;
    std::string val;
    
    if (db_->Get(rocksdb::ReadOptions(), "meta:term", &val).ok()) term = std::stoi(val);
    if (db_->Get(rocksdb::ReadOptions(), "meta:vote", &val).ok()) votedFor = std::stoi(val);
    
    return {term, votedFor};
}

void RocksDBStore::saveHardState(int term, int voted_for) {
    rocksdb::WriteBatch batch;
    batch.Put("meta:term", std::to_string(term));
    batch.Put("meta:vote", std::to_string(voted_for));
    db_->Write(rocksdb::WriteOptions(), &batch);
}

// --- 3. THE LOG ---
int RocksDBStore::getLastLogIndex() {
    std::lock_guard<std::mutex> lock(db_mutex_);
    return last_log_index_cache_;
}

raft::LogEntry RocksDBStore::getLogEntry(int index) {
    std::string value;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), formatLogKey(index), &value);
    
    raft::LogEntry entry;
    if (s.ok()) {
        entry.ParseFromString(value);
    } else {
        entry.set_term(0); // Flag as empty/snapshot gap
    }
    return entry;
}

int RocksDBStore::appendLog(const raft::LogEntry& entry) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    
    int new_index = entry.index();
    std::string value;
    entry.SerializeToString(&value);
    
    rocksdb::WriteBatch batch;
    batch.Put(formatLogKey(new_index), value);
    batch.Put("sys:lastLogIndex", std::to_string(new_index));
    
    db_->Write(rocksdb::WriteOptions(), &batch);
    
    last_log_index_cache_ = new_index;
    return new_index;
}

void RocksDBStore::appendLogs(const std::vector<raft::LogEntry>& entries) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    if (entries.empty()) return;

    rocksdb::WriteBatch batch;
    int max_index = last_log_index_cache_;

    for (const auto& entry : entries) {
        std::string value;
        entry.SerializeToString(&value);
        batch.Put(formatLogKey(entry.index()), value);
        if (entry.index() > max_index) max_index = entry.index();
    }
    
    batch.Put("sys:lastLogIndex", std::to_string(max_index));
    db_->Write(rocksdb::WriteOptions(), &batch);
    last_log_index_cache_ = max_index;
}

// Used when a Follower conflicts with Leader logs
void RocksDBStore::truncateLog(int start_index) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    
    for (int i = start_index; i <= last_log_index_cache_; ++i) {
        db_->Delete(rocksdb::WriteOptions(), formatLogKey(i));
    }
    
    last_log_index_cache_ = start_index - 1;
    db_->Put(rocksdb::WriteOptions(), "sys:lastLogIndex", std::to_string(last_log_index_cache_));
}

// --- 4. SNAPSHOTS & COMPACTION ---

// A. Create Snapshot: Dumps "acct:" and "req:" keys to a string
RocksDBStore::SnapshotData RocksDBStore::createSnapshot() {
    std::lock_guard<std::mutex> lock(db_mutex_);
    
    SnapshotData snap;
    snap.last_index = getLastAppliedIndex();
    
    // Get last term from log, or from metadata if log was compacted
    raft::LogEntry lastEntry = getLogEntry(snap.last_index);
    snap.last_term = lastEntry.term();

    // We use a simple text format: "KEY\nVALUE\nKEY\nVALUE..."
    // In production, use Protobuf, but this is easy to debug.
    std::ostringstream oss;
    
    // 1. Scan DB
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        
        // Only include State Machine (acct) and Idempotency (req)
        // Exclude logs and metadata
        if (key.rfind("acct:", 0) == 0 || key.rfind("req:", 0) == 0) {
            oss << key << "\n" << it->value().ToString() << "\n";
        }
    }
    
    snap.data = oss.str();
    return snap;
}

// B. Apply Snapshot: Replaces State Machine
void RocksDBStore::applySnapshot(const std::string& snapshot_data, int last_index, int last_term) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    
    rocksdb::WriteBatch batch;
    
    // 1. Clear existing State Machine (Slow but safe)
    // Real RocksDB uses DeleteRange, here we iterate.
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        if (key.rfind("acct:", 0) == 0 || key.rfind("req:", 0) == 0) {
            batch.Delete(key);
        }
    }
    
    // 2. Parse and Insert New Data
    std::istringstream iss(snapshot_data);
    std::string key, value;
    while (std::getline(iss, key) && std::getline(iss, value)) {
        batch.Put(key, value);
    }
    
    // 3. Update System Metadata
    batch.Put("sys:lastApplied", std::to_string(last_index));
    batch.Put("sys:snapshotIndex", std::to_string(last_index));
    batch.Put("sys:snapshotTerm", std::to_string(last_term));
    
    db_->Write(rocksdb::WriteOptions(), &batch);

    if (DevFlags::LOG_STORAGE)
    {
        spdlog::info("[Storage] Snapshot Applied. State machine restored up to Index {}", last_index);
    }
    
}

// C. Compact Log: Deletes OLD logs [0 ... compact_index]
void RocksDBStore::compactLog(int compact_index) {
    std::lock_guard<std::mutex> lock(db_mutex_);

    if (DevFlags::LOG_STORAGE)
    {
        spdlog::info("[Storage] Compacting Log up to index {}...", compact_index);
    }
    
    
    for (int i = 0; i <= compact_index; ++i) {
        db_->Delete(rocksdb::WriteOptions(), formatLogKey(i));
    }
    
    // We don't change last_log_index_cache_ because HEAD hasn't moved.
    // We just deleted the tail.
}

// --- 5. STATE MACHINE ACCESS ---

int RocksDBStore::getBalance(const std::string& account_id) {
    std::string val;
    if (db_->Get(rocksdb::ReadOptions(), "acct:" + account_id, &val).ok()) {
        return std::stoi(val);
    }
    return 0; 
}

int RocksDBStore::getRequestIndex(const std::string& request_id) {
    std::string val;
    if (db_->Get(rocksdb::ReadOptions(), "req:" + request_id, &val).ok()) {
        raft::CommandResult res;
        res.ParseFromString(val);
        return res.executed_at_index();
    }
    return 0;
}

std::string RocksDBStore::getRequestResult(const std::string& request_id) {
    std::string val;
    if (db_->Get(rocksdb::ReadOptions(), "req:" + request_id, &val).ok()) {
        raft::CommandResult res;
        res.ParseFromString(val);
        return res.result_message();
    }
    return "";
}

void RocksDBStore::applyTransaction(const raft::LogEntry& entry,  int sender_new_bal, int receiver_new_bal, std::string result_msg) {
    std::lock_guard<std::mutex> lock(db_mutex_);
    rocksdb::WriteBatch batch;

    const auto& cmd = entry.command();
    
    if (sender_new_bal >= 0) {
        batch.Put("acct:" + cmd.sender_id(), std::to_string(sender_new_bal));
    }
    if (receiver_new_bal >= 0) {
        batch.Put("acct:" + cmd.receiver_id(), std::to_string(receiver_new_bal));
    }

    // 2. Save Result (Idempotency)
    raft::CommandResult res;
    res.set_executed_at_index(entry.index());
    res.set_result_message(result_msg);
    std::string res_str;
    res.SerializeToString(&res_str);
    batch.Put("req:" + entry.command().request_id(), res_str);
    
    // 3. Update Last Applied
    batch.Put("sys:lastApplied", std::to_string(entry.index()));
    
    db_->Write(rocksdb::WriteOptions(), &batch);
}