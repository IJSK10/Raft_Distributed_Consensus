#pragma once

#include <vector>
#include <memory>
#include <mutex>
#include <condition_variable>
#include "client/raft_client.h"

class Benchmark {
public:
    Benchmark(const std::vector<std::unique_ptr<RaftClient>>& clients);
    
    // Runs for 'duration_seconds' and prints stats
    void run(int total_requests, double read_ratio);

private:

    const std::vector<std::unique_ptr<RaftClient>>& clients_;
    std::mutex stats_mutex_;
    std::condition_variable done_cv_;
    
    int completed_requests_ = 0;
    int failed_requests_ = 0;
    std::vector<double> latencies_;
};