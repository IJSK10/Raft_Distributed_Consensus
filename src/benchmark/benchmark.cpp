#include "benchmark/benchmark.h"
#include "spdlog/spdlog.h"
#include <iostream>
#include <iomanip>
#include <numeric>
#include <algorithm>
#include <mutex>
#include <condition_variable>

// --- HELPER CLASS: Semaphore (Ensures compilation on all C++ versions) ---
class Semaphore {
public:
    Semaphore(int count) : count_(count) {}

    void acquire() {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return count_ > 0; });
        count_--;
    }

    void release() {
        std::lock_guard<std::mutex> lock(mutex_);
        count_++;
        cv_.notify_one();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int count_;
};
// ------------------------------------------------------------

Benchmark::Benchmark(const std::vector<std::unique_ptr<RaftClient>>& clients)
    : clients_(clients) {}

void Benchmark::run(int total_requests, double read_ratio) {
    spdlog::info("--- STARTING ASYNC BENCHMARK ---");
    spdlog::info("Target: {} Requests | Read Ratio: {:.0f}%", total_requests, read_ratio * 100);

    completed_requests_ = 0;
    failed_requests_ = 0;
    latencies_.clear();
    latencies_.reserve(total_requests);

    auto start_time = std::chrono::steady_clock::now();

    // Limit concurrency to 50 active threads to prevent OS resource exhaustion.
    Semaphore sem(50); 

    for (int i = 0; i < total_requests; ++i) {
        // Blocks here if 50 requests are currently in-flight
        sem.acquire(); 

        auto& client = clients_[i % clients_.size()];
        
        bool is_read = ((double)rand() / RAND_MAX) < read_ratio;
        std::string type = is_read ? "GET" : "TRANSFER";
        std::string key = std::to_string((rand() % 100) + 1); // Random User 1-100
        std::string key2 = std::to_string((rand() % 100) + 1); // Random User 1-100

        // Send Command with Callback
        client->sendCommand(key, 1, type, key2, 
            [this, &sem, total_requests](bool success, double latency) {
                
                // --- CALLBACK: Runs when request finishes ---
                {
                    std::lock_guard<std::mutex> lock(stats_mutex_);
                    latencies_.push_back(latency);
                    if (!success) failed_requests_++;
                    completed_requests_++;
                    
                    // Show progress every 10%
                    if (total_requests >= 10 && completed_requests_ % (total_requests / 10) == 0) {
                        spdlog::info("Progress: {}/{}", completed_requests_, total_requests);
                    }

                    // If this was the last request, wake up the main thread
                    if (completed_requests_ >= total_requests) {
                        done_cv_.notify_one();
                    }
                }

                // Important: Release slot for the next request
                sem.release(); 
            });
    }

    // Main thread waits here until all callbacks are done
    {
        std::unique_lock<std::mutex> lock(stats_mutex_);
        done_cv_.wait(lock, [this, total_requests] { 
            return completed_requests_ >= total_requests; 
        });
    }

    auto end_time = std::chrono::steady_clock::now();
    double duration_sec = std::chrono::duration<double>(end_time - start_time).count();

    // --- STATISTICS ---
    std::sort(latencies_.begin(), latencies_.end());
    
    double sum = std::accumulate(latencies_.begin(), latencies_.end(), 0.0);
    double avg = latencies_.empty() ? 0.0 : sum / latencies_.size();
    double p50 = latencies_.empty() ? 0.0 : latencies_[latencies_.size() * 0.50];
    double p99 = latencies_.empty() ? 0.0 : latencies_[latencies_.size() * 0.99];
    double throughput = (duration_sec > 0) ? completed_requests_ / duration_sec : 0.0;

    std::cout << "\n=========================================\n";
    std::cout << "          BENCHMARK RESULTS              \n";
    std::cout << "=========================================\n";
    std::cout << "Requests:     " << completed_requests_ << "\n";
    std::cout << "Failed:       " << failed_requests_ << "\n";
    std::cout << "Duration:     " << std::fixed << std::setprecision(2) << duration_sec << " s\n";
    std::cout << "Throughput:   " << throughput << " ops/sec\n";
    std::cout << "Latency Avg:  " << avg << " ms\n";
    std::cout << "Latency P50:  " << p50 << " ms\n";
    std::cout << "Latency P99:  " << p99 << " ms\n";
    std::cout << "=========================================\n";
}