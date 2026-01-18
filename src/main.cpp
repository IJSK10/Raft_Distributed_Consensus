#include <iostream>
#include <fstream>
#include <vector>
#include <thread>
#include <chrono>
#include <memory>
#include <atomic>
#include <sstream>
#include <iomanip>
#include <filesystem>
#include <nlohmann/json.hpp> 

#include "raft/node.h"
#include "storage/rocksdb_store.h"
#include "gateway/server.h"
#include "client/raft_client.h"
#include "benchmark/benchmark.h" // <--- Include Benchmark Header
#include "utils/globals.h"
#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

using json = nlohmann::json;

// --- CONFIG LOADING ---
struct NodeConfig {
    int id;
    int rpc_port;
    int tcp_port;
    std::string host;
};

// Global handles to keep objects alive
std::vector<std::shared_ptr<RaftNode>> nodes;
std::vector<std::unique_ptr<GatewayServer>> gateways;
std::vector<std::thread> gateway_threads;
std::vector<std::shared_ptr<boost::asio::io_context>> io_contexts;
std::vector<std::unique_ptr<RaftClient>> clients;

std::vector<NodeConfig> loadClusterConfig(const std::string& path) {
    std::ifstream f(path);
    if (!f.is_open()) {
        std::cerr << "Error: Could not open config file at " << path << std::endl;
        exit(1);
    }
    json data = json::parse(f);
    
    std::vector<NodeConfig> res;
    for (const auto& item : data["nodes"]) {
        res.push_back({
            item["id"],
            item["rpc_port"],
            item["tcp_port"],
            item.value("host", "127.0.0.1")
        });
    }
    return res;
}

void setupLogging() {
    auto console = spdlog::stdout_color_mt("console");
    spdlog::set_default_logger(console);
    spdlog::set_pattern("[%H:%M:%S.%e] %^[%l]%$ %v"); 
}

void runCsvMode(const std::string& filename) {
    spdlog::info("--- STARTING CSV EXECUTION ---");

    if (clients.empty()) {
        spdlog::error("CRITICAL: No clients initialized! Check config/cluster.json or main() logic.");
        return;
    }

    std::ifstream file(filename);
    if (!file.is_open()) {
        spdlog::error("Failed to open CSV file: {}", filename);
        return;
    }

    std::string line;
    int count = 0;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::stringstream ss(line);
        std::string cmd, arg1, arg2, arg3;
        
        std::getline(ss, cmd, ',');
        std::getline(ss, arg1, ',');
        std::getline(ss, arg2, ',');
        std::getline(ss, arg3, ',');

        // Round-robin load balancing for clients
        auto& client = clients[count % clients.size()];

        if (cmd == "TRANSFER") {
            // CSV Format: TRANSFER,sender_id,receiver_id,amount
            int amt = std::stoi(arg3);
            client->sendCommand(arg2, amt, "TRANSFER", arg1); 
        } else if (cmd == "GET") {
            // CSV Format: GET,user_id
            client->sendCommand(arg1, 0, "GET");
        }
        
        count++;
        // Small delay to ensure logs print nicely in order (optional)
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); 
    }
    spdlog::info("CSV Execution Complete. {} commands sent.", count);
    
    // Allow time for last async requests to finish
    std::this_thread::sleep_for(std::chrono::seconds(1));
}

// --- MAIN SIMULATION ---
int main() {

    setupLogging();

    // DYNAMIC CONFIG: Change these here without touching other files
    DevFlags::LOG_REPLICATION = false; 
    DevFlags::LOG_ELECTION = false;
    DevFlags::LOG_EXECUTION   = false;
    DevFlags::LOG_NETWORK     = false;
    DevFlags::LOG_CLIENT      = true;

    spdlog::info("--- STARTING RAFT CLUSTER ---");

    std::string config_path = "config/server_config.json";
    auto node_configs = loadClusterConfig(config_path);

    // 2. START CLUSTER
    spdlog::info("Starting Cluster with {} nodes...", node_configs.size());

    std::filesystem::create_directories("data");

    // 1. START NODES & GATEWAYS
    for (const auto& cfg : node_configs) {
        // A. Build Peer List for this Node
        std::vector<std::pair<int, std::string>> rpc_peers;
        for (const auto& peer : node_configs) {
            rpc_peers.push_back({peer.id, peer.host + ":" + std::to_string(peer.rpc_port)});
        }

        ClusterConfig raft_config;
        raft_config.my_id = cfg.id;
        raft_config.rpc_port = cfg.rpc_port; // Used for binding the gRPC server
        raft_config.tcp_port = cfg.tcp_port;
        raft_config.peers = rpc_peers;

        // B. Initialize Storage
        std::string db_path = "data/node_" + std::to_string(cfg.id);
        auto storage = std::make_shared<RocksDBStore>(db_path);
        storage->initializeGenesisData(); // Create accounts 1, 2, 3...

        // C. Start Raft Node
        auto node = std::make_shared<RaftNode>(raft_config, storage);
        node->start(); // Launches internal threads (Election, Replication, etc.)
        nodes.push_back(node);

        // D. Start Gateway (TCP Server)
        auto ioc = std::make_shared<boost::asio::io_context>();
        io_contexts.push_back(ioc);
        
        auto gateway = std::make_unique<GatewayServer>(*ioc, cfg.tcp_port, *node);
        gateways.push_back(std::move(gateway));

        // Run Gateway in its own thread (blocking call)
        gateway_threads.emplace_back([ioc]() {
            ioc->run();
        });

        std::cout << "[Main] Node " << cfg.id << " started (RPC:" << cfg.rpc_port 
                  << ", TCP:" << cfg.tcp_port << ")" << std::endl;
    }

    std::cout << "[Main] Waiting 3s for Leader Election..." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // 2. INITIALIZE CLIENTS
    // Each client needs the list of TCP ports to find the leader
    std::vector<std::pair<int, int>> tcp_peers;
    for (const auto& cfg : node_configs) {
        tcp_peers.push_back({cfg.id, cfg.tcp_port});
    }

    for (int i = 0; i < 5; ++i) {
        clients.push_back(std::make_unique<RaftClient>(i + 1, tcp_peers));
    }

    // 3. RUN SIMULATION SCENARIO
    std::cout << "\n--- STARTING COMMAND SIMULATION ---\n";

    // Scenario: 
    // Random clients sending Transfer and Get commands.
    // We send 10 commands in quick succession.
    
    // 4. SELECTION LOGIC
    std::cout << "\n================================\n";
    std::cout << "Select Operation Mode:\n";
    std::cout << "1. Run CSV Workload (Verification)\n";
    std::cout << "2. Run Benchmark (Performance)\n";
    std::cout << "Select [1 or 2]: ";
    
    int choice;
    std::cin >> choice;

    if (choice == 2) {
        // --- BENCHMARK MODE ---
        DevFlags::LOG_CLIENT = false;      // Silence logs for speed
        DevFlags::LOG_EXECUTION = false;   // Silence logs for speed

        int count;
        double ratio;
        std::cout << "Enter number of requests (e.g., 10000): ";
        std::cin >> count;
        std::cout << "Enter Read Ratio (0.0 to 1.0, e.g., 0.5): ";
        std::cin >> ratio;

        Benchmark bench(clients);
        bench.run(count, ratio);

        // Benchmark finished -> Exit immediately
        spdlog::info("Benchmark Complete. Shutting down.");
    } 
    else {
        // --- CSV / VERIFICATION MODE ---
        // 1. Run the file exactly once
        runCsvMode("config/workload.csv");

        // 2. Loop for manual verification
        std::string user_input;
        while (true) {
            std::cout << "\n[Verify] Enter User ID to check Balance (or 'exit'): ";
            std::cin >> user_input;
            
            if (user_input == "exit") {
                break;
            }

            std::cout << "\n--- Cluster Consistency Check for User " << user_input << " ---\n";
            std::cout << std::left << std::setw(10) << "Node" 
                      << std::setw(15) << "Role" 
                      << std::setw(15) << "Balance" 
                      << "Log Index" << std::endl;
            std::cout << "--------------------------------------------------------\n";

            for (const auto& node : nodes) {
                // Determine Role String
                std::string role_str;
                int state = node->getState();
                if (state == 0) role_str = "FOLLOWER";
                else if (state == 1) role_str = "CANDIDATE";
                else role_str = "LEADER";

                // Direct DB Read (Thread-safe via RocksDB)
                int bal = node->getStorage()->getBalance(user_input);
                int log_idx = node->getStorage()->getLastLogIndex();

                std::cout << std::left << std::setw(10) << node->getConfig().my_id 
                          << std::setw(15) << role_str 
                          << std::setw(15) << bal 
                          << log_idx << std::endl;
            }
            std::cout << "--------------------------------------------------------\n";

            // Sleep briefly so the async log output appears before the next prompt
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }
    }

    // 5. CLEANUP
    spdlog::info("Shutting down system...");

    // A. Stop Network (Gateways) - Prevents new requests
    for (auto& ioc : io_contexts) ioc->stop();
    for (auto& t : gateway_threads) if(t.joinable()) t.join();
    gateways.clear(); // Destroy Gateway objects

    // B. Stop Raft Nodes - Stops processing logic
    for (auto& node : nodes) node->stop();
    
    // C. Destroy Nodes - Forces destructors to run (closing RocksDB/gRPC)
    nodes.clear();
    clients.clear();

    return 0;
}