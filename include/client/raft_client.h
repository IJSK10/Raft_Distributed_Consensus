#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <functional>
#include <thread>
#include "client.pb.h"

class RaftClient {
public:

    using ResultCallback = std::function<void(bool, double)>;

    RaftClient(int id, const std::vector<std::pair<int, int>>& peers);

    void sendCommand(const std::string& key, 
                     int value, 
                     const std::string& type, 
                     const std::string& sender = "1",
                     ResultCallback callback = nullptr);

private:
    void runCommand(client::ClientRequest req, ResultCallback callback);
    int id_;
    std::vector<std::pair<int, int>> peers_;

    // --- SHARED STATE ---
    std::mutex client_mutex_;
    int current_leader_index_;
};