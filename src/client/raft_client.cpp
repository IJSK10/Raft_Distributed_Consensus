#include "client/raft_client.h"
#include "utils/globals.h"
#include "spdlog/spdlog.h"
#include <boost/asio.hpp>
#include <thread>
#include <random>
#include <chrono>
#include <arpa/inet.h> 
#include <sys/socket.h>

using boost::asio::ip::tcp;

RaftClient::RaftClient(int id, const std::vector<std::pair<int, int>>& peers) 
    : id_(id), peers_(peers) {
    current_leader_index_ = rand() % peers_.size();
}

void RaftClient::sendCommand(const std::string& key, int value, const std::string& type, 
                             const std::string& sender, ResultCallback callback) {
    client::ClientRequest req;
    req.set_request_id(std::to_string(id_) + "-" + std::to_string(std::rand()));
    
    if (type == "TRANSFER") {
        req.set_type(client::ClientRequest::TRANSFER);
        req.set_sender_id(sender);
        req.set_receiver_id(key); 
        req.set_amount(value);
    } else {
        req.set_type(client::ClientRequest::GET_BALANCE);
        req.set_sender_id(key);
    }

    // Spawn a detached thread to handle this request asynchronously
    std::thread(&RaftClient::runCommand, this, req, callback).detach();
}

void RaftClient::runCommand(client::ClientRequest req, ResultCallback callback) {
    auto start_time = std::chrono::high_resolution_clock::now();
    int attempts = 0;
    int max_retries = 5;
    bool success = false;

    while (attempts < max_retries) {
        attempts++;
        int target_port = 0;
        int target_idx = 0;

        {
            std::lock_guard<std::mutex> lock(client_mutex_);
            target_idx = current_leader_index_;
            target_port = peers_[target_idx].second;
        }

        try {
            boost::asio::io_context io_context;
            tcp::socket socket(io_context);
            tcp::resolver resolver(io_context);
            
            // Connect
            boost::asio::connect(socket, resolver.resolve("127.0.0.1", std::to_string(target_port)));

            // Configure Socket Timeout (2 seconds)
            struct timeval tv;
            tv.tv_sec = 2;
            tv.tv_usec = 0;
            setsockopt(socket.native_handle(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            setsockopt(socket.native_handle(), SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

            // Send (Framing: 4-byte length header + Protobuf payload)
            std::string payload;
            req.SerializeToString(&payload);
            uint32_t net_len = htonl(payload.size());
            boost::asio::write(socket, boost::asio::buffer(&net_len, 4));
            boost::asio::write(socket, boost::asio::buffer(payload));

            // Receive (Framing)
            char header[4];
            boost::asio::read(socket, boost::asio::buffer(header, 4));
            uint32_t reply_len = ntohl(*reinterpret_cast<uint32_t*>(header));
            std::vector<char> reply_buf(reply_len);
            boost::asio::read(socket, boost::asio::buffer(reply_buf));

            client::ClientResponse resp;
            resp.ParseFromArray(reply_buf.data(), reply_len);

            if (resp.success()) {
                success = true;
                if (req.type() == client::ClientRequest::GET_BALANCE) {
                         // Print: [Client 1] Account 5: Balance: 100
                         spdlog::info("[Client {}] Account {}: {}", id_, req.sender_id(), resp.result());
                    } else {
                         spdlog::info("[Client {}] Transfer: {}", id_, resp.result());
                    }
                break; // Success! Exit loop.
            }

            // Handle Leader Redirect Hint
            if (resp.leader_hint() != -1) {
                std::lock_guard<std::mutex> lock(client_mutex_);
                for (size_t i = 0; i < peers_.size(); ++i) {
                    if (peers_[i].first == resp.leader_hint()) {
                        current_leader_index_ = i;
                        break;
                    }
                }
            } else {
                 // No hint, just try next peer
                 std::lock_guard<std::mutex> lock(client_mutex_);
                 current_leader_index_ = (current_leader_index_ + 1) % peers_.size();
            }

        } catch (std::exception& e) {
            // Connection failed, try next peer
            std::lock_guard<std::mutex> lock(client_mutex_);
            current_leader_index_ = (current_leader_index_ + 1) % peers_.size();
        }

        // Wait a bit before retrying
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // Measure Latency
    auto end_time = std::chrono::high_resolution_clock::now();
    double latency = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    if (!success && DevFlags::LOG_CLIENT) {
        spdlog::error("[Client {}] Request Failed after retries.", id_);
    }

    // Invoke Callback (Only if provided, e.g., in Benchmark Mode)
    if (callback) {
        callback(success, latency);
    }
}