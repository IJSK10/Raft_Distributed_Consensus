#pragma once

#include <memory>
#include <iostream>
#include "network/connection.h" // Shared TCP wrapper
#include "raft/node.h"          // The backend logic
#include "client.pb.h"          // The Protobuf definitions

class ClientSession : public std::enable_shared_from_this<ClientSession> {
public:
    // Initialize with a ready-to-go TCP connection and the Raft backend
    ClientSession(TcpConnectionPtr conn, RaftNode& raft)
        : connection_(conn), raft_node_(raft) {}

    // Kicks off the read loop
    void start();

private:
    // Called when we receive raw bytes from the network
    void onRequestReceived(const std::vector<char>& payload);

    TcpConnectionPtr connection_;
    RaftNode& raft_node_;
};