#pragma once

#include <memory>
#include <iostream>
#include "network/connection.h" // Shared TCP wrapper
#include "raft/node.h"          // The backend logic

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(boost::asio::ip::tcp::socket socket, RaftNode& node);
    void start();

private:
    void readLoop();
    TcpConnectionPtr connection_;
    RaftNode& node_;
};