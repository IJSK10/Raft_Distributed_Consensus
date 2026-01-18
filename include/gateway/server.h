#pragma once

#include <boost/asio.hpp>
#include "raft/node.h" 


class GatewayServer {
public:
    GatewayServer(boost::asio::io_context& io_context, int port, RaftNode& node);

private:
    void startAccept();

    boost::asio::ip::tcp::acceptor acceptor_;
    RaftNode& node_;
};