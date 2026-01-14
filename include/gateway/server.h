#pragma once

#include <boost/asio.hpp>
#include "raft/node.h" 

using boost::asio::ip::tcp;

class GatewayServer {
public:
    // We need the IO Context (event loop), the Port to listen on, and the Raft Node.
    GatewayServer(boost::asio::io_context& io_context, short port, RaftNode& raft_node);

private:
    void do_accept();

    tcp::acceptor acceptor_;
    boost::asio::io_context& io_context_;
    RaftNode& raft_node_;
};