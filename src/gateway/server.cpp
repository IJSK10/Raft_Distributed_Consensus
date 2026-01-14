#include "gateway/server.h"
#include "gateway/session.h"
#include "network/connection.h"
#include <iostream>

GatewayServer::GatewayServer(boost::asio::io_context& io_context, short port, RaftNode& raft_node)
    : acceptor_(io_context, tcp::endpoint(tcp::v4(), port)), 
      io_context_(io_context),
      raft_node_(raft_node) {
    
    std::cout << "[Gateway] Listening on port " << port << "..." << std::endl;
    
    // Start the accept loop immediately
    do_accept();
}

void GatewayServer::do_accept() {
    // 1. Prepare to accept a new connection
    // The lambda function below is the "Callback" that runs when a user connects.
    acceptor_.async_accept(
        [this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) {
                std::cout << "[Gateway] New Client Connected!" << std::endl;

                // 2. Wrap the raw socket in our shared 'TcpConnection' class
                // This gives us the easy readMessage/send functions we wrote earlier.
                auto conn = TcpConnection::create(std::move(socket));

                // 3. Create a Session for this specific user
                // We pass the connection and the RaftNode backend.
                // make_shared creates the object, and ->start() kicks off the read loop.
                std::make_shared<ClientSession>(conn, raft_node_)->start();
            } else {
                std::cerr << "[Gateway] Accept failed: " << ec.message() << std::endl;
            }

            // 4. Loop: Go back to waiting for the NEXT user.
            do_accept();
        });
}