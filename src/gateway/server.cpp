#include "gateway/server.h"
#include "gateway/session.h"
#include "network/connection.h"
#include <iostream>

GatewayServer::GatewayServer(boost::asio::io_context& io_context, int port, RaftNode& node)
    : acceptor_(io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
      node_(node) {
    std::cout << "Gateway listening on port " << port << std::endl;
    startAccept();
}

void GatewayServer::startAccept() {
    acceptor_.async_accept(
        [this](boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
            if (!ec) {
                // Create and start a new Session for this connection
                std::make_shared<Session>(std::move(socket), node_)->start();
            }
            startAccept(); // Loop
        });
}