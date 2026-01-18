#include "gateway/session.h"
#include "client.pb.h"

Session::Session(boost::asio::ip::tcp::socket socket, RaftNode& node)
    : node_(node) {
    connection_ = TcpConnection::create(std::move(socket));
}

void Session::start() {
    readLoop();
}

void Session::readLoop() {
    auto self(shared_from_this());

    // Use the framer to get a full message
    connection_->readMessage([this, self](const std::vector<char>& data) {
        
        client::ClientRequest req;
        if (!req.ParseFromArray(data.data(), data.size())) {
            std::cerr << "Error parsing request." << std::endl;
            connection_->close();
            return;
        }

        // Pass to Raft Node
        node_.processClientRequest(req, [this, self](client::ClientResponse resp) {
            // Send response back using framing
            connection_->send(resp);
            // Keep reading for next request
            readLoop();
        });
    });
}