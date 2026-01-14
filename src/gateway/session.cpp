#include "gateway/session.h"

void ClientSession::start() {
    // 1. Tell the connection to read the next message
    // We pass a lambda that calls 'onRequestReceived' when data arrives
    auto self(shared_from_this());
    
    connection_->readMessage(
        [this, self](const std::vector<char>& payload) {
            onRequestReceived(payload);
        });
}

void ClientSession::onRequestReceived(const std::vector<char>& payload) {
    auto self(shared_from_this());

    // 2. Parse the Raw Bytes -> Protobuf Object
    client::ClientRequest req;
    if (!req.ParseFromArray(payload.data(), payload.size())) {
        std::cerr << "[Session] Error: Failed to parse ClientRequest." << std::endl;
        connection_->close(); // Disconnect invalid clients
        return;
    }

    std::cout << "[Session] Received Request ID: " << req.request_id() << std::endl;

    // 3. Define the Callback (What happens when Raft finishes?)
    // This lambda captures 'self' so the session stays alive while Raft works
    auto on_raft_complete = [this, self](client::ClientResponse resp) {
        
        // 4. Send the result back to the client
        // We use the generic 'send' template we wrote earlier
        connection_->send(resp);

        // Optional: Loop back to start() if you want to support multiple requests 
        // per connection (Keep-Alive). For now, let's keep it simple.
        // start(); 
    };

    // 5. Hand off to Raft
    // We assume RaftNode has a method: processClientRequest(req, callback)
    raft_node_.processClientRequest(req, on_raft_complete);
}