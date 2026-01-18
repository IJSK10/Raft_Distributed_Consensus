#pragma once

#include <boost/asio.hpp>
#include <memory>
#include <vector>
#include <functional>
#include <iostream>
#include <cstring> 
#include <arpa/inet.h> // Use <winsock2.h> on Windows

using boost::asio::ip::tcp;

class TcpConnection;
typedef std::shared_ptr<TcpConnection> TcpConnectionPtr;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    // Factory method - keeps the object alive via shared_ptr
    static TcpConnectionPtr create(boost::asio::io_context& io_context) {
        return TcpConnectionPtr(new TcpConnection(io_context));
    }
    
    static TcpConnectionPtr create(tcp::socket socket) {
        return TcpConnectionPtr(new TcpConnection(std::move(socket)));
    }

    tcp::socket& socket() { return socket_; }

    // --- GENERIC SEND ---
    // Serializes ANY protobuf message, adds 4-byte header, and sends.
    template <typename ProtoMessage>
    void send(const ProtoMessage& msg) {
        // 1. Serialize Protobuf to string
        std::string payload;
        if (!msg.SerializeToString(&payload)) {
            std::cerr << "Error: Failed to serialize message." << std::endl;
            return;
        }

        // 2. Prepare Header (Network Byte Order)
        uint32_t msg_size = static_cast<uint32_t>(payload.size());
        uint32_t net_len = htonl(msg_size);

        // 3. Create a buffer holding [HEADER][PAYLOAD]
        // We use a shared_ptr<vector> to keep data alive during async_write
        auto buffer = std::make_shared<std::vector<char>>(sizeof(uint32_t) + msg_size);
        
        std::memcpy(buffer->data(), &net_len, sizeof(uint32_t));
        std::memcpy(buffer->data() + sizeof(uint32_t), payload.data(), msg_size);

        // 4. Async Write
        auto self(shared_from_this());
        boost::asio::async_write(socket_, boost::asio::buffer(*buffer),
            [this, self, buffer](boost::system::error_code ec, std::size_t /*length*/) {
                if (ec) {
                    std::cerr << "Write failed: " << ec.message() << std::endl;
                    close();
                }
            });
    }

    void readMessage(std::function<void(const std::vector<char>&)> on_receive);


    void close();

private:
    // Private constructors (force use of create())
    TcpConnection(boost::asio::io_context& io) : socket_(io) {}
    TcpConnection(tcp::socket socket) : socket_(std::move(socket)) {}

    tcp::socket socket_;
    
    // Fixed buffer for the size header (4 bytes)
    char header_buffer_[4];
    
    // Dynamic buffer for the payload
    std::vector<char> body_buffer_;
};