#include "network/connection.h"
#include <iostream>

void TcpConnection::readMessage(std::function<void(const std::vector<char>&)> on_receive) {
    auto self(shared_from_this());

    // Step 1: Read exactly 4 bytes (The Header)
    boost::asio::async_read(socket_, boost::asio::buffer(header_buffer_, 4),
        [this, self, on_receive](boost::system::error_code ec, std::size_t /*length*/) {
            if (ec) {
                // If EOF, connection closed cleanly. Otherwise, error.
                if (ec != boost::asio::error::eof) {
                    std::cerr << "Read Header Failed: " << ec.message() << std::endl;
                }
                close();
                return;
            }

            // Step 2: Decode the size
            uint32_t net_len;
            std::memcpy(&net_len, header_buffer_, 4);
            uint32_t body_len = ntohl(net_len);

            // Safety check: Don't allocate 2GB if data is corrupt
            if (body_len > 10 * 1024 * 1024) { // Limit to 10MB
                std::cerr << "Error: Message too large (" << body_len << " bytes). Closing." << std::endl;
                close();
                return;
            }

            // Step 3: Resize buffer and read the body
            body_buffer_.resize(body_len);

            boost::asio::async_read(socket_, boost::asio::buffer(body_buffer_),
                [this, self, on_receive](boost::system::error_code ec, std::size_t /*length*/) {
                    if (ec) {
                        std::cerr << "Read Body Failed: " << ec.message() << std::endl;
                        close();
                        return;
                    }

                    // Step 4: Success! Call the user's callback
                    on_receive(body_buffer_);
                });
        });
}

void TcpConnection::close() {
    boost::system::error_code ec;
    socket_.shutdown(tcp::socket::shutdown_both, ec);
    socket_.close(ec);
}