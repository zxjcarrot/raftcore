#include <iostream>
#include <boost/bind.hpp>

#include "carrot_kv_reply.hpp"
#include "carrot_kv_connection.hpp"
#include "carrot_kv_connection_manager.hpp"

namespace carrot {
namespace kv {

connection::connection (tcp::socket socket, connection_manager & manager, command_handler handler)
    : socket_(std::move(socket)), manager_(manager), command_handler_(handler)
{
    
}

void connection::start_read() {
    socket_.async_read_some(boost::asio::buffer(buffer_),
        boost::bind(&connection::handle_read, shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));
}

void connection::start_write(const std::string & status, const std::string & data) {
    resp_data_ = std::move(std::string(std::move(status) + " " + data + "\r\n"));
    boost::asio::async_write(socket_, boost::asio::buffer(resp_data_.data(), resp_data_.size()),
        boost::bind(&connection::handle_write,shared_from_this(),
            boost::asio::placeholders::error,
            boost::asio::placeholders::bytes_transferred));
}

void connection::discard_line() {
    while(pos_ < end_  && *pos_++ != '\n');
}

void connection::parse_command() {
    command_parser::result_type result;

    std::tie(result, pos_) = parser_.parse(cmd_, pos_, end_);

    if (result == command_parser::good) {
        command_handler_(shared_from_this(), cmd_);
    } else if (result == command_parser::bad) {
        discard_line();
        start_write(reply_status_invalid_request_format, "");
    } else if (result == command_parser::need_more || pos_ == end_) {
        start_read();
    }
}

void connection::handle_read(const boost::system::error_code & ec,
                             std::size_t bytes_transferred)
{
    if (!ec) {
        pos_ = buffer_.data();
        end_ = buffer_.data() + bytes_transferred;
        parse_command();
    } else {
        std::cerr <<  ec.message() << std::endl;
        manager_.stop(shared_from_this());
    }
}

void connection::handle_write(const boost::system::error_code & ec,
                              std::size_t bytes_transferred)
{
    if (!ec) {
        parser_.reset();
        cmd_.reset();
        parse_command();
    } else {
        std::cerr <<  ec.message() << std::endl;
        manager_.stop(shared_from_this());
    }
}

}
}
