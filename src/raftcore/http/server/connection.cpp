//
// connection.cpp
// ~~~~~~~~~~~~~~
//
// Copyright (c) 2003-2014 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include "connection.hpp"
#include <utility>
#include <vector>
#include <boost/bind.hpp>
#include "connection_manager.hpp"

namespace http {
namespace server {

using namespace boost::asio::local;
connection::connection(boost::asio::ip::tcp::socket socket,
    connection_manager& manager, dispatcher& dispatcher)
  : socket_(std::move(socket)),
    connection_manager_(manager),
    dispatcher_(dispatcher)
{
}

void connection::start()
{
  do_read();
}

void connection::stop()
{
  socket_.close();
}

void connection::do_read()
{
  auto self(shared_from_this());
  socket_.async_read_some(boost::asio::buffer(buffer_),
      [this, self](boost::system::error_code ec, std::size_t bytes_transferred)
      {
        if (!ec)
        {
          request_parser::result_type result;
          std::tie(result, std::ignore) = request_parser_.parse(
              request_, buffer_.data(), buffer_.data() + bytes_transferred);

          if (result == request_parser::good)
          {
            dispatcher_.dispatch(request_, reply_);
            if (reply_.ready_to_go){
              do_write();
            } else {

              if (reply_.read_side.get() == nullptr || !reply_.read_side->is_open() ||
                  reply_.write_side.get() == nullptr || !reply_.write_side->is_open()) {
                reply_.read_side = 
                  std::shared_ptr<stream_protocol::socket>(new stream_protocol::socket(socket_.get_io_service()));
                reply_.write_side = 
                  std::shared_ptr<stream_protocol::socket>(new stream_protocol::socket(socket_.get_io_service()));
                boost::asio::local::connect_pair(*reply_.read_side, *reply_.write_side);
              }

              reply_.read_side->async_read_some(boost::asio::buffer(&reply_.ready_to_go, sizeof(reply_.ready_to_go)),
                boost::bind(&connection::handle_ready_to_go, shared_from_this(),
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
            }
          }
          else if (result == request_parser::bad)
          {
            reply_ = reply::stock_reply(reply::bad_request);
            do_write();
          }
          else
          {
            do_read();
          }
        }
        else if (ec != boost::asio::error::operation_aborted)
        {
          connection_manager_.stop(shared_from_this());
        }
      });
}

void connection::handle_ready_to_go(const boost::system::error_code& error, // Result of operation.
                                    std::size_t bytes_transferred           // Number of bytes written.
)
{
  do_write();
}

void connection::do_write()
{
  auto self(shared_from_this());
  boost::asio::async_write(socket_, reply_.to_buffers(),
      [this, self](boost::system::error_code ec, std::size_t)
      {
        if (!ec)
        {
          // Initiate graceful connection closure.
          boost::system::error_code ignored_ec;
          socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both,
            ignored_ec);
        }

        if (ec != boost::asio::error::operation_aborted)
        {
          connection_manager_.stop(shared_from_this());
        }
      });
}

} // namespace server
} // namespace http
