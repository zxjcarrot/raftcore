//
// server.cpp
// ~~~~~~~~~~
//
// Copyright (c) 2003-2014 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include "server.hpp"
#include <signal.h>
#include <utility>

namespace http {
namespace server {

server::server(boost::asio::io_service& io_service,
      const std::string& address, const std::string& port,
      const std::string& doc_root)
  : io_service_(io_service),
    acceptor_(io_service_),
    connection_manager_(),
    socket_(io_service_),
    dispatcher_(),
    address_(address),
    port_(port),
    doc_root_(doc_root)
{}

void server::register_handler(const std::string& uri, request_handler handler)
{
  dispatcher_.register_handler(uri, handler);
}

void server::start()
{
  // Open the acceptor with the option to reuse the address (i.e. SO_REUSEADDR).
  boost::asio::ip::tcp::resolver resolver(io_service_);
  boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve({address_, port_});
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen();

  do_accept();

}

void server::do_accept()
{
  acceptor_.async_accept(socket_,
      [this](boost::system::error_code ec)
      {
        // Check whether the server was stopped by a signal before this
        // completion handler had a chance to run.
        if (!acceptor_.is_open())
        {
          return;
        }

        if (!ec)
        {
          connection_manager_.start(std::make_shared<connection>(
              std::move(socket_), connection_manager_, dispatcher_));
        }

        do_accept();
      });
}

void server::stop() {
  acceptor_.close();
  connection_manager_.stop_all();
}

} // namespace server
} // namespace http
