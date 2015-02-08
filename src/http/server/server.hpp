//
// server.hpp
// ~~~~~~~~~~
//
// Copyright (c) 2003-2014 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef HTTP_SERVER_HPP
#define HTTP_SERVER_HPP

#include <boost/asio.hpp>
#include <string>
#include "utils.hpp"
#include "mime_types.hpp"
#include "reply.hpp"
#include "request.hpp"
#include "connection.hpp"
#include "connection_manager.hpp"
#include "dispatcher.hpp"

namespace http {
namespace server {

/// The top-level class of the HTTP server.
class server
{
public:
  server(const server&) = delete;
  server& operator=(const server&) = delete;

  /// Construct the server to listen on the specified TCP address and port, and
  /// serve up files from the given directory.
  explicit server(boost::asio::io_service& io_service,
      const std::string& address, const std::string& port = "80",
      const std::string& doc_root = ".");

  void register_handler(const std::string& uri, request_handler handler);

  /// start the server
  void start();

  void stop();
private:
  /// Perform an asynchronous accept operation.
  void do_accept();

  /// The io_service used to perform asynchronous operations.
  boost::asio::io_service& io_service_;
  /// Acceptor used to listen for incoming connections.
  boost::asio::ip::tcp::acceptor acceptor_;

  /// The connection manager which owns all live connections.
  connection_manager connection_manager_;

  /// The next socket to be accepted.
  boost::asio::ip::tcp::socket socket_;

  /// Dispatch request to handler based on uri
  dispatcher dispatcher_;

  std::string address_;
  std::string port_;
  std::string doc_root_;
};

} // namespace server
} // namespace http

#endif // HTTP_SERVER_HPP
