#ifndef CARROT_KV_CONNECTION_HPP
#define CARROT_KV_CONNECTION_HPP

#include <memory>
#include <array>
#include <string>
#include <functional>
#include <boost/asio.hpp>

#include "carrot_kv_command.hpp"
#include "carrot_kv_command_parser.hpp"

namespace carrot {
namespace kv {

using namespace boost::asio::ip;

class connection;
class connection_manager;

typedef std::shared_ptr<connection> connection_sptr;

typedef std::function<void(connection_sptr, const command&)> command_handler;

class connection:
    public std::enable_shared_from_this<connection> {
public:
    connection(const connection&) = delete;
    connection& operator=(const connection&) = delete;

    explicit connection(tcp::socket socket, connection_manager & manager, command_handler handler);

    void start_write(const std::string & status, const std::string & data);
    void start_read();

    command & cmd() { return cmd_; }
private:
    
    void discard_line();

    void handle_read(const boost::system::error_code & ec,
                     std::size_t bytes_transferred);
    void handle_write(const boost::system::error_code & ec,
                     std::size_t bytes_transferred);
    void parse_command();

    char*                   pos_;
    char*                   end_;
    std::array<char, 8192>  buffer_;
    command                 cmd_;
    std::string             resp_data_;
    tcp::socket             socket_;
    connection_manager&     manager_;
    command_handler         command_handler_;
    command_parser          parser_;
};


}
}

#endif