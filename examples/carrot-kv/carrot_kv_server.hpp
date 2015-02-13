#ifndef CARROT_KV_SERVER_H_
#define CARROT_KV_SERVER_H_

#include <map>
#include <memory>
#include <functional>
#include <unordered_map>
#include <raftcore/raftcore.h>

#include <boost/asio.hpp>

#include "carrot_kv_connection_manager.hpp"
#include "carrot_kv_reply.hpp"

namespace carrot{
namespace kv {

using namespace boost::asio::ip;

class server {
public:
    server(const server&) = delete;
    server& operator=(const server&) = delete;

    void on_log_entry_committed(const raftcore::log_entry *);

    explicit server(std::string bind_ip, std::string port, std::string raft_conf_file = "raft.conf");

    int init();
    void start();
    void stop();
private:
    void raft_thread();
    void do_accept();
    void handle_accept(const boost::system::error_code & ec);
    void handle_command(connection_sptr c, const command & cmd);

    boost::asio::io_service ios_;
    tcp::acceptor           acceptor_;
    connection_manager      manager_;
    raftcore::raft          raft_;
    raftcore::ios_work_uptr work_;
    tcp::socket             socket_;
    std::thread             raft_thread_;
    std::unordered_map<std::string, std::string> m_;
    std::unordered_map<uint64_t, connection_sptr> connection_map;
};
}
}
#endif