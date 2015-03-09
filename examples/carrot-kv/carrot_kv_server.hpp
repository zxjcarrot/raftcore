#ifndef CARROT_KV_SERVER_H_
#define CARROT_KV_SERVER_H_

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>

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

#define SNAPSHOT_FILE "carrot_kv.img"

class server {
public:
    server(const server&) = delete;
    server& operator=(const server&) = delete;

    void on_log_entry_committed(const raftcore::log_entry *);

    explicit server(std::string bind_ip, std::string port, std::string raft_conf_file = "raft.conf");
    ~server();

    int  init();
    void start();
    void stop();
private:
    void raft_thread();
    void snapshotting_thread();
    int  update_snapshot_size();
    void take_snapshot();
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
    std::thread             snapshotting_thread_;
    /* size of the previous snapshot */
    uint64_t                prev_snapshot_size_;
    int                     snapshot_fd_;
    bool                    snapshotting_out_;
    std::unordered_map<std::string, std::string> m_;
    /* mutex for the hash map */
    std::mutex              mmtx_;
    std::unordered_map<uint64_t, connection_sptr> connection_map;
};
}
}
#endif