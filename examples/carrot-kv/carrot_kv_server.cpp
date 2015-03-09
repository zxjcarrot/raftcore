#include <sstream>
#include <fstream>
#include "carrot_kv_server.hpp"

namespace carrot {
namespace kv {
server::server(std::string bind_ip, std::string port,
               std::string raft_conf_file)
    : acceptor_(ios_), 
      raft_(raft_conf_file, std::bind(&server::on_log_entry_committed, this, std::placeholders::_1)),
      socket_(ios_),
      prev_snapshot_size_(0),
      snapshot_fd_(-1),
      snapshotting_out_(false)
{

  std::cerr << bind_ip << " " << port << std::endl;
  boost::asio::ip::tcp::resolver resolver(ios_);
  boost::asio::ip::tcp::endpoint endpoint = *resolver.resolve({bind_ip, port});
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen();

  do_accept();
}

server::~server() {
    ::close(snapshot_fd_);
}

void server::on_log_entry_committed(const raftcore::log_entry * entry) {
    auto it = connection_map.find(entry->idx);
    if (it == connection_map.end()) {
        //std::cerr << "carrot-kv: connection not found at index "
        //          << entry->idx << std::endl;
        std::string type;
        std::string key;
        std::string value;
        std::istringstream input(std::move(std::string(entry->data, LOG_ENTRY_DATA_LEN(entry))));

        input >> type;

        if (type == "set"){
            input >> key;
            input.get();
            std::getline(input, value);
            //std::cerr << "carrot-kv: restoring: applying 'set [" << key << "] [" << value << "]' to the hashtable" << std::endl;
            std::lock_guard<std::mutex> g(mmtx_);
            m_[key] = value;
        } else if (type == "del") {
            input >> key;
            //std::cerr << "carrot-kv: restoring: applying 'del [" << key << "]' on the hashtable" << std::endl;
            std::lock_guard<std::mutex> g(mmtx_);
            m_.erase(key);
        }
    } else {
        connection_sptr c = it->second;
        command & cmd = c->cmd();

        switch(cmd.type) {
        case get:
        break;
        case carrot::kv::set: {
            std::lock_guard<std::mutex> g(mmtx_);
            auto it = m_.find(cmd.key);
            std::string old_value;

            if (it == m_.end())
                old_value = "";
            else{
                old_value = std::move(it->second);
            }
            //std::cerr << "carrot-kv: entry committed: applying 'set [" << cmd.key << "] [" << cmd.value << "]' to the hashtable" << std::endl;
            m_[cmd.key] = cmd.value;
            c->start_write(reply_status_ok, old_value);
        }
        break;
        case carrot::kv::del:{
            std::lock_guard<std::mutex> g(mmtx_);
            auto it = m_.find(cmd.key);
            std::string old_value;

            if (it == m_.end())
                old_value = "";
            else{
                old_value = std::move(it->second);
            }
            //std::cerr << "carrot-kv: entry committed: applying 'del [" << cmd.key << "]' on the hashtable" << std::endl;
            m_.erase(cmd.key);
            c->start_write(reply_status_ok, "");
        }
        break;
        case carrot::kv::unknown:
        break;
        };
        connection_map.erase(it);
    }
}

void server::handle_command(connection_sptr c, const command & cmd) {
    switch(cmd.type) {
    case carrot::kv::get :{
        auto it = m_.find(cmd.key);
        if (it == m_.end())
            c->start_write(reply_status_ok, "null");
        else
            c->start_write(reply_status_ok, it->second);
    }
    break;
    case carrot::kv::set: {
        if (raft_.current_role() != raftcore::LEADER){
            c->start_write(reply_status_not_leader, raft_.current_leader());
        } else {
            std::cerr << "carrot-kv: " << "set " + cmd.key + " " + cmd.value << std::endl;
            uint64_t index = raft_.append_log_entry("set " + cmd.key + " " + cmd.value);
            if (index == 0)
                c->start_write(reply_status_internal_error, "");
            else
                connection_map.insert(std::make_pair(index, c));
        }
    }
    break;
    case carrot::kv::del:{
        if (raft_.current_role() != raftcore::LEADER){
            c->start_write(reply_status_not_leader, raft_.current_leader());
        } else {
            std::cerr << "carrot-kv: " << "del " + cmd.key << std::endl;
            uint64_t index = raft_.append_log_entry("del " + cmd.key);
            if (index == 0)
                c->start_write(reply_status_internal_error, "");
            else
                connection_map.insert(std::make_pair(index, c));
        }
    }
    break;
    case carrot::kv::unknown:
    break;
    }
}

int server::update_snapshot_size() {
    struct stat st;

    snapshot_fd_ = ::open(SNAPSHOT_FILE, RC_MMAP_FILE_FLAGS | O_CREAT, RC_MMAP_FILE_MODE);

    if (snapshot_fd_ < 0){
        std::cerr << "carrot-kv: failed to open snapshot file " << SNAPSHOT_FILE
                  << " " << ::strerror(errno) << std::endl;
        return RC_ERROR;
    }

    if (::fstat(snapshot_fd_, &st) == -1) {
        std::cerr << "carrot-kv: failed to fstat() on file " << SNAPSHOT_FILE
                  << " " << ::strerror(errno) << std::endl;
        return RC_ERROR;
    }

    std::cerr << "carrot-kv: updated prev_snapshot_size_ from " << prev_snapshot_size_
              << " to " << st.st_size << std::endl;;
    prev_snapshot_size_ = st.st_size;

    ::close(snapshot_fd_);
    return RC_GOOD;
}

int server::init() {
    if (update_snapshot_size() != RC_GOOD || raft_.init() != RC_GOOD)
        return -1;
    return 0;
}

void server::raft_thread() {
    raft_.start();
}

void server::take_snapshot() {
    FILE * f = fopen(SNAPSHOT_FILE, "w+");

    if (f == nullptr) {
        std::cerr << "carrot-kv-snapshotting-process: failed to open snapshot file " << SNAPSHOT_FILE << std::endl;
        exit(-1);
    }

    std::cerr << "carrot-kv-snapshotting-process: taking a snapshot of current system." << std::endl;

    for (auto & x : m_)
        fprintf(f, "%s %s\n", x.first.c_str(), x.second.c_str());

    fclose(f);

}

void server::snapshotting_thread() {
    while(snapshotting_out_ == false) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        std::cerr << "carrot-kv: prev_snapshot_size_: " << prev_snapshot_size_
                      << ", raft_.log_size() : " << raft_.log_size()  << std::endl;; 
        while(prev_snapshot_size_ * 4 >= raft_.log_size()){
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        int cpid;
        {
            /* waits for other threads to complete their mutations */
            std::lock_guard<std::mutex> g(mmtx_);
            switch(cpid = ::fork()) {
                case -1:
                    std::cerr << "carrot-kv: failed to fork snapshotting process "
                              << ::strerror(errno) << std::endl;
                    exit(-1);
                break;
                case 0:
                    /* child */
                    take_snapshot();

                    /* 
                     * simply replace program image to avoid problems of
                     * forking in a multithreaded program since we do
                     * nothing more than taking snapshot. 
                     */ 
                    ::close(STDIN_FILENO);
                    ::close(STDOUT_FILENO);
                    ::close(STDERR_FILENO);
                    execlp("echo", "echo", NULL);
                    std::cerr << "carrot-kv-snapshotting-process: failed to execl(\"echo\", NULL);"
                              << ::strerror(errno) << std::endl;
                    exit(-1);
                break;
                default:
                    /* parent */
                break;
            }
        }
        if (cpid > 0) {
            int stat_loc;

            if (::wait(&stat_loc) == -1){
                std::cerr << "carrot-kv: failed to wait for snapshotting process[" << cpid
                          << "] to terminate, " << ::strerror(errno) << std::endl;
                exit(-1);
            }

            update_snapshot_size();
        }
    }
}

void server::start() {
    raft_thread_ = std::thread(&server::raft_thread, this);

    snapshotting_out_ = false;
    snapshotting_thread_ = std::thread(&server::snapshotting_thread, this);

    work_.reset(new boost::asio::io_service::work(ios_));
    do_accept();
    try {
        ios_.run();
    } catch (std::exception & e) {
        std::cerr << "carrot-kv: " << e.what() << std::endl;
    }
}


void server::stop() {
    snapshotting_out_ = true;
    ios_.stop();
    raft_.stop();
    raft_thread_.join();
    snapshotting_thread_.join();
}

void server::do_accept() {
    acceptor_.async_accept(socket_,
        boost::bind(&server::handle_accept,this,
            boost::asio::placeholders::error));
}

void server::handle_accept(const boost::system::error_code & ec) {
    if (!ec) {
        manager_.start(std::make_shared<connection>(
            std::move(socket_), manager_, std::bind(&server::handle_command, this, std::placeholders::_1, std::placeholders::_2)));
    } else {
        std::cerr << ec.message() << std::endl;
    }
    do_accept();
}

}
}