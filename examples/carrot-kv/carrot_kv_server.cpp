#include <sstream>
#include "carrot_kv_server.hpp"

namespace carrot {
namespace kv {
server::server(std::string bind_ip, std::string port,
               std::string raft_conf_file)
    : acceptor_(ios_), 
      raft_(raft_conf_file, std::bind(&server::on_log_entry_committed, this, std::placeholders::_1)),
      socket_(ios_)
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

void server::on_log_entry_committed(const raftcore::log_entry * entry) {
    auto it = connection_map.find(entry->idx);
    if (it == connection_map.end()) {
        std::cerr << "connection not found at index "
                  << entry->idx << std::endl;
        std::string type;
        std::string key;
        std::string value;
        std::istringstream input(std::move(std::string(entry->data, LOG_ENTRY_DATA_LEN(entry))));

        input >> type;

        if (type == "set"){
            input >> key;
            input.get();
            std::getline(input, value);
            std::cerr << "restoring: applying 'set [" << key << "] [" << value << "]' to the hashtable" << std::endl;
            m_[key] = value;
        } else if (type == "del") {
            input >> key;
            std::cerr << "restoring: applying 'del [" << key << "]' on the hashtable" << std::endl;
            m_.erase(key);
        }
    } else {
        connection_sptr c = it->second;
        command & cmd = c->cmd();

        switch(cmd.type) {
        case get:
        break;
        case carrot::kv::set: {
            auto it = m_.find(cmd.key);
            std::string old_value;

            if (it == m_.end())
                old_value = "";
            else{
                old_value = std::move(it->second);
            }
            std::cerr << "entry committed: applying 'set [" << cmd.key << "] [" << cmd.value << "]' to the hashtable" << std::endl;
            m_[cmd.key] = cmd.value;
            c->start_write(reply_status_ok, old_value);
        }
        break;
        case carrot::kv::del:{
            auto it = m_.find(cmd.key);
            std::string old_value;

            if (it == m_.end())
                old_value = "";
            else{
                old_value = std::move(it->second);
            }
            std::cerr << "entry committed: applying 'del [" << cmd.key << "]' on the hashtable" << std::endl;
            m_.erase(cmd.key);
            c->start_write(reply_status_ok, "");
        }
        break;
        case carrot::kv::unknown:
        break;
        };
        connection_map.erase(it);
    }
    std::cerr << "hashtable content: " << std::endl;
    for (auto & x : m_) 
        std::cerr << x.first << ": " << x.second << std::endl;
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

int server::init() {
    return raft_.init() == RC_GOOD ? 0 : -1;
}

void server::raft_thread() {
    raft_.start();
}

void server::start() {
    raft_thread_ = std::thread(&server::raft_thread, this);

    work_.reset(new boost::asio::io_service::work(ios_));
    do_accept();
    try {
        ios_.run();
    } catch (std::exception & e) {
        std::cerr << e.what() << std::endl;
    }
}


void server::stop() {
    ios_.stop();
    raft_.stop();
    raft_thread_.join();
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