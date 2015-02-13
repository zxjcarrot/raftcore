#ifndef CARROT_KV_CONNECTION_MANAGER_H_
#define CARROT_KV_CONNECTION_MANAGER_H_
#include <set>
#include "carrot_kv_connection.hpp"

namespace carrot {
namespace kv {

class connection_manager {
public:
    connection_manager(const connection_manager&) = delete;
    connection_manager& operator=(const connection_manager&) = delete;

    connection_manager() {}


    void start(connection_sptr c) {
        connections.insert(c);
        c->start_read();
    }


    void stop(connection_sptr c) {
        connections.erase(c);
    }
private:  
    std::set<connection_sptr> connections;
};

}
}
#endif 