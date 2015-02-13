#include <csignal>
#include <iostream>
#include <memory>

#include <raftcore/raftcore.h>

using namespace raftcore;

void on_committed(const raftcore::log_entry * l) {
    std::cout << "log: " << LOG_ENTRY_TO_STRING(l) << " committed" << std::endl;
}

int main(int argc, char const *argv[]){
    std::unique_ptr<raftcore::raft> r = std::unique_ptr<raftcore::raft>(new raftcore::raft("raft.conf", on_committed));

    if (r->init() != RC_GOOD) {
        std::cout << "failed in initializing raftcore" << std::endl;
        exit(1);
    }

    //::signal(SIGINT, on_sigint);

    r->start();

    std::cout << "exitting..." << std::endl;
    return 0;
}