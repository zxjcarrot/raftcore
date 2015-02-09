#include <raftcore/core_log.h>
#include <iostream>
#include <iomanip>

using namespace raftcore;

int main(int argc, char const *argv[]) {
    if (argc != 2) {
        std::cout << "usage: ./log_reader <log-file-path>" << std::endl;
        std::cout << "commands: show | append <log entry content> | chop <log entry idx>" << std::endl;
        return -1;
    }

    std::unique_ptr<core_logger> logger = std::unique_ptr<core_logger>(new core_logger(argv[1]));

    if (logger->init() != RC_GOOD) {
        std::cerr << "failed to initialize logger" << std::endl;
        return -1;
    }

    std::string command;
    while(std::cin >> command) {
        if (command == "show") {
            std::cout << std::setw(19) << "DataLen" << std::setw(19) << "Index" << std::setw(19) << "Term" << std::setw(7) << "config" << "\tData" << std::endl;
            for (uint64_t i = logger->first_entry_idx(); i <= logger->last_entry_idx(); ++i) {
                log_entry * e = (*logger)[i];
                std::cout << std::setw(19) << LOG_ENTRY_DATA_LEN(e)
                << std::setw(19) << e->idx
                << std::setw(19) << e->term << "\t"
                << std::setw(7) << e->cfg
                << std::string(e->data, LOG_ENTRY_DATA_LEN(e)) 
                << std::endl;;
            }
        } else if (command == "append") {
            std::string content;
            std::cin >> content;
            log_entry_sptr new_entry = make_log_entry(content.size());
            new_entry->idx = logger->last_entry_idx() + 1;
            new_entry->term = logger->last_entry_term();
            ::memcpy(new_entry->data, content.c_str(), LOG_ENTRY_DATA_LEN(new_entry));
            if (logger->append(new_entry, true) != RC_GOOD) {
                glogger::l(ERROR, "failed to append new_entry: %s", LOG_ENTRY_TO_STRING(new_entry).c_str());
            }
        } else if (command == "chop") {
            std::string sindex;
            std::cin >> sindex;
            uint64_t idx = std::stoi(sindex);
            if (logger->chop(idx, true) != RC_GOOD) {
                glogger::l(ERROR, "failed to chop entry at index %lld", idx);
            }
        }
    }
    return 0;
}