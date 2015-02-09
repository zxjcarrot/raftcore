/*
* Copyright (C) Xinjing Cho
*/
#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <cstring>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <raftcore/core_simple_logger.h>
#include <raftcore/core_log.h>
#include <raftcore/core_config.h>

namespace raftcore {

    rc_errno core_config::do_parse(std::istream & in) {
        int lineno = 0;
        while (true){
            std::string line;

            std::getline(in, line);
            ++lineno;
            if (line.empty())
                break;
            else if (line[0] == '#')
                continue;

            char * p = strtok((char *)line.data(), "=");

            if (p == NULL) {
                glogger::l(ERROR, "syntax error at line %d, no key=value pair found", lineno);
                return RC_CONF_ERROR;
            }

            std::string key(p);

            p = strtok(NULL, "=");

            if (p == NULL) {
                glogger::l(ERROR, "syntax error at line %d, no '=' found", lineno);
                return RC_CONF_ERROR;
            }

            std::string value(p);

            p = strtok(NULL, "=");
            
            if (p != NULL) {
                glogger::l(ERROR, "syntax error at line %d: at most one \'=\' could appear on each line.", lineno);
                return RC_CONF_ERROR;
            }

            conf_map_.insert(std::pair<std::string, std::string>(key, value));
        }

        inited_ = true;

        return RC_GOOD;
    }

    rc_errno core_config::parse() {
        if (inited_)
            return RC_GOOD;
        
        std::ifstream ifs(cfg_file_.c_str(), std::ifstream::in);

        if (!ifs.is_open()) {
            glogger::l(FATAL, "failed to open configuration file %s", cfg_file_.c_str());
            return RC_ERROR;
        }
        
        return do_parse(ifs);
    }
}