/*
* Copyright (C) Xinjing Cho
*/

#ifndef RAFTCORE_CONFIG_H_
#define RAFTCORE_CONFIG_H_

#include <istream>
#include <string>
#include <map>

#include <raftcore/core_define.h>

namespace raftcore {

    class core_config {
    public:

        core_config(std::string cfg_filename):cfg_file_(cfg_filename), inited_(false) {}

        /*
        * Parse the configuration file and fill up fields of this class.
        */
        rc_errno parse();

        std::string * get(const char * key) { 
            std::map<std::string, std::string>::iterator it = conf_map_.find(key);
            if (it == conf_map_.end())
                return nullptr;

            return &it->second;
        }

        void set(const char * key, const char * value) { conf_map_[key] = value; }

        void set(const std::string & key, const char * value) { conf_map_[key] = value; }

        void set(const char * key, const std::string & value) { conf_map_[key] = value; }

        void set(const std::string & key, const std::string & value) { conf_map_[key] = value; }

        const std::string cfg_file() { return cfg_file_; }
    private:
        rc_errno do_parse(std::istream & in);

        std::string                         cfg_file_;
        std::map<std::string, std::string>  conf_map_;
        bool                                inited_;
    };

}

#endif