/*
* Copyright (C) Xinjing Cho
*/
#ifndef RAFTCORE_CORE_LOG_H_
#define RAFTCORE_CORE_LOG_H_

#include <string>
#include <vector>
#include <cstdint>
#include <memory>

#include <sys/mman.h>

#include <raftcore/core_define.h>
#include <raftcore/core_utils.h>
#include <raftcore/core_filemap.h>

namespace raftcore {

/* 0 len for a log_entry indicates the end of the log file */
const uint64_t log_end_marker = 0;

#define LOG_ENTRY_DATA_LEN(l) ((l)->data_len)


struct log_entry {
    uint64_t     len;       /* length of this entry including @len and padding */
    uint64_t     idx;       /* index of this entry */
    uint64_t     term;      /* term to which this entry belongs */
    bool         cfg;       /* if this is a configuration entry */
    uint64_t     data_len;  /* length of @data */
    /* 
    * if this is a configuration entry, then the first eight bytes
    * of @data stores the index of the previous configuration entry,
    * 0 for the first configuration entry.
    * Otherwise @data stores plain log data
    */
    char         data[];
};


#define LOG_ENTRY_DATA(e)\
    ((e)->cfg ? std::string((e)->data + sizeof(uint64_t), LOG_ENTRY_DATA_LEN(e) - sizeof(uint64_t)) : std::string((e)->data, LOG_ENTRY_DATA_LEN(e)))

#define LOG_ENTRY_TO_STRING(e)\
    ("[len: " + std::to_string((e)->len) + ", idx: " + std::to_string((e)->idx) +\
   ", term: " + std::to_string((e)->term) + ", cfg: " + std::to_string((e)->cfg) +\
   ", data: '" + LOG_ENTRY_DATA(e) + "']")

#define LOG_ENTRY_SENTINEL log_entry{sizeof(struct log_entry), 0, 0, 0}
#define LOG_ENTRY_ALIGNMENT 8

typedef std::shared_ptr<log_entry> log_entry_sptr;

/* make a log entry holding (@data_len + sizeof(log_entry)) bytes */
log_entry_sptr make_log_entry(uint64_t data_len, bool is_config = false);

#define RAFTCORE_LOGMAP_FILE_PROT   PROT_WRITE | PROT_READ
#define RAFTCORE_LOGMAP_FILE_FLAGS  MAP_SHARED
                                            /* one for LOG_ENTRY_SENTINEL, one for log_end_marker */
#define RAFTCORE_LOGFILE_DEFAULT_SIZE (sizeof(struct raftcore::log_entry) + sizeof(log_end_marker))

/* metadata for every log entry, always in memory */
struct log_entry_meta {
    /* index of the log entry with which this metadata is associated */
    uint64_t idx;
    /* size of log entries up to @idx(exclusive) */
    uint64_t prefix_sum;
    log_entry_meta(uint64_t index, uint64_t sum): idx(index), prefix_sum(sum)
        {}
};

class core_logger: public noncopyable {
public:
    core_logger(std::string logfile, double growing_factor = 1.25);
    core_logger(int fd, double growing_factor = 1.25);

    /* read entries from log file, do proper initializations. */
    rc_errno init();

    /* dumps whole log to a string for debugging */
    std::string debug_log_string();

    /*
    * append entries to the log.
    */
    rc_errno append(log_entry_sptr entry, bool sync = false);
    rc_errno append(const std::vector<log_entry_sptr> & entries, bool sync = false);

    /*
    * erase entries starting at @idx and all that follow it.
    */
    rc_errno chop(uint64_t idx, bool sync = false);

    uint64_t payload() { return payload_size_; }
    uint64_t size() { return size_; }

    /* return # of entries in the log.*/
    uint64_t entries() { return metas_.size(); }

    log_entry* operator[] (uint64_t i);

    /* check if there is a log with index @idx and term @term */
    bool has_log_entry(uint64_t idx, uint64_t term);
    /* check if log entry at @idx has a different term other than @term */
    bool log_entry_conflicted(uint64_t idx, uint64_t term);

    /* return current configuration data */
    std::string config();

    void copy_log_data(log_entry* entry, const std::string & data, bool if_config = false);

    log_entry* config_entry();

    uint64_t cfg_entry_idx() {
        return cfg_entry_idx_;
    }
    
    log_entry* first_entry() {
        return static_cast<log_entry*>((void*)(((char *)log_map_.addr()) + metas_.front().prefix_sum));
    }

    uint64_t first_entry_idx() {
        return first_entry()->idx;
    }

    uint64_t first_entry_term() {
        return first_entry()->term;
    }

    log_entry* last_entry() {
        return static_cast<log_entry*>((void*)(((char *)log_map_.addr()) + metas_.back().prefix_sum));
    }

    uint64_t last_entry_idx() {
        return last_entry()->idx;
    }

    uint64_t last_entry_term() {
        return last_entry()->term;
    }

    uint64_t last_entry_len() {
        return last_entry()->len;
    }

    uint64_t last_entry_data_len() {
        return LOG_ENTRY_DATA_LEN(last_entry());
    }

private:
    rc_errno grow_log();

    /* file name of the log file */
    std::string                 logfile_;
    /* underlying log file */
    core_filemap                log_map_;
    /* prefix sum of length of log entries */
    std::vector<log_entry_meta> metas_;
    /* total size of the log entries */
    uint64_t                    payload_size_;
    /* total size of the log */
    uint64_t                    size_;
    /* growing factor of log file */
    double                      growing_factor_;
    /* current configuration entry index, 0 if the log is bootstrapping */
    uint64_t                    cfg_entry_idx_;
};

}
#endif