/*
* Copyright (C) Xinjing Cho
*/
#include <stdexcept>
#include <system_error>
#include <utility>

#include <raftcore/core_simple_logger.h>
#include <raftcore/core_define.h>
#include <raftcore/core_log.h>

namespace raftcore {

log_entry_sptr make_log_entry(uint64_t data_len, bool is_config) {
    uint64_t total_len = sizeof(log_entry) + (is_config ? sizeof(uint64_t) : 0) + data_len;
    total_len = (total_len + LOG_ENTRY_ALIGNMENT - 1) & ~(LOG_ENTRY_ALIGNMENT - 1);
    log_entry_sptr les = log_entry_sptr(static_cast<log_entry*>((void*)(new char[total_len])));
    ::memset(les.get(), 0, total_len);
    les->len = total_len;
    les->data_len = data_len + (is_config ? sizeof(uint64_t) : 0);
    return les;
}

/* return current configuration data */
std::string core_logger::config() {
    log_entry * entry = config_entry();

    if (entry == nullptr)
        return std::string("");
    else                    /* skip first eight bytes */
        return std::string(entry->data + sizeof(uint64_t), LOG_ENTRY_DATA_LEN(entry) - sizeof(uint64_t));
}

/* return current configuration data */
log_entry* core_logger::config_entry() {
    try {
        LOG_INFO << "config_entry: cfg_entry_idx_ " << cfg_entry_idx_; 
        log_entry * entry = operator[](cfg_entry_idx_);

        return entry;
    } catch (std::out_of_range & oor) {
        return nullptr;
    }
    return nullptr;
}

void core_logger::copy_log_data(log_entry* entry, const std::string & data, bool if_config) {
    if (if_config) {
        uint64_t prev_cfg_idx = HTON64(cfg_entry_idx_);
        ::memcpy(entry->data, &prev_cfg_idx, sizeof(uint64_t));
        ::memcpy(entry->data + sizeof(uint64_t), data.c_str(), LOG_ENTRY_DATA_LEN(entry) - sizeof(uint64_t));
    } else {
        ::memcpy(entry->data, data.c_str(), LOG_ENTRY_DATA_LEN(entry));
    }
}

core_logger::core_logger(std::string logfile, double growing_factor):
    logfile_(logfile), 
    log_map_(logfile, RAFTCORE_LOGMAP_FILE_PROT, RAFTCORE_LOGMAP_FILE_FLAGS,0, RAFTCORE_LOGFILE_DEFAULT_SIZE),
    growing_factor_(growing_factor),
    cfg_entry_idx_(0) {

    if (growing_factor < 1.0)
        throw std::domain_error("growing_factor must be >= 1.0");
}

core_logger::core_logger(int fd, double growing_factor):
    log_map_(fd, RAFTCORE_LOGMAP_FILE_PROT, RAFTCORE_LOGMAP_FILE_FLAGS, 0, RAFTCORE_LOGFILE_DEFAULT_SIZE),
    growing_factor_(growing_factor),
    cfg_entry_idx_(0) {

    if (growing_factor < 1.0)
        throw std::domain_error("growing_factor must be >= 1.0");
}

/* read entries from log file, do proper initializations. */
rc_errno core_logger::init() {
    rc_errno rc = log_map_.map();

    if (rc == RC_MMAP_NEW_FILE){
        LOG_INFO << "first boot, fill in sentinel log entry";
        log_entry sentinel = LOG_ENTRY_SENTINEL;
        log_entry* pl = static_cast<log_entry*>(log_map_.addr());
        ::memcpy(pl, &sentinel, sentinel.len);
        log_map_.sync_all();
    } else if (rc != RC_GOOD) {
        return rc;
    }

    logfile_ = log_map_.filename();

    log_entry* pl = static_cast<log_entry*>(log_map_.addr());
    uint64_t   payload_size = 0;

    // advise the OS that we will be accessing the memory region in a sequential manner.
    log_map_.advise(MADV_SEQUENTIAL);

    /*
    * calculate prefix sum of length of log entries.
    */
    while (pl->len != log_end_marker) {
        metas_.push_back({pl->idx, payload_size});

        /* choose last configuration entry as current configuration */
        if (pl->cfg)
            cfg_entry_idx_ = pl->idx;
        
        payload_size += pl->len;

        pl = static_cast<log_entry*>((void*)((char*)pl + pl->len));
    }

    size_ = log_map_.size();
    LOG_INFO << "raftcore log size: " << log_map_.size();
    payload_size_ = payload_size;

    log_map_.advise(MADV_NORMAL);

    return RC_GOOD;
}

std::string core_logger::debug_log_string() {
    std::string s;
    log_entry* pl = static_cast<log_entry*>(log_map_.addr());

    // advise the OS that we will be accessing the memory region in a sequential manner.
    log_map_.advise(MADV_SEQUENTIAL);

    /*
    * calculate prefix sum of length of log entries.
    */
    while (pl->len != log_end_marker) {
        s.append(LOG_ENTRY_DATA(pl) + "\n");
        pl = static_cast<log_entry*>((void*)((char*)pl + pl->len));
    }

    log_map_.advise(MADV_NORMAL);

    return s;
}

log_entry* core_logger::operator[] (uint64_t i) {
    if (metas_.empty() || i < metas_.front().idx || i > metas_.back().idx)
        throw std::out_of_range("log entry index " + std::to_string(i) +
            " out of range. valid range: [" + std::to_string(metas_.front().idx) + "," +
            std::to_string(metas_.back().idx) + "]");

    log_entry_meta & m = metas_[i - metas_.front().idx];

    return static_cast<log_entry*>((void*)((char*)log_map_.addr() + m.prefix_sum));
}


bool core_logger::has_log_entry(uint64_t idx, uint64_t term) {
    try {
        log_entry * entry = operator[](idx);

        if (entry == nullptr || entry->idx != idx || entry->term != term)
            return false;
        
    } catch (std::out_of_range & oor) {
        return false;
    }

    return true;
}

/* 
* return true if log entry at @idx has a different term other than term.
* return false if the entry with index @idx does not exist yet.
*/
bool core_logger::log_entry_conflicted(uint64_t idx, uint64_t term) {
    try {
        log_entry * entry = operator[](idx);

        if (entry->term != term)
            return true;

    } catch (std::out_of_range & oor) {
        return false;
    }

    return false;
}
rc_errno core_logger::append(log_entry_sptr entry, bool sync) {
    return append(std::vector<log_entry_sptr>{entry}, sync);
}

rc_errno core_logger::append(const std::vector<log_entry_sptr> & entries, bool sync) {
    uint64_t   s = 0;
    rc_errno   res;

    /* calculate sum of length */
    for(log_entry_sptr les : entries) {
        s += les->len;
    }

    /* expand log file if necessary */
    while (size_ - payload_size_ - sizeof(log_end_marker) <= s) {
        if ((res = grow_log()) != RC_GOOD)
            return res;        
    }

    LOG_INFO << "appending log entires total size: " << s 
             << ", available size: " << size_ - payload_size_ - sizeof(log_end_marker)
             << ", payload_size_:" << payload_size_
             << ", metas_.back().prefix_sum: " << metas_.back().prefix_sum;

    uint64_t   pos = metas_.back().prefix_sum;
    log_entry* pre = static_cast<log_entry*>((void*)((char*)log_map_.addr() + pos));
    log_entry* next;

    /* write entries to mapping region and maintain metadatas at the same time */
    for(log_entry_sptr les : entries) {
        if (les->cfg)
            cfg_entry_idx_ = les->idx;

        pos += pre->len;
        next = static_cast<log_entry*>((void*)((char *)pre + pre->len));
        
        ::memcpy((char*)next, (char*)les.get(), les->len);
        metas_.push_back({les->idx, pos});
        pre = next;
    }

    /* put end marker right after the last entry */
    next = static_cast<log_entry*>((void*)((char*)log_map_.addr() + metas_.back().prefix_sum));
    next = static_cast<log_entry*>((void*)((char*)next + next->len));
    next->len = log_end_marker;

    payload_size_ += s;

    /* flush entries to disk */
    if (sync) {
        return log_map_.sync_range((char *)next - s, s + sizeof(log_end_marker));
    }



    return RC_GOOD;
}

rc_errno core_logger::chop(uint64_t idx, bool sync) {
    if (metas_.empty() || idx < metas_.front().idx || idx > metas_.back().idx)
        return RC_OOR;

    while (idx <= cfg_entry_idx_) {
        /* current configuration entry is being chopped off, rollback to previous one */
        log_entry * cfg_entry = config_entry();
        uint64_t prev_cfg_idx;
        ::memcpy(&prev_cfg_idx, cfg_entry->data, sizeof(uint64_t));
        cfg_entry_idx_ = NTOH64(prev_cfg_idx);
    }

    log_entry_meta & m = metas_[idx - metas_.front().idx];
    log_entry* e = static_cast<log_entry*>((void*)((char*)log_map_.addr() + m.prefix_sum));

    /* put down the end marker */
    e->len = log_end_marker;
    metas_.erase(metas_.begin() + idx, metas_.end());
    payload_size_ = m.prefix_sum;

    if (sync) {
        return log_map_.sync_range((char *)e, sizeof(log_end_marker));
    }

    return RC_GOOD;
}

rc_errno core_logger::grow_log() {
    rc_errno res = log_map_.remap(size_ * growing_factor_);

    if (res != RC_GOOD)
        return res;

    size_ = log_map_.size();

    return res;
}

}