/*
* Copyright (C) Xinjing Cho
*/
#include <stdexcept>
#include <system_error>
#include <utility>

#include <raftcore/core_define.h>
#include <raftcore/core_log.h>

namespace raftcore {

log_entry_sptr make_log_entry(uint64_t data_len) {
    log_entry_sptr les = log_entry_sptr(static_cast<log_entry*>((void*)(new char[data_len + sizeof(log_entry)])));
    les->len = data_len + sizeof(log_entry);
    return les;
}

core_logger::core_logger(std::string logfile, double growing_factor):
    logfile_(logfile), 
    log_map_(logfile, RAFTCORE_LOGMAP_FILE_PROT, RAFTCORE_LOGMAP_FILE_FLAGS,0, RAFTCORE_LOGFILE_DEFAULT_SIZE),
    growing_factor_(growing_factor) {

    if (growing_factor < 1.0)
        throw std::domain_error("growing_factor must be >= 1.0");
}

core_logger::core_logger(int fd, double growing_factor):
    log_map_(fd, RAFTCORE_LOGMAP_FILE_PROT, RAFTCORE_LOGMAP_FILE_FLAGS, 0, RAFTCORE_LOGFILE_DEFAULT_SIZE),
    growing_factor_(growing_factor) {

    if (growing_factor < 1.0)
        throw std::domain_error("growing_factor must be >= 1.0");
}

/* read entries from log file, do proper initializations. */
rc_errno core_logger::init() {
    rc_errno rc = log_map_.map();

    if (rc == RC_MMAP_NEW_FILE){
        glogger::l(INFO, "first boot, fill in sentinel log entry");
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
        payload_size += pl->len;
        pl = static_cast<log_entry*>((void*)((char*)pl + pl->len));
    }

    size_ = log_map_.size();
    payload_size_ = payload_size;

    log_map_.advise(MADV_NORMAL);

    return RC_GOOD;
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
    while (s > RAFTCORE_LOG_AVAIL_SIZE) {
        if ((res = grow_log()) != RC_GOOD)
            return res;
    }

    uint64_t   pos = metas_.back().prefix_sum;
    log_entry* pre = static_cast<log_entry*>((void*)((char*)log_map_.addr() + pos));
    log_entry* next;

    /* write entries to mapping region and maintain metadatas at the same time */
    for(log_entry_sptr les : entries) {
        pos += pre->len;
        next = static_cast<log_entry*>((void*)((char *)pre + pre->len));
        
        ::memcpy((char*)next, (char*)les.get(), les->len);

        metas_.push_back({les->idx, pos});
        pre = next;
    }

    /* put the end marker right after last entry */
    next = static_cast<log_entry*>((void*)((char*)log_map_.addr() + metas_.back().prefix_sum));
    next = static_cast<log_entry*>((void*)((char*)next + next->len));
    next->len = log_end_marker;

    payload_size_ = metas_.back().prefix_sum;
    /* flush entries to disk */
    if (sync) {
        return log_map_.sync_range((char *)next - s, s + sizeof(log_end_marker));
    }

    return RC_GOOD;
}

rc_errno core_logger::chop(uint64_t idx, bool sync) {
    if (metas_.empty() || idx < metas_.front().idx || idx > metas_.back().idx)
        return RC_OOR;

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
    uint64_t new_size = size_ * growing_factor_;
    rc_errno res = log_map_.remap(new_size);

    if (res != RC_GOOD)
        return res;

    size_ = new_size;

    return res;
}

}