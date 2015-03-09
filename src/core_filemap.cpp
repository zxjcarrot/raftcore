/*
* Copyright (C) Xinjing Cho
*/
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <raftcore/core_simple_logger.h>
#include <raftcore/core_define.h>
#include <raftcore/core_filemap.h>

namespace raftcore {

rc_errno core_filemap::map() {
    bool just_created = false;

    if (mapped_)
        return RC_MMAP_ALREADY_MAPPED;

    if (filename_.empty() && fd_ < 0) {
        LOG_ERROR << "failied in map: filename is empty and file descriptor not specified."; 
        return RC_MMAP_INVALID_FILE;
    }

    if (fd_ < 0) {
        fd_ = ::open(filename_.c_str(), RC_MMAP_FILE_FLAGS, RC_MMAP_FILE_MODE);

        if (fd_ == -1) {
            if (errno == ENOENT) {
                fd_ = ::open(filename_.c_str(), RC_MMAP_FILE_FLAGS | O_CREAT, RC_MMAP_FILE_MODE);

                if (fd_ == -1) {
                    LOG_ERROR << "failed to create file " << filename_ << "\"" << glogger::strerror_s(errno);
                    return RC_MMAP_INVALID_FILE;
                }
                /* align at page size */
                size_ = (size_ + RATCORE_PAGESIZE - 1) & ~(RATCORE_PAGESIZE - 1);
                ftruncate(fd_, size_);
                just_created = true;
            }
        }
    } else {
        #ifndef F_GETPATH
            filename_ = "unknown file";
        #else
            char filepath[1024];
            if (::fcntl(fd_, F_GETPATH, filepath) == -1) {
                LOG_ERROR << "failed to get file path of fd " << fd_ << "\"" << glogger::strerror_s(errno);
                return RC_MMAP_ERROR;
            }
            filename_ = filepath;
        #endif 

        
    }

    if (size_ == RC_MMAP_WHOLE_FILE) {
        struct stat st;
        if (::fstat(fd_, &st) == -1) {
            LOG_ERROR << "failed to fstat() on file " << filename_ << "\"" << glogger::strerror_s(errno);
            return RC_MMAP_ERROR;
        }

        size_ = st.st_size;

        /* align at page size */
        size_ = (size_ + RATCORE_PAGESIZE - 1) & ~(RATCORE_PAGESIZE - 1);
        
        /* at least one byte of file contents needed to be mapped to memory */
        if (size_ == 0) {
            size_ = if_empty_size_;
            /* align at page size */
            size_ = (size_ + RATCORE_PAGESIZE - 1) & ~(RATCORE_PAGESIZE - 1);
            ftruncate(fd_, size_);
        }
    }

    addr_ = ::mmap(hint_, size_, prot_, flags_, fd_, off_);

    if (addr_ == MAP_FAILED) {
        LOG_ERROR << "failed to mmap() " << glogger::strerror_s(errno);
        return RC_MMAP_ERROR;
    }

    mapped_ = true;

    /* fill with zeros if this file is just created */
    if (just_created) {
        memset(addr_, 0, size_);
        return RC_MMAP_NEW_FILE;
    }

    return RC_GOOD;
}

rc_errno core_filemap::unmap() {
    if (!mapped_)
        return RC_GOOD;
    LOG_INFO << "unmmapping : " << addr_ << " , " << size_;
    if(::munmap(addr_, size_) == -1) {
        LOG_ERROR << "failed to munmap() " << glogger::strerror_s(errno);
        return RC_MMAP_ERROR;
    }

    mapped_ = false;

    return RC_GOOD;
}

rc_errno core_filemap::remap(size_t new_size) {
    void* new_addr;

    /* align at page size */
    new_size = (new_size + RATCORE_PAGESIZE - 1) & ~(RATCORE_PAGESIZE - 1);

    if (!mapped_) {
        LOG_ERROR << "the file is not yet mapped";
        return RC_MMAP_NOT_MAPPED;
    }
    
    #ifdef HAVE_MREMAP
        new_addr = ::mremap(addr_, size_, new_size, MREMAP_MAYMOVE);

        if (new_addr == MAP_FAILED) {
            LOG_ERROR << "failed to mremap() " << glogger::strerror_s(errno);
            return RC_MMAP_ERROR;
        }
    #else
        rc_errno r = unmap();

        mapped_ = false;
        
        if (r != RC_GOOD)
            return r;
        LOG_INFO << "ftruncate : " << fd_ << " , " << new_size;
        if (::ftruncate(fd_, new_size) == -1) {
            LOG_ERROR << "failed to ftruncate() " << glogger::strerror_s(errno);
            return RC_MMAP_ERROR;
        }

        LOG_INFO << "mmap : " << new_size << ", " << off_;
        new_addr = ::mmap(hint_, new_size, prot_, flags_, fd_, off_);

        if (new_addr == MAP_FAILED) {
            LOG_ERROR << "failed to mmap() " << glogger::strerror_s(errno);

            if (errno == ENOMEM)
                return RC_OOM;
            else
                return RC_MMAP_ERROR;
        }

        mapped_ = true;
    #endif

    addr_ = new_addr;
    size_ = new_size;

    return RC_GOOD;
}

rc_errno core_filemap::sync_all() {
    return sync_range(addr_, size_);
}

rc_errno core_filemap::sync_range(void* addr, size_t len) {
    if (!mapped_) {
        LOG_ERROR << "the file is not yet mapped";
        return RC_MMAP_NOT_MAPPED;
    }
    uint64_t left = (uint64_t)addr & (RATCORE_PAGESIZE - 1);
    void * aligned_addr = reinterpret_cast<void*>((uint64_t)addr - left);
    /* msync requires @addr must be a multiple of hardware page size */
    if (::msync(aligned_addr, len + left, MS_SYNC) == -1){
        LOG_ERROR << "failed to msync() " << glogger::strerror_s(errno);
        return RC_MMAP_ERROR;
    }

    return RC_GOOD;
}

rc_errno core_filemap::advise(int advise) {
    if (!mapped_) {
        LOG_ERROR << "the file is not yet mapped";
        return RC_MMAP_NOT_MAPPED;
    }

    if (::madvise(addr_, size_, advise) == -1) {
        LOG_ERROR << "failed to madvise() " << glogger::strerror_s(errno);
        return RC_MMAP_ERROR;
    }

    return RC_GOOD;
}

}