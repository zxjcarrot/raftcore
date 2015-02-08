/*
* Copyright (C) Xinjing Cho
*/
#ifndef RAFTCORE_CORE_FILEMAP_H_
#define RAFTCORE_CORE_FILEMAP_H_

#include <string>

#include <raftcore/core_utils.h>

namespace raftcore {

#define RC_MMAP_FILE_MODE      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH
#define RC_MMAP_FILE_FLAGS     O_RDWR
#define RC_MMAP_WHOLE_FILE     0
#define RC_MMAP_EMPTY_FILE_DEFAULT_SIZE 10

class core_filemap: public noncopyable {
public:
    core_filemap(std::string filename, int prot, int flags,
                  size_t size = RC_MMAP_WHOLE_FILE, size_t if_empty_size = RC_MMAP_EMPTY_FILE_DEFAULT_SIZE,
                  off_t off = 0, void* hint = nullptr):
                  filename_(filename), fd_(-1), 
                  size_(size), if_empty_size_(if_empty_size), off_(off), prot_(prot),
                  flags_(flags), hint_(hint), mapped_(false)
        {}

    core_filemap(int fd, int prot, int flags, 
                  size_t size = RC_MMAP_WHOLE_FILE, size_t if_empty_size = RC_MMAP_EMPTY_FILE_DEFAULT_SIZE,
                  off_t off = 0, void* hint = nullptr):
                  filename_(), fd_(fd), size_(size), if_empty_size_(if_empty_size), 
                  off_(off), prot_(prot),
                  flags_(flags), hint_(hint), mapped_(false)
        {}

    /*
    * open the file(create if not exist) and bring it into memory.
    * Return: RC_GOOD on success, 
    *         RC_MMAP_INVALID_FILE if the file specified in constructor(either by fd or filename) is not valid
    *         RC_MMAP_ERROR or RC_ERROR on failure.
    */
    rc_errno map();

    /*
    * unmmap the file.
    * Return: RC_GOOD on success, RC_MMAP_ERROR on failure.
    */
    rc_errno unmap();

    /*
    * expands (or shrinks) the file memory mapping.
    * Return: RC_GOOD on success, RC_MMAP_ERROR or RC_ERROR on failure.
    */
    rc_errno remap(size_t new_size);

    /*
    * flush all modified data in the file to disk.
    * Return: RC_GOOD on success, RC_MMAP_ERROR on failure.
    */
    rc_errno sync_all();

    /*
    * flush all modified data whthin a range specified by @addr and @len to disk.
    */
    rc_errno sync_range(void* addr, size_t len);

    /* tells os about the intended usage patterns of the memory region */
    rc_errno advise(int advise);

    size_t  size() { return size_; }

    int     fd() { return fd_; }

    std::string filename() { return filename_; }

    void* addr() { return addr_; }
private:
    std::string filename_;
    int         fd_;
    size_t      size_;
    /* if the file being mapped does not exist or is empty in size, expand it to @if_empty_size_ size. */
    size_t      if_empty_size_;
    off_t       off_;
    int         prot_;
    int         flags_;
    void*       hint_;
    bool        mapped_;
    void*       addr_;
};

}
#endif