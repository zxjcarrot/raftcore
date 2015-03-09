/*
* Copyright (C) Xinjing Cho
*/

#ifndef RAFTCORE_DEFINE_H_
#define RAFTCORE_DEFINE_H_

#include <cstdint>
#include <cstddef>
#include <cerrno>
#include <string>
#include <cassert>

#include <unistd.h>
#include <netinet/in.h>

#define RATCORE_PAGESIZE 4096

enum rc_errno{
    RC_NOT_LEADER = -11,
    RC_UNKNOWN_HOST,
    RC_CONF_ERROR,
    RC_MMAP_NEW_FILE,
    RC_MMAP_NOT_MAPPED,
    RC_MMAP_ALREADY_MAPPED,
    RC_MMAP_INVALID_FILE,
    RC_MMAP_ERROR,
    RC_OOR, /* out of range */
    RC_OOM, /* out of memory */
    RC_ERROR,
    RC_GOOD
};

#endif