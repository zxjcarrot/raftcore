/*
* Copyright (C) Xinjing Cho
*/

#ifndef RAFTCORE_DEFINE_H_
#define RAFTCORE_DEFINE_H_

#include <cstdint>
#include <cstddef>
#include <cerrno>
#include <string>

#include <unistd.h>
#include <netinet/in.h>

enum rc_errno{
    RC_UNKNOWN_HOST = -10,
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