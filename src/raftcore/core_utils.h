/*
* Copyright (C) Xinjing Cho
*/
#ifndef RAFTCORE_UTILS_H_
#define RAFTCORE_UTILS_H_
#include <string>
#include <mutex>
#include <memory.h>
#include <memory>

#include <core_config.h>
#include <core_queue.h>

namespace raftcore {

#define RATCORE_PAGESIZE 4096

#define IFNULL(expr, replacement) ((expr) == nullptr ? (replacement) : (expr)) 

#define REVERSE_BYTE_ORDER64(ll) (((ll) >> 56) |         \
                    (((ll) & 0x00ff000000000000) >> 40) |\
                    (((ll) & 0x0000ff0000000000) >> 24) |\
                    (((ll) & 0x000000ff00000000) >> 8)  |\
                    (((ll) & 0x00000000ff000000) << 8)  |\
                    (((ll) & 0x0000000000ff0000) << 24) |\
                    (((ll) & 0x000000000000ff00) << 40) |\
                    (((ll) << 56)))

#define HTON64(ll) (little_endian() ? REVERSE_BYTE_ORDER64((ll)) : (ll))

#define NTOH64(ll) (HTON64(ll))


/* convenient base class for noncopyable objects */
class noncopyable {
public:
    noncopyable() { }
private:
    noncopyable(const noncopyable&);             // no implementation
    noncopyable& operator=(const noncopyable&);  // no implementation
};

enum severity {
    TRACE,
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    FATAL
};

class log_string {
public:
    log_string(int size, severity level = DEBUG)
        : p_(new char[size]), level_(level)
        {}

    char* get()
    {
        return p_.get();
    }

    severity level()
    {
        return level_;
    }
private:
    std::unique_ptr<char[]> p_;
    severity level_;
};

typedef std::shared_ptr<log_string> log_string_sptr;

/* simple global logging utility */
class glogger: public noncopyable {
public:
    /* spits @message to stderr */
    static void l(severity level, const std::string fmt, ...);
    static void l(severity level, const char * fmt, ...);
    
    /* spits @message to stderr width perror()'s result appended */
    static void l_perror(severity level, int errno_copy, const std::string fmt, ...);
    static void l_perror(severity level, int errno_copy, const char* fmt, ...);
    static void output_thread();

    static std::string strerror_s(int errno_copy);
    static void output(severity level, const char* fmt, va_list list);
    static thread_safe_queue<log_string_sptr> entries;
};

/* similar to sprintf, using std::string instead of char buffer */
std::string string_format(const std::string fmt_str, ...);

/*
* tests whether @str is a ip address(xxx.xxx.xxx.xxx).
*/
bool is_valid_ipv4_address(std::string str);

/*
* query ipv4 address of a particular interface.
*/
std::string get_if_ipv4_address(std::string ifn);

/* if machine byte order is little endian*/
bool little_endian();

/* split a string by @delimiter */
std::vector<std::string> split(const std::string & src, const std::string & delimiter);
}
#endif