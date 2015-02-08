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
private:
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

}
#endif