#include <memory.h>

#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <cstdarg>
#include <utility>
#include <memory>
#include <string>
#include <chrono>

#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/time.h>
#include <net/if.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <raftcore/core_simple_logger.h>
#include <raftcore/core_utils.h>

namespace raftcore {

void glogger::l(severity level, const char* fmt, ...) {
    va_list     list;

    va_start(list, fmt);
    output(level, fmt, list);
    va_end(list);
}

void glogger::l(severity level, const std::string fmt, ...) {
    va_list     list;

    va_start(list, fmt);
    output(level, fmt.c_str(), list);
    va_end(list);
}

void glogger::l_perror(severity level, int errno_copy, const std::string fmt, ...) {
    std::string s = fmt + strerror_s(errno_copy);
    va_list     list;

    va_start(list, fmt);
    output(level, s.c_str(), list);
    va_end(list);
}

void glogger::l_perror(severity level, int errno_copy, const char* fmt, ...) {
    std::string s = fmt + strerror_s(errno_copy);
    va_list     list;

    va_start(list, fmt);
    output(level, s.c_str(), list);
    va_end(list);
}

std::string glogger::strerror_s(int errno_copy) {
    #ifdef HAVE_STRERROR_R
        char errbuf[1024];
        if (strerror_r(errno_copy, errbuf, 1024) == -1) {
            return std::string("strerror_r failed with errno = " +
                                 std::to_string(errno_copy));
        }
        return std::string(errbuf);
    #else
        return std::string(" errno = " + std::to_string(errno_copy));
    #endif
}

thread_safe_queue<log_string_sptr> glogger::entries;

void glogger::output(severity level, const char* fmt, va_list list) {
    int32_t size = 256;

    while(1) {
        std::unique_ptr<char[]> buffer = std::unique_ptr<char[]>(new char[size]);
        if (vsnprintf(buffer.get(), size, fmt, list) >= size){
            size *= 2;
            continue;
        }
        switch(level) {
        case TRACE:
            BOOST_GLOBAL_LOG(trace) << buffer.get();
            break;
        case DEBUG:
            BOOST_GLOBAL_LOG(debug) << buffer.get();
            break;
        case INFO:
            BOOST_GLOBAL_LOG(info) << buffer.get();
            break;
        case WARNING:
            BOOST_GLOBAL_LOG(warning) << buffer.get();
            break;
        case ERROR:
            BOOST_GLOBAL_LOG(error) << buffer.get();
            break;
        case FATAL:
            BOOST_GLOBAL_LOG(fatal) << buffer.get();
            break;
        }
        break;
    }
}

std::string string_format(const std::string fmt_str, ...) {
    int final_n, n = ((int)fmt_str.size()) << 2; /* reserve 4 times as much as the length of the fmt_str */
    std::unique_ptr<char[]> formatted;
    va_list ap;

    while(1) {
        formatted.reset(new char[n]); /* wrap the plain char array into the unique_ptr */
        strcpy(&formatted[0], fmt_str.c_str());

        va_start(ap, fmt_str);
        final_n = vsnprintf(&formatted[0], n, fmt_str.c_str(), ap);
        va_end(ap);

        if (final_n < 0 || final_n >= n)
            n += abs(final_n - n + 1);
        else
            break;
    }
    return std::string(formatted.get());
}

bool is_valid_ipv4_address(std::string str) {
    std::unique_ptr<char[]> b = std::unique_ptr<char[]>(new char[str.size() + 1]);
    ::strcpy(b.get(), str.c_str());
    char *save_ptr;
    char *p = ::strtok_r(b.get(), ".", &save_ptr);
    int   cnt = 0, val;

    while (p) {
        ++cnt;
        val = std::stol(p);

        if (val > 255 || val < 0)
            return false;

        p = ::strtok_r(NULL, ".", &save_ptr);
    }

    return cnt == 4;
}

std::string get_if_ipv4_address(std::string ifn) {
    char         buf[30];
    struct ifreq ifr;
    int          sock = socket(AF_INET, SOCK_DGRAM, 0);

    strcpy(ifr.ifr_name, ifn.c_str());

    if (ioctl(sock, SIOCGIFADDR, &ifr) < 0) {
        LOG_ERROR << "faield in ioctl " << glogger::strerror_s(errno);
        ::close(sock);

        return std::string("");
    }

    if (inet_ntop(AF_INET, &(((struct sockaddr_in*)&(ifr.ifr_addr))->sin_addr), buf, sizeof(buf)) == NULL) {
        LOG_ERROR << "faield in inet_ntop " << glogger::strerror_s(errno);
        ::close(sock);

        return std::string("");   
    }

    return std::string(buf);
}

bool little_endian() {
    const uint16_t dummy = 0x0001;

    if (*((char*)&dummy))
        return true;
    
    return false;
}

std::vector<std::string> split(const std::string & src, const std::string & delimiter) {
    std::vector<std::string> v;
    size_t pos = 0;
    size_t found_pos = 0;

    while ((found_pos = src.find_first_of(delimiter, pos)) != std::string::npos){
        v.emplace_back(std::string(src.begin() + pos, src.begin() + found_pos));
        pos = found_pos + 1;
    }
    if (v.empty()) {
        v.emplace_back(src);
    } else {
        v.emplace_back(std::string(src.begin() + pos, src.end()));
    }
    return v;
}
}