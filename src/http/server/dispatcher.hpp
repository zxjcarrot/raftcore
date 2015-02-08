#ifndef HTTP_SERVER_DISPATCHER_HPP
#define HTTP_SERVER_DISPATCHER_HPP

#include <string>
#include <map>
#include <functional>

namespace http {
namespace server {

struct reply;
struct request;

typedef std::function<void(const request& req, reply& rep)> request_handler;

class dispatcher
{
public:
    dispatcher() {}
    dispatcher(const dispatcher&) = delete;
    dispatcher& operator=(const dispatcher&) = delete;

    void dispatch(const request& req, reply& rep);

    void register_handler(const std::string& uri, request_handler handler);

private:
    /// The handlers for all incoming requests.
    std::map<std::string, request_handler> handlers_;
};

}
}
#endif