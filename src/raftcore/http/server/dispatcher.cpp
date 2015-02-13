#include "dispatcher.hpp"
#include "mime_types.hpp"
#include "reply.hpp"
#include "request.hpp"


namespace http {
namespace server {


void dispatcher::register_handler(const std::string& uri, request_handler handler)
{
    handlers_.insert(std::make_pair(uri, handler));
}

void dispatcher::dispatch(const request& req, reply& rep)
{  

    /* get rid of the part being after the question mark */
    size_t question_mark_pos = req.uri.find_last_of('?');
    std::string uri_processed;
    
    if (question_mark_pos != std::string::npos){
        uri_processed = std::string(req.uri.begin(), req.uri.begin() + question_mark_pos);
    } else {
        uri_processed = req.uri;
    }

    auto it = handlers_.find(uri_processed);
    if (it == handlers_.end()){
        rep = reply::stock_reply(reply::not_found);
        return;
    }

    (it->second)(req, rep);
}

}
}