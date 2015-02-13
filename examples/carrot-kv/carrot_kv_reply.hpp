#ifndef CARROT_KV_REPLY_H_
#define CARROT_KV_REPLY_H_

namespace carrot {
namespace kv {

#define reply_status_internal_error         "INTERNAL_ERROR"
#define reply_status_invalid_request_format "INVALID_FOMRAT"
#define reply_status_ok                     "OK"
#define reply_status_not_leader             "NOT_LEADER"

}
}
#endif