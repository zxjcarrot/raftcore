#ifndef CARROT_KV_COMMAND_HPP
#define CARROT_KV_COMMAND_HPP

#include <boost/asio.hpp>

namespace carrot {
namespace kv {

typedef enum {
    get,
    set,
    del,
    unknown
} command_type;

struct command {
    void reset()
    {
        key.clear();
        value.clear();
        type = unknown;
    }

    std::string  key;
    std::string  value;
    command_type type;
};

}
}
#endif