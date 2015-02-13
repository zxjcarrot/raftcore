#ifndef CARROT_KV_COMMAND_PARSER_HPP_
#define CARROT_KV_COMMAND_PARSER_HPP_

#include <tuple>
#include "carrot_kv_command.hpp"

namespace carrot {
namespace kv {

class command_parser {
public:
    enum result_type {
        good,
        bad,
        need_more
    };

    /* reset parser to initial state */
    void reset();

    template <typename InputIterator>
    std::tuple<result_type, InputIterator> 
        parse(command & cmd, InputIterator begin, InputIterator end)
    {
        result_type result = need_more;
        while (begin != end) {
            result = do_parse(cmd, *begin++);
            if (result != need_more)
                return std::make_tuple(result, begin);
        }
        return std::make_tuple(result, end);
    }


private:

    result_type do_parse(command & cmd, char c);

    enum state {
        start,
        type_g,
        type_ge,
        type_s,
        type_se,
        type_set,
        type_d,
        type_de,
        type_done,
        read_key,
        read_key_value_key,
        read_key_value_blank,
        read_key_value_value,
        carriage
    } state_;
    static const char* state_strings[];
};

}
}

#endif