#include <cctype>
#include "carrot_kv_command_parser.hpp"

namespace carrot {
namespace kv {

const char* command_parser::state_strings[] = {
    "start",
    "type_g",
    "type_ge",
    "type_s",
    "type_se",
    "type_set",
    "type_d",
    "type_de",
    "type_done",
    "read_key",
    "read_key_value_key",
    "read_key_value_blank",
    "read_key_value_value",
    "carriage"
};

void command_parser::reset() {
    state_ = start;
}

command_parser::result_type command_parser::do_parse(command & cmd, char c) {
    switch (state_) {
    case start:
        if(isblank(c)){
            return need_more;
        } else if (c == 'g'){
            state_ = type_g;
            return need_more;
        } else if (c == 's'){
            state_ = type_s;
            return need_more;
        } else if (c == 'd') {
            state_ = type_d;
            return need_more;
        } else {
            return bad;
        }
    break;
    case type_g:
        if (c == 'e'){
            state_ = type_ge;
            return need_more;
        } else {
            return bad;
        }
    break;
    case type_ge:
        if (c == 't'){
            state_ = type_done;
            cmd.type = get;
            return need_more;
        } else {
            return bad;
        }
    break;
    case type_s:
        if (c == 'e'){
            state_ = type_se;
            return need_more;
        } else {
            return bad;
        }
    break;
    case type_se:
        if (c == 't'){
            state_ = type_set;
            cmd.type = set;
            return need_more;
        } else {
            return bad;
        }
    break;
    case type_set:
        if (isblank(c)) {
            return need_more;
        } else {
            cmd.key.push_back(c);
            state_ = read_key_value_key;
            return need_more;
        }
    break;
    case type_d:
        if (c == 'e'){
            state_ = type_de;
            return need_more;
        } else {
            return bad;
        }
    break;
    case type_de:
        if (c == 'l'){
            state_ = type_done;
            cmd.type = del;
            return need_more;
        } else {
            return bad;
        }
    break;
    case type_done:
        if (c == '\r' || c == '\n'){
            return bad;
        } else if (isblank(c)){
            return need_more;
        } else{
            cmd.key.push_back(c);
            state_ = read_key;
            return need_more;
        }
    break;
    case read_key_value_key:
        if (c == '\r' || c == '\n') {
            return bad;
        } else if (isblank(c)){
            state_ = read_key_value_blank;
            return need_more;
        } else {
            cmd.key.push_back(c);
            return need_more;
        }
    break;

    case read_key_value_blank:
        if (c == '\r' || c == '\n') {
            return bad;
        } else if (isblank(c)) {
            return need_more;
        } else {
            cmd.value.push_back(c);
            state_ = read_key_value_value;
            return need_more;
        }
    break;
    case read_key_value_value:
        if (c == '\r'){
            state_ = carriage;
            return need_more;
        } else {
            cmd.value.push_back(c);
            return need_more;
        }
    break;
    case read_key:
        if (c == '\r'){
            state_ = carriage;
            return need_more;
        } else {
            cmd.key.push_back(c);
            return need_more;
        }
    break;
    case carriage:
        if (c == '\n'){
            return good;
        } else {
            return bad;
        }
    break;
    }
    return bad;
}

}
}