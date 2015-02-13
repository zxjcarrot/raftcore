#include <iostream>
#include <memory>

#include "carrot_kv_server.hpp"

int main(int argc, char const *argv[]) {
    carrot::kv::server server("0.0.0.0", "8877");
    if (server.init() == -1) {
        exit(-1);
    } else {
        server.start();
    }
    return 0;
}