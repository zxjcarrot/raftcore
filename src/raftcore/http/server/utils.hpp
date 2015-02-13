#ifndef HTTP_SERVER_UTILS_HPP
#define HTTP_SERVER_UTILS_HPP

#include <string>

/// Perform URL-decoding on a string. Returns false if the encoding was
/// invalid.
bool url_decode(const std::string& in, std::string& out);

#endif