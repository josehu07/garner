#include <random>
#include <stdexcept>
#include <string>

#pragma once

class FuzzyTestException : public std::exception {
    std::string what_msg;

   public:
    FuzzyTestException(std::string&& what_msg) : what_msg(what_msg) {}
    ~FuzzyTestException() = default;

    const char* what() const noexcept override { return what_msg.c_str(); }
};

// generate random string
static constexpr char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

thread_local std::uniform_int_distribution<size_t> rand_idx(0,
                                                            sizeof(alphanum) -
                                                                2);

static std::string gen_rand_string(std::mt19937& gen, size_t len) {
    std::string str;
    str.reserve(len);
    for (size_t i = 0; i < len; ++i) str += alphanum[rand_idx(gen)];

    return str;
}
