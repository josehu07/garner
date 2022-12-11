#include <cassert>
#include <random>
#include <stdexcept>
#include <string>

#pragma once

class BenchmarkException : public std::exception {
    std::string what_msg;

   public:
    BenchmarkException(std::string&& what_msg) : what_msg(what_msg) {}
    ~BenchmarkException() = default;

    const char* what() const noexcept override { return what_msg.c_str(); }
};

/**
 * Generate random alpha-numerical string.
 */
static constexpr char alphanum[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";

thread_local std::uniform_int_distribution<size_t> rand_idx(0,
                                                            sizeof(alphanum) -
                                                                2);

[[maybe_unused]] static std::string gen_rand_string(std::mt19937& gen,
                                                    size_t len) {
    std::string str;
    str.reserve(len);
    for (size_t i = 0; i < len; ++i) str += alphanum[rand_idx(gen)];

    return str;
}

/**
 * Garner request struct for benchmarking.
 */
typedef enum GarnerOp { GET, PUT, DELETE, SCAN, UNKNOWN } GarnerOp;

struct GarnerReq {
    GarnerOp op;
    std::string key;
    std::string rkey;
    std::string value;

    GarnerReq() = delete;
    GarnerReq(GarnerOp op, std::string key, std::string rkey, std::string val)
        : op(op), key(key), rkey(rkey), value(val) {}
};
