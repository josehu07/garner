#include <cassert>
#include <random>
#include <stdexcept>
#include <string>

#pragma once

class FuzzTestException : public std::exception {
    std::string what_msg;

   public:
    FuzzTestException(std::string&& what_msg) : what_msg(what_msg) {}
    ~FuzzTestException() = default;

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

static std::string gen_rand_string(std::mt19937& gen, size_t len) {
    std::string str;
    str.reserve(len);
    for (size_t i = 0; i < len; ++i) str += alphanum[rand_idx(gen)];

    return str;
}

/**
 * Garner request struct for testing.
 */
typedef enum GarnerOp { GET, PUT, DELETE, SCAN, UNKNOWN } GarnerOp;

struct GarnerReq {
    GarnerOp op;
    std::string key;
    std::string rkey;
    std::string value;
    bool get_found;
    std::vector<std::tuple<std::string, std::string>> scan_result;
    bool committed;
    uint64_t ser_order;

    GarnerReq() = delete;
    GarnerReq(GarnerOp op, std::string key)
        : op(op),
          key(key),
          rkey(),
          value(),
          get_found(false),
          scan_result(),
          committed(false),
          ser_order(0) {
        assert(op == GET);
    }
    GarnerReq(GarnerOp op, std::string key, std::string val)
        : op(op),
          key(key),
          rkey(),
          value(val),
          get_found(false),
          scan_result(),
          committed(false),
          ser_order(0) {
        assert(op == PUT);
    }
    GarnerReq(GarnerOp op, std::string lkey, std::string rkey,
              std::vector<std::tuple<std::string, std::string>> scan_result)
        : op(op),
          key(lkey),
          rkey(rkey),
          value(),
          get_found(false),
          scan_result(scan_result),
          committed(false),
          ser_order(0) {
        assert(op == SCAN);
    }
};
