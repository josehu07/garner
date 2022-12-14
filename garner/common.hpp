// Common helper classes and functions.

#include <syscall.h>
#include <unistd.h>

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#pragma once

namespace garner {

/** Universal exception type. */
class GarnerException : public std::exception {
    std::string what_msg;

   public:
    GarnerException(std::string&& what_msg)
        : what_msg("GarnerException: " + what_msg) {}
    ~GarnerException() = default;

    const char* what() const noexcept override { return what_msg.c_str(); }
};

/** Debug printing utilities. */
// thread ID
extern thread_local const pid_t tid;

// get std::string representation of any variable that has operator<<
template <typename T>
static inline std::string StreamStr(const T& item) {
    std::ostringstream ss;
    ss << item;
    return std::move(ss.str());
}

// `DEBUG()` and `assert()` are not active if NDEBUG is defined at
// compilation time by the build system.
#ifdef NDEBUG
#define DEBUG(msg, ...)
#else
#define DEBUG(msg, ...)                                                       \
    do {                                                                      \
        const char* tmp = strrchr(__FILE__, '/');                             \
        const char* file = tmp ? tmp + 1 : __FILE__;                          \
        fprintf(stderr, "[%20s:%-4d@t:%-6d]  " msg "\n", file, __LINE__, tid, \
                ##__VA_ARGS__);                                               \
    } while (0)
#endif

}  // namespace garner
