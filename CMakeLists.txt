cmake_minimum_required(VERSION 3.16)
project(garner)
set(CMAKE_CXX_STANDARD 20)

# Garner DB library.
add_subdirectory(garner)

# Client, testing, and benchmarking executables.
add_subdirectory(clients)

# Tests:
enable_testing()
add_test(
    NAME FuzzyTest
    COMMAND $<TARGET_FILE:fuzzy_test> -f /tmp/garner_fuzzy_test_temp.txt)
