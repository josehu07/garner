cmake_minimum_required(VERSION 3.16)

project(garner)
set(CMAKE_CXX_STANDARD 20)

find_package(Threads)

if(NOT CMAKE_BUILD_TYPE OR CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "" FORCE)
endif()

add_compile_definitions("$<$<CONFIG:RELEASE>:NDEBUG>")
add_compile_options(-Wall -Wextra -Wpedantic -Werror)

# Garner DB library.
add_subdirectory(garner)

# Client, testing, and benchmarking executables.
add_subdirectory(tests)
add_subdirectory(bench)

# Tests:
enable_testing()
add_test(
    NAME TestSingleBPTree
    COMMAND $<TARGET_FILE:test_single_bptree>)
add_test(
    NAME TestConcurBPTree
    COMMAND $<TARGET_FILE:test_concur_bptree>)
add_test(
    NAME TestSingleTxnRun_Silo
    COMMAND $<TARGET_FILE:test_single_txnrun> -p silo)
add_test(
    NAME TestConcurTxnRun_Silo
    COMMAND $<TARGET_FILE:test_concur_txnrun> -p silo)
add_test(
    NAME TestSingleTxnRun_SiloHV
    COMMAND $<TARGET_FILE:test_single_txnrun> -p silo_hv)
add_test(
    NAME TestConcurTxnRun_SiloHV_Static
    COMMAND $<TARGET_FILE:test_concur_txnrun> -p silo_hv -s)
# add_test(
#     NAME TestConcurTxnRun_SiloHV
#     COMMAND $<TARGET_FILE:test_concur_txnrun> -p silo_hv)
