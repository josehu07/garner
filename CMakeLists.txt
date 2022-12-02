cmake_minimum_required(VERSION 3.16)
project(garner)
set(CMAKE_CXX_STANDARD 20)

# Garner DB library.
add_subdirectory(garner)

# Client, testing, and benchmarking executables.
add_subdirectory(clients)
