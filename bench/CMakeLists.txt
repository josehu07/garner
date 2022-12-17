set(YCSB_CLIENT_SRC
    "ycsb_client.cpp"
    "utils.hpp"
    "cxxopts.hpp"
)
add_executable(ycsb_client ${YCSB_CLIENT_SRC})

target_include_directories(ycsb_client
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
        ${CMAKE_BINARY_DIR}
    PUBLIC
        ${PROJECT_SOURCE_DIR}/garner/include)
target_link_libraries(ycsb_client garner pthread)

set(SIMPLE_BENCH_SRC
    "simple_bench.cpp"
    "utils.hpp"
    "cxxopts.hpp"
)
add_executable(simple_bench ${SIMPLE_BENCH_SRC})

target_include_directories(simple_bench
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
        ${CMAKE_BINARY_DIR}
    PUBLIC
        ${PROJECT_SOURCE_DIR}/garner/include)
target_link_libraries(simple_bench garner pthread)
