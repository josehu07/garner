set(FUZZY_TEST_SRC
    "fuzzy_test.cpp"
    "cxxopts.hpp"
)
add_executable(fuzzy_test ${FUZZY_TEST_SRC})

target_include_directories(fuzzy_test
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
    PUBLIC
        ${PROJECT_SOURCE_DIR}/garner/include)
target_link_libraries(fuzzy_test garner)

set(YCSB_CLIENT_SRC
    "ycsb_client.cpp"
    "cxxopts.hpp"
)
add_executable(ycsb_client ${YCSB_CLIENT_SRC})

target_include_directories(ycsb_client
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
    PUBLIC
        ${PROJECT_SOURCE_DIR}/garner/include)
target_link_libraries(ycsb_client garner)
