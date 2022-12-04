set(TEST_SINGLE_BPTREE_SRC
    "test_single_bptree.cpp"
    "cxxopts.hpp"
    "utils.hpp"
)
add_executable(test_single_bptree ${TEST_SINGLE_BPTREE_SRC})

target_include_directories(test_single_bptree
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
    PUBLIC
        ${PROJECT_SOURCE_DIR}/garner/include)
target_link_libraries(test_single_bptree garner)

set(TEST_CONCUR_BPTREE_SRC
    "test_concur_bptree.cpp"
    "cxxopts.hpp"
    "utils.hpp"
)
add_executable(test_concur_bptree ${TEST_CONCUR_BPTREE_SRC})

target_include_directories(test_concur_bptree
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
    PUBLIC
        ${PROJECT_SOURCE_DIR}/garner/include)
target_link_libraries(test_concur_bptree garner pthread)
