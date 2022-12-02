set(GARNER_SRC
    "include/garner.hpp"
    "bptree.hpp"
    "bptree.tpl.hpp"
    "common.cpp"
    "common.hpp"
    "format.cpp"
    "format.hpp"
    "garner_impl.hpp"
    "garner_impl.tpl.hpp"
    "garner.cpp"
    "pager.cpp"
    "pager.hpp"
)
add_library(garner ${GARNER_SRC})

target_include_directories(garner
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
    PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}/include)
