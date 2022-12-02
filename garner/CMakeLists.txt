set(GARNER_SRC
    "include/garner.hpp"
    "bptree.hpp"
    "bptree.tpl.hpp"
    "common.hpp"
    "garner_impl.hpp"
    "garner_impl.tpl.hpp"
    "garner.cpp"
    "page.hpp"
    "page.cpp"
)
add_library(garner ${GARNER_SRC})

target_include_directories(garner
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
    PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}/include)
