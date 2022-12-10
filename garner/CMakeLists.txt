set(GARNER_SRC
    "include/garner.hpp"
    "bptree.hpp"
    "bptree.tpl.hpp"
    "common.hpp"
    "common.cpp"
    "garner_impl.hpp"
    "garner_impl.tpl.hpp"
    "open.cpp"
    "page.hpp"
    "page.tpl.hpp"
    "record.hpp"
    "txn.hpp"
    "txn_silo.hpp"
    "txn_silo.tpl.hpp"
    "txn_silo_hv.hpp"
    "txn_silo_hv.tpl.hpp"
)
add_library(garner ${GARNER_SRC})

target_include_directories(garner
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
    PUBLIC
        ${CMAKE_CURRENT_LIST_DIR}/include)
