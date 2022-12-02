set(CLIENT_SRC
    "client.cpp"
    "cxxopts.hpp"
)

add_executable(client ${CLIENT_SRC})
target_include_directories(client
    PRIVATE
        ${CMAKE_CURRENT_LIST_DIR}
        ${PROJECT_SOURCE_DIR}/garner/include)
target_link_libraries(client garner)
