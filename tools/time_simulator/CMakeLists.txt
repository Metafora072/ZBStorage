cmake_minimum_required(VERSION 3.16)
project(zbstorage_time_simulator LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(time_simulator_core STATIC
    src/time_simulator.cpp
    src/trace_generator.cpp
    src/fuse_executor.cpp
)

target_include_directories(time_simulator_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(time_simulator_core
    PUBLIC
        Threads::Threads
)

target_compile_options(time_simulator_core
    PRIVATE
        -Wall
        -Wextra
        -Wpedantic
)

add_executable(zbstorage_time_simulator
    src/main.cpp
)

target_link_libraries(zbstorage_time_simulator
    PRIVATE
        time_simulator_core
)

include(CTest)
if(BUILD_TESTING)
    add_executable(time_simulator_smoke_test
        tests/smoke_test.cpp
    )
    target_link_libraries(time_simulator_smoke_test
        PRIVATE
            time_simulator_core
    )
    add_test(
        NAME time_simulator_smoke_test
        COMMAND time_simulator_smoke_test
    )
endif()
