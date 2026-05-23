cmake_minimum_required(VERSION 3.10)
project(ZBSimTool CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

if(UNIX)
    set(SIM_MASSTREE_REAL_INDEX_DEFAULT ON)
else()
    set(SIM_MASSTREE_REAL_INDEX_DEFAULT OFF)
endif()
option(SIM_MASSTREE_REAL_INDEX "Use the repository Masstree wrapper for the sim metadata sparse index" ${SIM_MASSTREE_REAL_INDEX_DEFAULT})

add_executable(zb_sim_tool
    SimCluster.cpp
    SimFileIO.cpp
    sim_tool.cpp
    masstree/SimMasstreeCodec.cpp
    masstree/SimMasstreeIndexRuntime.cpp
    masstree/SimMasstreeManifest.cpp
    masstree/SimMasstreeOpticalAllocator.cpp
    masstree/SimMasstreePageLayout.cpp
    masstree/SimMasstreeStore.cpp
)

if(SIM_MASSTREE_REAL_INDEX)
    target_sources(zb_sim_tool
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree/masstree_wrapper.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree/compiler.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree/kvthread.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree/string.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree/str.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree/straccum.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree/string_slice.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree/misc.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree/memdebug.cc
    )
    target_compile_definitions(zb_sim_tool PRIVATE SIM_MASSTREE_REAL_INDEX=1)
endif()

target_include_directories(zb_sim_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/..
        ${CMAKE_CURRENT_SOURCE_DIR}/../../masstree/index/masstree
)

target_link_libraries(zb_sim_tool
    PRIVATE
        Threads::Threads
)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS 9)
    target_link_libraries(zb_sim_tool PRIVATE stdc++fs)
endif()
