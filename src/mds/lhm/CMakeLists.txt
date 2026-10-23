set_source_files_properties(${CMAKE_SOURCE_DIR}/src/mds/storage/RocksMetaStore.cpp PROPERTIES COMPILE_OPTIONS "-fno-rtti")
set(ZB_LHM_SOURCE_DIR "${CMAKE_SOURCE_DIR}/third_party/lhm" CACHE PATH "Block-indexed LHM source")
if(NOT UNIX OR APPLE)
    message(FATAL_ERROR "LHM bridge symbol isolation currently supports Linux ELF only")
endif()
add_library(zb_lhm_bridge SHARED LhmBridge.cpp
    ${ZB_LHM_SOURCE_DIR}/compiler.cc
    ${ZB_LHM_SOURCE_DIR}/kvthread.cc
    ${ZB_LHM_SOURCE_DIR}/memdebug.cc
    ${ZB_LHM_SOURCE_DIR}/persistent_store.cc
    ${ZB_LHM_SOURCE_DIR}/str.cc
    ${ZB_LHM_SOURCE_DIR}/straccum.cc
    ${ZB_LHM_SOURCE_DIR}/string.cc
    ${ZB_LHM_SOURCE_DIR}/string_slice.cc)
target_compile_features(zb_lhm_bridge PRIVATE cxx_std_17)
target_compile_options(zb_lhm_bridge PRIVATE -include ${ZB_LHM_SOURCE_DIR}/config.h)
target_include_directories(zb_lhm_bridge PRIVATE ${ZB_LHM_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(zb_lhm_bridge PRIVATE Threads::Threads)
# Localize even template/runtime symbols; only the plain C bridge is exported.
target_link_options(zb_lhm_bridge PRIVATE
    "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/exports.map" "LINKER:-Bsymbolic" "LINKER:-z,defs")
set_property(TARGET zb_lhm_bridge APPEND PROPERTY LINK_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/exports.map)
if(BUILD_TESTING)
    add_executable(lhm_bridge_test ${CMAKE_SOURCE_DIR}/tests/mds/lhm_bridge_test.cpp)
    target_compile_features(lhm_bridge_test PRIVATE cxx_std_17)
    target_include_directories(lhm_bridge_test PRIVATE ${CMAKE_SOURCE_DIR}/src)
    target_link_libraries(lhm_bridge_test PRIVATE zb_lhm_bridge Threads::Threads)
    target_link_options(lhm_bridge_test PRIVATE "LINKER:--export-dynamic")
endif()
if(BUILD_TESTING)
    set(ZB_OLD_MASSTREE ${CMAKE_SOURCE_DIR}/masstree/index/masstree)
    add_executable(lhm_coexist_test ${CMAKE_SOURCE_DIR}/tests/mds/lhm_coexist_test.cpp)
    foreach(source clp.c compiler.cc kvthread.cc masstree_wrapper.cc memdebug.cc misc.cc str.cc straccum.cc string.cc string_slice.cc)
        target_sources(lhm_coexist_test PRIVATE ${ZB_OLD_MASSTREE}/${source})
    endforeach()
    target_compile_features(lhm_coexist_test PRIVATE cxx_std_20)
    target_include_directories(lhm_coexist_test PRIVATE ${CMAKE_SOURCE_DIR}/src ${ZB_OLD_MASSTREE})
    target_link_libraries(lhm_coexist_test PRIVATE zb_lhm_bridge Threads::Threads)
    target_link_options(lhm_coexist_test PRIVATE "LINKER:--export-dynamic")
endif()

add_library(zb_lhm_inode_store STATIC LhmInodeStore.cpp LhmNamespaceStore.cpp)
target_compile_features(zb_lhm_inode_store PUBLIC cxx_std_17)
target_include_directories(zb_lhm_inode_store PUBLIC ${CMAKE_SOURCE_DIR}/src ${CMAKE_BINARY_DIR})
target_link_libraries(zb_lhm_inode_store PUBLIC zb_namespace_scan zb_proto zb_lhm_bridge)
if(BUILD_TESTING)
    target_compile_definitions(zb_lhm_bridge PRIVATE ZB_LHM_TESTING=1)
    add_executable(lhm_creation_test ${CMAKE_SOURCE_DIR}/tests/mds/lhm_creation_test.cpp)
    target_link_libraries(lhm_creation_test PRIVATE zb_lhm_inode_store)
    target_compile_definitions(lhm_creation_test PRIVATE ZB_LHM_TESTING=1)
endif()
if(BUILD_TESTING)
    add_executable(lhm_namespace_test ${CMAKE_SOURCE_DIR}/tests/mds/lhm_namespace_test.cpp
        ${CMAKE_SOURCE_DIR}/src/mds/storage/RocksNamespaceStore.cpp
        ${CMAKE_SOURCE_DIR}/src/mds/storage/RocksMetaStore.cpp
        ${CMAKE_SOURCE_DIR}/src/mds/storage/MetaCodec.cpp
        ${CMAKE_SOURCE_DIR}/src/mds/storage/UnifiedInodeRecord.cpp)
    target_compile_features(lhm_namespace_test PRIVATE cxx_std_20)
    target_compile_definitions(lhm_namespace_test PRIVATE ZB_LHM_TESTING=1)
    target_include_directories(lhm_namespace_test PRIVATE ${ROCKSDB_ROOT}/include)
    target_link_libraries(lhm_namespace_test PRIVATE zb_lhm_inode_store ${ROCKSDB_TARGET})
endif()
if(BUILD_TESTING)
    add_executable(lhm_control_store_tool ${CMAKE_SOURCE_DIR}/tests/mds/lhm_control_store_tool.cpp
        ${CMAKE_SOURCE_DIR}/src/mds/storage/RocksMetaStore.cpp)
    target_compile_features(lhm_control_store_tool PRIVATE cxx_std_20)
    target_include_directories(lhm_control_store_tool PRIVATE ${CMAKE_SOURCE_DIR}/src ${ROCKSDB_ROOT}/include)
    target_link_libraries(lhm_control_store_tool PRIVATE ${ROCKSDB_TARGET})
endif()

if(BUILD_TESTING)
    add_executable(subtree_scan_test ${CMAKE_SOURCE_DIR}/tests/mds/subtree_scan_test.cpp
        ${CMAKE_SOURCE_DIR}/src/mds/storage/RocksNamespaceStore.cpp
        ${CMAKE_SOURCE_DIR}/src/mds/storage/RocksMetaStore.cpp
        ${CMAKE_SOURCE_DIR}/src/mds/storage/MetaCodec.cpp
        ${CMAKE_SOURCE_DIR}/src/mds/storage/UnifiedInodeRecord.cpp)
    target_compile_features(subtree_scan_test PRIVATE cxx_std_20)
    target_compile_definitions(subtree_scan_test PRIVATE ZB_LHM_TESTING=1)
    target_include_directories(subtree_scan_test PRIVATE ${ROCKSDB_ROOT}/include)
    target_link_libraries(subtree_scan_test PRIVATE zb_lhm_inode_store ${ROCKSDB_TARGET})
endif()

if(BUILD_TESTING)
    add_executable(archive_pipeline_test ${CMAKE_SOURCE_DIR}/tests/mds/archive_pipeline_test.cpp)
    target_compile_features(archive_pipeline_test PRIVATE cxx_std_17)
    target_link_libraries(archive_pipeline_test PRIVATE zb_lhm_inode_store zb_archive_pipeline)
endif()
