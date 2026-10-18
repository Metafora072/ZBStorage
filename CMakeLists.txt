cmake_minimum_required(VERSION 3.16)
project(ZBStorage C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Keep BUILD_TESTING and an empty CTest manifest for existing build directories.
# Project checks are manual targets/scripts; do not register them with add_test.
include(CTest)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc/cmake")

option(BUILD_BRPC_TOOLS "Build brpc tools" OFF)
option(BUILD_UNIT_TESTS "Build brpc unit tests" OFF)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)

add_subdirectory(third_party/brpc)

set(ZBSTORAGE_BRPC_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc/src
    ${CMAKE_CURRENT_BINARY_DIR}/third_party/brpc
)

find_package(Protobuf REQUIRED)
find_package(GFLAGS REQUIRED)
find_package(Threads REQUIRED)
find_package(ZLIB REQUIRED)
if(UNIX)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(FUSE3 REQUIRED fuse3)
endif()

set(ROCKSDB_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/third_party/rocksdb")
if(NOT EXISTS "${ROCKSDB_ROOT}/CMakeLists.txt")
    message(FATAL_ERROR "Bundled RocksDB not found. Please clone into third_party/rocksdb.")
endif()

set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(ROCKSDB_BUILD_STATIC ON CACHE BOOL "" FORCE)
set(ROCKSDB_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(ROCKSDB_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(ROCKSDB_BUILD_BENCH OFF CACHE BOOL "" FORCE)
# Upstream RocksDB uses WITH_* switches. Disable its private test/tool matrix
# so the top-level ZBStorage CTest suite contains only built project tests.
set(WITH_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_ALL_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)

# 默认产物需要能在不同 CPU 的 CI runner / 存储节点运行。RocksDB 默认
# PORTABLE=0 会启用 -march=native，其全局初始化也可能生成宿主特有指令。
# 保留显式 -DPORTABLE=0 的同机优化入口，不强制覆盖调用者配置。
set(PORTABLE ON CACHE STRING "Minimum CPU arch to support; ON builds portable RocksDB")

add_subdirectory(${ROCKSDB_ROOT} ${CMAKE_CURRENT_BINARY_DIR}/third_party/rocksdb EXCLUDE_FROM_ALL)

set(ROCKSDB_TARGET "")
if(TARGET rocksdb)
    set(ROCKSDB_TARGET rocksdb)
elseif(TARGET rocksdb-static)
    set(ROCKSDB_TARGET rocksdb-static)
elseif(TARGET rocksdb-shared)
    set(ROCKSDB_TARGET rocksdb-shared)
endif()
if(ROCKSDB_TARGET STREQUAL "")
    message(FATAL_ERROR "Failed to resolve RocksDB target from bundled build")
endif()
set_target_properties(${ROCKSDB_TARGET} PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

set(PROTO_FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msg/real_node.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msg/mds.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msg/scheduler.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msg/optical_node.proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/msg/client_prepare_control.proto
)
set(PROTOBUF_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/src/msg ${Protobuf_INCLUDE_DIRS})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(zb_proto ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(zb_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(zb_proto PUBLIC ${Protobuf_LIBRARIES})

set(REAL_NODE_SOURCES
    src/common/metrics/NodeMetricsCollector.cpp
    src/common/metrics/SchedulerMetricsClient.cpp
    src/data_node/real_node/config/NodeConfig.cpp
    src/data_node/real_node/io/DiskManager.cpp
    src/data_node/real_node/io/LocalPathResolver.cpp
    src/data_node/real_node/io/IOExecutor.cpp
    src/data_node/common/RealObjectStore.cpp
    src/data_node/real_node/service/ArchiveFileMetaStore.cpp
    src/data_node/real_node/service/ArchiveObjectMetaStore.cpp
    src/data_node/real_node/service/StorageServiceImpl.cpp
    src/data_node/real_node/service/BrpcStorageService.cpp
    src/data_node/real_node/server/real_node_server.cpp
)

add_executable(real_node_server ${REAL_NODE_SOURCES})
target_include_directories(real_node_server
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)

target_link_libraries(real_node_server
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)

get_directory_property(BRPC_DYNAMIC_LIB DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc DEFINITION DYNAMIC_LIB)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(real_node_server PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(virtual_node_server
    src/common/metrics/NodeMetricsCollector.cpp
    src/common/metrics/SchedulerMetricsClient.cpp
    src/data_node/virtual_node/config/VirtualNodeConfig.cpp
    src/data_node/common/VirtualObjectStore.cpp
    src/data_node/real_node/service/ArchiveFileMetaStore.cpp
    src/data_node/real_node/service/ArchiveObjectMetaStore.cpp
    src/data_node/virtual_node/service/VirtualStorageServiceImpl.cpp
    src/data_node/virtual_node/service/ModelStateStore.cpp
    src/data_node/virtual_node/service/BrpcVirtualStorageService.cpp
    src/data_node/virtual_node/server/virtual_node_server.cpp
)
set_target_properties(virtual_node_server PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
target_include_directories(virtual_node_server
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(virtual_node_server
    PRIVATE
        zb_proto
        brpc-static
        ${ROCKSDB_TARGET}
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(virtual_node_server PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_library(optical_node_manager STATIC
    src/data_node/optical_node/optical_node_manager/optical_node_manager.cpp
    src/data_node/optical_node/optical_node_manager/cd_manager_sim/cd_manager.cpp
    src/data_node/optical_node/optical_node_manager/space_manager/space_manager.cpp
    src/data_node/optical_node/optical_node_manager/space_manager/sha256.cpp
    src/data_node/optical_node/optical_node_manager/volume_manager/volume_manager.cpp
    src/data_node/optical_node/optical_node_manager/volume_manager/serializer.cpp
    src/data_node/optical_node/optical_node_manager/volume_manager/compression_utils.cpp
    src/data_node/optical_node/optical_node_manager/volume_manager/error_codes.cpp
    src/data_node/optical_node/optical_node_manager/volume_manager/utils.cpp
)
target_include_directories(optical_node_manager
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src/data_node/optical_node/optical_node_manager
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(optical_node_manager
    PRIVATE
        ZLIB::ZLIB
        ${CMAKE_THREAD_LIBS_INIT}
        zb_proto
        brpc-static
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(optical_node_manager PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(optical_node_server
    src/common/metrics/NodeMetricsCollector.cpp
    src/common/metrics/SchedulerMetricsClient.cpp
    src/data_node/optical_node/config/OpticalNodeConfig.cpp
    src/data_node/optical_node/service/OpticalStorageServiceImpl.cpp
    src/data_node/optical_node/service/OpticalLibraryHandoff.cpp
    src/data_node/optical_node/service/BrpcOpticalNodeService.cpp
    src/data_node/optical_node/service/ReadEventSessionRegistry.cpp
    src/data_node/optical_node/server/optical_node_server.cpp
)
target_include_directories(optical_node_server
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(optical_node_server
    PRIVATE
        optical_node_manager
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(optical_node_server PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(real_node_client
    src/data_node/real_node/client/real_node_client.cpp
)
target_include_directories(real_node_client
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(real_node_client
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(real_node_client PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(real_node_multi_test
    src/client/test/real_node_multi_test.cpp
)
target_include_directories(real_node_multi_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(real_node_multi_test
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(real_node_multi_test PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(virtual_node_test
    src/client/test/virtual_node_test.cpp
)
target_include_directories(virtual_node_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(virtual_node_test
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(virtual_node_test PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_library(optical_disc_catalog
    tools/storagenode/optical_disc_catalog.cpp
)
target_include_directories(optical_disc_catalog
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/tools
)

add_executable(generate_discs
    tools/storagenode/generate_discs.cpp
)
target_link_libraries(generate_discs PRIVATE optical_disc_catalog)

add_executable(generate_legacy_mixed_discs
    tools/storagenode/generate_legacy_mixed_discs.cpp
)
target_link_libraries(generate_legacy_mixed_discs PRIVATE optical_disc_catalog)

add_executable(import_discs_standalone
    tools/storagenode/import_discs_standalone.cpp
)
target_link_libraries(import_discs_standalone PRIVATE optical_disc_catalog)

add_executable(system_demo_tool
    tools/demo/demo_logger.cpp
    tools/demo/demo_menu.cpp
    tools/demo/demo_params.cpp
    tools/demo/demo_result.cpp
    tools/demo/system_demo_tool.cpp
)
set_target_properties(system_demo_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(system_demo_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/tools
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(system_demo_tool
    PRIVATE
        zb_proto
        optical_disc_catalog
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(system_demo_tool PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(random_read_6b_files
    tools/random_read_6b_files.cpp
)
set_target_properties(random_read_6b_files PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

add_executable(optical_archive_stress_test
    src/client/test/optical_archive_stress_test.cpp
)
target_include_directories(optical_archive_stress_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(optical_archive_stress_test
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(optical_archive_stress_test PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(scheduler_control_test
    src/client/test/scheduler_control_test.cpp
)
target_include_directories(scheduler_control_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(scheduler_control_test
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(scheduler_control_test PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(meta_plan_tool
    tools/meta_gen/capacity_planner.cpp
    tools/meta_gen/namespace_layout_planner.cpp
    tools/meta_gen/file_size_sampler.cpp
    tools/meta_gen/meta_plan_tool.cpp
)
target_include_directories(meta_plan_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tools/meta_gen
)

add_executable(disk_meta_gen_tool
    tools/meta_gen/namespace_layout_planner.cpp
    tools/meta_gen/file_size_sampler.cpp
    tools/meta_gen/workload_enumerator.cpp
    tools/meta_gen/disk_meta_generator.cpp
    tools/meta_gen/disk_meta_gen_tool.cpp
)
set_target_properties(disk_meta_gen_tool PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(disk_meta_gen_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tools/meta_gen
)

add_executable(optical_meta_gen_tool
    tools/meta_gen/capacity_planner.cpp
    tools/meta_gen/namespace_layout_planner.cpp
    tools/meta_gen/file_size_sampler.cpp
    tools/meta_gen/workload_enumerator.cpp
    tools/meta_gen/optical_meta_generator.cpp
    tools/meta_gen/optical_meta_gen_tool.cpp
)
set_target_properties(optical_meta_gen_tool PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(optical_meta_gen_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tools/meta_gen
)

add_executable(mds_sst_gen_tool
    tools/meta_gen/namespace_layout_planner.cpp
    tools/meta_gen/file_size_sampler.cpp
    tools/meta_gen/mds_sst_generator.cpp
    tools/meta_gen/mds_sst_gen_tool.cpp
    src/mds/storage/UnifiedInodeRecord.cpp
)
set_target_properties(mds_sst_gen_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(mds_sst_gen_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/tools/meta_gen
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ROCKSDB_ROOT}/include
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(mds_sst_gen_tool
    PRIVATE
        zb_proto
        ${ROCKSDB_TARGET}
)

add_executable(mds_sst_ingest_tool
    tools/meta_gen/mds_sst_ingest_tool.cpp
)
set_target_properties(mds_sst_ingest_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(mds_sst_ingest_tool
    PRIVATE
        ${ROCKSDB_ROOT}/include
)
target_link_libraries(mds_sst_ingest_tool
    PRIVATE
        ${ROCKSDB_TARGET}
)

add_executable(archive_ns_route_tool
    tools/archive_meta/archive_ns_route_tool.cpp
    src/mds/archive_meta/ArchiveGenerationPublisher.cpp
    src/mds/archive_meta/ArchiveManifest.cpp
    src/mds/archive_meta/ArchiveNamespaceCatalog.cpp
    src/mds/storage/RocksMetaStore.cpp
)
set_target_properties(archive_ns_route_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(archive_ns_route_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ROCKSDB_ROOT}/include
)
target_link_libraries(archive_ns_route_tool
    PRIVATE
        ${ROCKSDB_TARGET}
)

add_executable(archive_meta_build_tool
    tools/archive_meta/archive_meta_build_tool.cpp
    src/mds/archive_meta/ArchiveBloomFilter.cpp
    src/mds/archive_meta/ArchiveGenerationPublisher.cpp
    src/mds/archive_meta/ArchiveImportService.cpp
    src/mds/archive_meta/ArchiveManifest.cpp
    src/mds/archive_meta/ArchiveNamespaceCatalog.cpp
    src/mds/storage/MetaCodec.cpp
    src/mds/storage/UnifiedInodeRecord.cpp
    src/mds/storage/RocksMetaStore.cpp
)
set_target_properties(archive_meta_build_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(archive_meta_build_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ROCKSDB_ROOT}/include
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(archive_meta_build_tool
    PRIVATE
        zb_proto
        ${ROCKSDB_TARGET}
)

add_executable(masstree_meta_generate_tool
    tools/masstree/masstree_meta_generate_tool.cpp
    src/mds/masstree_meta/MasstreeBulkMetaGenerator.cpp
    src/mds/masstree_meta/MasstreeManifest.cpp
    src/mds/storage/UnifiedInodeRecord.cpp
)
set_target_properties(masstree_meta_generate_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(masstree_meta_generate_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(masstree_meta_generate_tool
    PRIVATE
        zb_proto
)
if(UNIX)
    target_compile_definitions(masstree_meta_generate_tool PRIVATE ZBSTORAGE_ENABLE_MASSTREE_INDEX=1)
endif()

add_executable(masstree_meta_import_tool
    tools/masstree/masstree_meta_import_tool.cpp
    src/mds/masstree_meta/MasstreeBulkImporter.cpp
    src/mds/masstree_meta/MasstreeIndexRuntime.cpp
    src/mds/masstree_meta/MasstreeInodeRecordCodec.cpp
    src/mds/masstree_meta/MasstreeManifest.cpp
    src/mds/masstree_meta/MasstreeOpticalAllocator.cpp
    src/mds/masstree_meta/MasstreePageLayout.cpp
    src/mds/masstree_meta/MasstreePageReader.cpp
    src/mds/storage/UnifiedInodeRecord.cpp
)
set_target_properties(masstree_meta_import_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(masstree_meta_import_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/masstree/index/masstree
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(masstree_meta_import_tool
    PRIVATE
        zb_proto
)
if(UNIX)
    target_sources(masstree_meta_import_tool PRIVATE
        masstree/index/masstree/clp.c
        masstree/index/masstree/compiler.cc
        masstree/index/masstree/kvthread.cc
        masstree/index/masstree/masstree_wrapper.cc
        masstree/index/masstree/memdebug.cc
        masstree/index/masstree/misc.cc
        masstree/index/masstree/str.cc
        masstree/index/masstree/straccum.cc
        masstree/index/masstree/string.cc
        masstree/index/masstree/string_slice.cc
    )
    target_compile_definitions(masstree_meta_import_tool PRIVATE ZBSTORAGE_ENABLE_MASSTREE_INDEX=1)
endif()

add_executable(masstree_random_bulk_import_tool
    tools/masstree/masstree_random_bulk_import_tool.cpp
    src/mds/masstree_meta/MasstreeBulkImporter.cpp
    src/mds/masstree_meta/MasstreeBulkMetaGenerator.cpp
    src/mds/masstree_meta/MasstreeIndexRuntime.cpp
    src/mds/masstree_meta/MasstreeInodeRecordCodec.cpp
    src/mds/masstree_meta/MasstreeManifest.cpp
    src/mds/masstree_meta/MasstreeOpticalAllocator.cpp
    src/mds/masstree_meta/MasstreePageLayout.cpp
    src/mds/masstree_meta/MasstreePageReader.cpp
    src/mds/storage/UnifiedInodeRecord.cpp
)
set_target_properties(masstree_random_bulk_import_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(masstree_random_bulk_import_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/masstree/index/masstree
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(masstree_random_bulk_import_tool
    PRIVATE
        zb_proto
)
if(UNIX)
    target_sources(masstree_random_bulk_import_tool PRIVATE
        masstree/index/masstree/clp.c
        masstree/index/masstree/compiler.cc
        masstree/index/masstree/kvthread.cc
        masstree/index/masstree/masstree_wrapper.cc
        masstree/index/masstree/memdebug.cc
        masstree/index/masstree/misc.cc
        masstree/index/masstree/str.cc
        masstree/index/masstree/straccum.cc
        masstree/index/masstree/string.cc
        masstree/index/masstree/string_slice.cc
    )
    target_compile_definitions(masstree_random_bulk_import_tool PRIVATE ZBSTORAGE_ENABLE_MASSTREE_INDEX=1)
endif()

add_executable(masstree_meta_build_tool
    tools/masstree/masstree_meta_build_tool.cpp
    src/mds/masstree_meta/MasstreeBulkImporter.cpp
    src/mds/masstree_meta/MasstreeBulkMetaGenerator.cpp
    src/mds/masstree_meta/MasstreeGenerationPublisher.cpp
    src/mds/masstree_meta/MasstreeImportService.cpp
    src/mds/masstree_meta/MasstreeIndexRuntime.cpp
    src/mds/masstree_meta/MasstreeInodeRecordCodec.cpp
    src/mds/masstree_meta/MasstreeManifest.cpp
    src/mds/masstree_meta/MasstreeNamespaceCatalog.cpp
    src/mds/masstree_meta/MasstreeOpticalAllocator.cpp
    src/mds/masstree_meta/MasstreeOpticalLayoutStore.cpp
    src/mds/masstree_meta/MasstreeOpticalLayoutTranslator.cpp
    src/mds/masstree_meta/MasstreePageLayout.cpp
    src/mds/masstree_meta/MasstreePageReader.cpp
    src/mds/masstree_meta/MasstreeStatsStore.cpp
    src/mds/storage/MetaCodec.cpp
    src/mds/storage/UnifiedInodeRecord.cpp
    src/mds/storage/RocksMetaStore.cpp
)
set_target_properties(masstree_meta_build_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(masstree_meta_build_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/masstree/index/masstree
        ${CMAKE_CURRENT_BINARY_DIR}
        ${ROCKSDB_ROOT}/include
)
target_link_libraries(masstree_meta_build_tool
    PRIVATE
        zb_proto
        ${ROCKSDB_TARGET}
)
if(UNIX)
    target_sources(masstree_meta_build_tool PRIVATE
        masstree/index/masstree/clp.c
        masstree/index/masstree/compiler.cc
        masstree/index/masstree/kvthread.cc
        masstree/index/masstree/masstree_wrapper.cc
        masstree/index/masstree/memdebug.cc
        masstree/index/masstree/misc.cc
        masstree/index/masstree/str.cc
        masstree/index/masstree/straccum.cc
        masstree/index/masstree/string.cc
        masstree/index/masstree/string_slice.cc
    )
    target_compile_definitions(masstree_meta_build_tool PRIVATE ZBSTORAGE_ENABLE_MASSTREE_INDEX=1)
endif()

add_executable(masstree_ns_route_tool
    tools/masstree/masstree_ns_route_tool.cpp
    src/mds/masstree_meta/MasstreeManifest.cpp
    src/mds/masstree_meta/MasstreeNamespaceCatalog.cpp
    src/mds/storage/RocksMetaStore.cpp
)
set_target_properties(masstree_ns_route_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(masstree_ns_route_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ROCKSDB_ROOT}/include
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(masstree_ns_route_tool
    PRIVATE
        zb_proto
        ${ROCKSDB_TARGET}
)

add_executable(masstree_optical_layout_migrate_tool
    tools/masstree/masstree_optical_layout_migrate_tool.cpp
    src/mds/masstree_meta/MasstreeOpticalLayoutStore.cpp
    src/mds/masstree_meta/MasstreeOpticalLayoutTranslator.cpp
    src/mds/masstree_meta/MasstreeStatsStore.cpp
    src/mds/storage/RocksMetaStore.cpp
)
set_target_properties(masstree_optical_layout_migrate_tool PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(masstree_optical_layout_migrate_tool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ROCKSDB_ROOT}/include
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(masstree_optical_layout_migrate_tool
    PRIVATE
        ${ROCKSDB_TARGET}
)

add_executable(layout_commit_conflict_test
    src/client/test/layout_commit_conflict_test.cpp
)
target_include_directories(layout_commit_conflict_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(layout_commit_conflict_test
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(layout_commit_conflict_test PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(pg_migration_sim_test EXCLUDE_FROM_ALL
    src/client/test/pg_migration_sim_test.cpp
    src/mds/allocator/PGManager.cpp
)
target_include_directories(pg_migration_sim_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${GFLAGS_INCLUDE_PATH}
)
target_link_libraries(pg_migration_sim_test
    PRIVATE
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)

add_executable(mds_server
    src/mds/config/MdsConfig.cpp
    src/mds/common/SystemMemory.cpp
    src/mds/archive_meta/ArchiveBloomFilter.cpp
    src/mds/archive_meta/ArchiveDataFile.cpp
    src/mds/archive_meta/ArchiveGenerationPublisher.cpp
    src/mds/archive_meta/ArchiveImportService.cpp
    src/mds/archive_meta/ArchiveSparseIndex.cpp
    src/mds/archive_meta/ArchiveManifest.cpp
    src/mds/archive_meta/ArchiveMetaStore.cpp
    src/mds/archive_meta/ArchiveNamespaceCatalog.cpp
    src/mds/masstree_meta/MasstreeBulkMetaGenerator.cpp
    src/mds/masstree_meta/MasstreeBulkImporter.cpp
    src/mds/masstree_meta/MasstreeGenerationPublisher.cpp
    src/mds/masstree_meta/MasstreeImportService.cpp
    src/mds/masstree_meta/MasstreeIndexRuntime.cpp
    src/mds/masstree_meta/MasstreeInodeRecordCodec.cpp
    src/mds/masstree_meta/MasstreeManifest.cpp
    src/mds/masstree_meta/MasstreeMetaStore.cpp
    src/mds/masstree_meta/MasstreeNamespaceCatalog.cpp
    src/mds/masstree_meta/MasstreeOpticalAllocator.cpp
    src/mds/masstree_meta/MasstreeOpticalLayoutStore.cpp
    src/mds/masstree_meta/MasstreeOpticalLayoutTranslator.cpp
    src/mds/masstree_meta/MasstreePageLayout.cpp
    src/mds/masstree_meta/MasstreePageReader.cpp
    src/mds/masstree_meta/MasstreeStatsStore.cpp
    src/mds/storage/MetaCodec.cpp
    src/mds/storage/UnifiedInodeRecord.cpp
    src/mds/storage/MetaStoreRouter.cpp
    src/mds/storage/RocksMetaStore.cpp
    src/mds/allocator/NodeStateCache.cpp
    src/mds/allocator/CmsNodeRegistry.cpp
    src/mds/allocator/PGManager.cpp
    src/mds/allocator/ObjectAllocator.cpp
    src/mds/archive/ArchiveBatchStager.cpp
    src/mds/archive/ArchiveLeaseManager.cpp
    src/mds/archive/OpticalArchiveManager.cpp
    src/common/metrics/NodeMetricsCollector.cpp
    src/common/metrics/SchedulerMetricsClient.cpp
    src/mds/service/MdsServiceImpl.cpp
    src/mds/service/OpticalLibraryHandoff.cpp
    src/mds/server/mds_server.cpp
)
set_target_properties(mds_server PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_include_directories(mds_server
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/masstree/index/masstree
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${ROCKSDB_ROOT}/include
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(mds_server
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
        ${ROCKSDB_TARGET}
)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(mds_server PRIVATE -Wno-deprecated-declarations)
endif()
if(UNIX)
    target_sources(mds_server PRIVATE
        masstree/index/masstree/clp.c
        masstree/index/masstree/compiler.cc
        masstree/index/masstree/kvthread.cc
        masstree/index/masstree/masstree_wrapper.cc
        masstree/index/masstree/memdebug.cc
        masstree/index/masstree/misc.cc
        masstree/index/masstree/str.cc
        masstree/index/masstree/straccum.cc
        masstree/index/masstree/string.cc
        masstree/index/masstree/string_slice.cc
    )
    target_compile_definitions(mds_server PRIVATE ZBSTORAGE_ENABLE_MASSTREE_INDEX=1)
endif()
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(mds_server PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_library(zb_storage_model
    src/storage_model/StorageModel.cpp
)
target_include_directories(zb_storage_model
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
set_target_properties(zb_storage_model PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)

add_library(scheduler_node_model
    src/scheduler/model/ManagedNode.cpp
    src/scheduler/power/NodePowerManager.cpp
)
target_include_directories(scheduler_node_model
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
set_target_properties(scheduler_node_model PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF
)
target_link_libraries(scheduler_node_model PUBLIC zb_storage_model)

add_library(zb_scheduler_api_client STATIC
    src/scheduler/api/SchedulerClient.cpp
    src/scheduler/api/OpticalArchiveClient.cpp
)
target_compile_features(zb_scheduler_api_client PUBLIC cxx_std_17)
target_include_directories(zb_scheduler_api_client PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(zb_scheduler_api_client PUBLIC
    zb_proto brpc-static ${GFLAGS_LIBRARY} ${CMAKE_THREAD_LIBS_INIT})
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(zb_scheduler_api_client PUBLIC ${BRPC_DYNAMIC_LIB})
endif()

add_library(zb_node_metrics STATIC
    src/common/metrics/NodeMetricsCollector.cpp
)
target_include_directories(zb_node_metrics PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)

add_library(zb_simulated_disk STATIC
    src/data_node/virtual_node/config/VirtualNodeConfig.cpp
    src/data_node/common/VirtualObjectStore.cpp
    src/data_node/real_node/service/ArchiveFileMetaStore.cpp
    src/data_node/real_node/service/ArchiveObjectMetaStore.cpp
    src/data_node/virtual_node/service/VirtualStorageServiceImpl.cpp
    src/data_node/virtual_node/service/ModelStateStore.cpp
    src/data_node/virtual_node/service/BrpcVirtualStorageService.cpp
    src/scheduler/simulation/service/InProcessDiskService.cpp
)
set_target_properties(zb_simulated_disk PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
target_include_directories(zb_simulated_disk PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(zb_simulated_disk PUBLIC
    zb_node_metrics scheduler_node_model zb_proto brpc-static ${ROCKSDB_TARGET} ${CMAKE_THREAD_LIBS_INIT})
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(zb_simulated_disk PUBLIC ${BRPC_DYNAMIC_LIB})
endif()

add_library(zb_simulated_optical STATIC
    src/scheduler/simulation/optical/OpticalReadModel.cpp
    src/scheduler/simulation/optical/OpticalStateStore.cpp
    src/scheduler/simulation/service/InProcessOpticalService.cpp
    src/scheduler/simulation/service/InProcessOpticalPrepared.cpp
)
set_target_properties(zb_simulated_optical PROPERTIES CXX_STANDARD 20 CXX_STANDARD_REQUIRED ON)
target_include_directories(zb_simulated_optical PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
    ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(zb_simulated_optical PUBLIC
    zb_simulated_disk zb_node_metrics scheduler_node_model zb_proto brpc-static ${ROCKSDB_TARGET} ${CMAKE_THREAD_LIBS_INIT})
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(zb_simulated_optical PUBLIC ${BRPC_DYNAMIC_LIB})
endif()

add_executable(scheduler_server
    src/scheduler/cms/CmsNodeCatalogPublisher.cpp
    src/scheduler/config/SchedulerConfig.cpp
    src/scheduler/health/FailureDetector.cpp
    src/scheduler/model/ClusterState.cpp
    src/scheduler/lifecycle/NodeActuator.cpp
    src/scheduler/lifecycle/LifecycleManager.cpp
    src/scheduler/migration/DrainMigrationCoordinator.cpp
    src/scheduler/migration/PhysicalOpticalDrain.cpp
    src/scheduler/migration/BrpcObjectMigrationBackend.cpp
    src/scheduler/power/PowerActuationController.cpp
    src/scheduler/persistence/ManagedNodeSnapshotStore.cpp
    src/scheduler/service/ManagedNodeProtoAdapter.cpp
    src/scheduler/service/SchedulerServiceImpl.cpp
    src/scheduler/server/scheduler_server.cpp
)
target_include_directories(scheduler_server
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(scheduler_server
    PRIVATE
        zb_simulated_disk
        zb_simulated_optical
        scheduler_node_model
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(scheduler_server PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

if(BUILD_TESTING)
    # 系统闭环 RPC 驱动仅手动指定目标构建，不进入默认 ALL，也不注册全仓 CTest。
    add_executable(scheduler_inprocess_disk_route_probe EXCLUDE_FROM_ALL
        tests/scheduler/inprocess_disk_route_probe.cpp)
    target_link_libraries(scheduler_inprocess_disk_route_probe PRIVATE zb_proto brpc-static)
    target_include_directories(scheduler_inprocess_disk_route_probe PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR})
    if(BRPC_DYNAMIC_LIB)
        target_link_libraries(scheduler_inprocess_disk_route_probe PRIVATE ${BRPC_DYNAMIC_LIB})
    endif()
    add_executable(scheduler_model_closure_probe EXCLUDE_FROM_ALL
        tests/scheduler/model_closure_probe.cpp)
    target_link_libraries(scheduler_model_closure_probe PRIVATE zb_scheduler_api_client)
    target_include_directories(scheduler_model_closure_probe PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${CMAKE_CURRENT_BINARY_DIR})
    # API 库已 PUBLIC 传播 BRPC 及其系统依赖，避免重复直连打乱静态库顺序。
endif()

add_executable(scheduler_trace_replay
    tools/workload_trace/replay_trace.cpp
)
target_include_directories(scheduler_trace_replay
    PRIVATE
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(scheduler_trace_replay
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(scheduler_trace_replay PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(api_gateway
    gateway/api_gateway.cpp
    gateway/demo_runner.cpp
)
target_include_directories(api_gateway
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/gateway
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(api_gateway
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(api_gateway PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

if(UNIX)
    add_library(zb_client_metrics src/client/metrics/IoLatency.cpp)
    target_include_directories(zb_client_metrics PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(zb_client_metrics PUBLIC Threads::Threads)

    add_library(zb_prepare_sdk
        src/client/control/LocalProtocol.cpp
        src/client/sdk/PrepareClient.cpp)
    target_include_directories(zb_prepare_sdk PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(zb_prepare_sdk PUBLIC zb_proto Threads::Threads)

    add_library(zb_client_optical
        src/client/optical/OpticalReadClient.cpp
        src/client/control/PrepareControlServer.cpp)
    target_include_directories(zb_client_optical PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/src ${ZBSTORAGE_BRPC_INCLUDE_DIRS})
    target_link_libraries(zb_client_optical PUBLIC zb_prepare_sdk zb_client_metrics brpc-static)
    if(BRPC_DYNAMIC_LIB)
        target_link_libraries(zb_client_optical PUBLIC ${BRPC_DYNAMIC_LIB})
    endif()

    add_library(zb_batch_sdk src/client/sdk/BatchIoClient.cpp)
    target_include_directories(zb_batch_sdk PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
    target_link_libraries(zb_batch_sdk PUBLIC Threads::Threads)
    add_executable(zb_batch src/client/tools/zb_batch.cpp)
    target_link_libraries(zb_batch PRIVATE zb_batch_sdk)

    # Manual client/model integration driver; never part of ALL or CTest.
    add_executable(scheduler_simulation_fixture EXCLUDE_FROM_ALL
        tests/client/scheduler_simulation_fixture.cpp)
    target_include_directories(scheduler_simulation_fixture PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR} ${ZBSTORAGE_BRPC_INCLUDE_DIRS})
    target_link_libraries(scheduler_simulation_fixture PRIVATE zb_proto brpc-static
        ${GFLAGS_LIBRARY} ${CMAKE_THREAD_LIBS_INIT})
    if(BRPC_DYNAMIC_LIB)
        target_link_libraries(scheduler_simulation_fixture PRIVATE ${BRPC_DYNAMIC_LIB})
    endif()

    add_executable(zbctl src/client/tools/zbctl.cpp)
    target_link_libraries(zbctl PRIVATE zb_prepare_sdk)

    add_executable(zb_fuse_client
        src/client/fuse/zb_fuse_client.cpp
    )
    target_include_directories(zb_fuse_client
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
            ${GFLAGS_INCLUDE_PATH}
            ${FUSE3_INCLUDE_DIRS}
            ${CMAKE_CURRENT_BINARY_DIR}
    )
    target_compile_options(zb_fuse_client PRIVATE ${FUSE3_CFLAGS_OTHER})
    target_link_directories(zb_fuse_client PRIVATE ${FUSE3_LIBRARY_DIRS})
    target_compile_definitions(zb_fuse_client PRIVATE FUSE_USE_VERSION=35
        ZBSTORAGE_SOURCE_ROOT="${CMAKE_CURRENT_SOURCE_DIR}")
    target_link_libraries(zb_fuse_client
        PRIVATE
            zb_client_metrics
            zb_client_optical
            zb_proto
            brpc-static
            ${GFLAGS_LIBRARY}
            ${CMAKE_THREAD_LIBS_INIT}
            ${FUSE3_LIBRARIES}
    )
    if(BRPC_DYNAMIC_LIB)
        target_link_libraries(zb_fuse_client PRIVATE ${BRPC_DYNAMIC_LIB})
    endif()
    if(BUILD_TESTING)
        # External dependencies only: production client and optical node run as separate processes.
        add_executable(optical_external_services EXCLUDE_FROM_ALL tests/client/optical_external_services.cpp)
        target_include_directories(optical_external_services PRIVATE
            ${CMAKE_CURRENT_BINARY_DIR} ${ZBSTORAGE_BRPC_INCLUDE_DIRS})
        target_link_libraries(optical_external_services PRIVATE zb_proto brpc-static ${CMAKE_THREAD_LIBS_INIT})
        if(BRPC_DYNAMIC_LIB)
            target_link_libraries(optical_external_services PRIVATE ${BRPC_DYNAMIC_LIB})
        endif()

        set(REAL_NODE_TEST_SOURCES ${REAL_NODE_SOURCES})
        list(REMOVE_ITEM REAL_NODE_TEST_SOURCES src/data_node/real_node/server/real_node_server.cpp)
        add_executable(real_node_concurrent_test EXCLUDE_FROM_ALL tests/real_node/concurrent_io_test.cpp ${REAL_NODE_TEST_SOURCES})
        target_include_directories(real_node_concurrent_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src ${CMAKE_CURRENT_BINARY_DIR} ${ZBSTORAGE_BRPC_INCLUDE_DIRS})
        target_link_libraries(real_node_concurrent_test PRIVATE zb_proto brpc-static Threads::Threads ${GFLAGS_LIBRARY})
        if(BRPC_DYNAMIC_LIB)
            target_link_libraries(real_node_concurrent_test PRIVATE ${BRPC_DYNAMIC_LIB})
        endif()

        add_executable(concurrent_io_test EXCLUDE_FROM_ALL tests/client/concurrent_io_test.cpp
            src/data_node/real_node/io/IOExecutor.cpp)
        target_link_libraries(concurrent_io_test PRIVATE zb_batch_sdk)

        add_executable(disk_external_services EXCLUDE_FROM_ALL tests/client/disk_external_services.cpp)
        target_include_directories(disk_external_services PRIVATE
            ${CMAKE_CURRENT_BINARY_DIR} ${ZBSTORAGE_BRPC_INCLUDE_DIRS})
        target_link_libraries(disk_external_services PRIVATE zb_proto brpc-static Threads::Threads)
        if(BRPC_DYNAMIC_LIB)
            target_link_libraries(disk_external_services PRIVATE ${BRPC_DYNAMIC_LIB})
        endif()

        add_executable(client_optical_read_test EXCLUDE_FROM_ALL tests/client/client_optical_read_test.cpp)
        add_executable(optical_cache_lifetime_test EXCLUDE_FROM_ALL tests/client/optical_cache_lifetime_test.cpp)
        target_include_directories(optical_cache_lifetime_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src/data_node/optical_node/optical_node_manager)
        target_link_libraries(optical_cache_lifetime_test PRIVATE Threads::Threads)
        target_include_directories(client_optical_read_test PRIVATE ${FUSE3_INCLUDE_DIRS} ${GFLAGS_INCLUDE_PATH})
        target_compile_definitions(client_optical_read_test PRIVATE ZBSTORAGE_SOURCE_ROOT="${CMAKE_CURRENT_SOURCE_DIR}")
        target_link_libraries(client_optical_read_test PRIVATE zb_client_optical ${FUSE3_LIBRARIES} ${GFLAGS_LIBRARY})

        add_executable(client_io_latency_test EXCLUDE_FROM_ALL tests/client/client_io_latency_test.cpp)
        target_link_libraries(client_io_latency_test PRIVATE zb_client_metrics)
        target_compile_definitions(client_io_latency_test PRIVATE
            ZBSTORAGE_TEST_RESULTS="${CMAKE_CURRENT_SOURCE_DIR}/tests/client/results")

        add_executable(client_io_latency_integration_test EXCLUDE_FROM_ALL tests/client/client_io_latency_integration_test.cpp)
        target_include_directories(client_io_latency_integration_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
            ${GFLAGS_INCLUDE_PATH} ${FUSE3_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
        target_compile_options(client_io_latency_integration_test PRIVATE ${FUSE3_CFLAGS_OTHER})
        target_link_directories(client_io_latency_integration_test PRIVATE ${FUSE3_LIBRARY_DIRS})
        target_compile_definitions(client_io_latency_integration_test PRIVATE
            ZBSTORAGE_SOURCE_ROOT="${CMAKE_CURRENT_SOURCE_DIR}"
            ZBSTORAGE_TEST_RESULTS="${CMAKE_CURRENT_SOURCE_DIR}/tests/client/results")
        target_link_libraries(client_io_latency_integration_test PRIVATE
            zb_client_metrics zb_client_optical zb_proto brpc-static ${GFLAGS_LIBRARY} ${FUSE3_LIBRARIES})
        if(BRPC_DYNAMIC_LIB)
            target_link_libraries(client_io_latency_integration_test PRIVATE ${BRPC_DYNAMIC_LIB})
        endif()
    endif()
endif()

if(BUILD_TESTING)
    add_executable(optical_node_archive_integration_test EXCLUDE_FROM_ALL
        tests/optical_node/optical_node_archive_integration_test.cpp
        tests/optical_node/fake_services.cpp
        tests/optical_node/fake_client.cpp
        tests/optical_node/fake_real_node_data.cpp
        src/common/metrics/NodeMetricsCollector.cpp
        src/data_node/optical_node/config/OpticalNodeConfig.cpp
        src/data_node/optical_node/service/OpticalStorageServiceImpl.cpp
        src/data_node/optical_node/service/OpticalLibraryHandoff.cpp
        src/data_node/optical_node/service/BrpcOpticalNodeService.cpp
        src/data_node/optical_node/service/ReadEventSessionRegistry.cpp
    )
    target_include_directories(optical_node_archive_integration_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/src/data_node/optical_node/service
            ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
            ${GFLAGS_INCLUDE_PATH}
            ${CMAKE_CURRENT_BINARY_DIR}
    )
    target_link_libraries(optical_node_archive_integration_test
        PRIVATE
            optical_node_manager
            zb_proto
            brpc-static
            ${GFLAGS_LIBRARY}
            ${CMAKE_THREAD_LIBS_INIT}
    )
    if(BRPC_DYNAMIC_LIB)
        target_link_libraries(optical_node_archive_integration_test PRIVATE ${BRPC_DYNAMIC_LIB})
    endif()
    # 综合用例内含「节点上报」测试点：会 fork/exec 真 optical_node_server 子进程，故建立构建依赖。
    add_dependencies(optical_node_archive_integration_test optical_node_server)

    add_executable(optical_node_runtime_state_codec_test EXCLUDE_FROM_ALL
        tests/optical_node/optical_node_runtime_state_codec_test.cpp
    )
    target_include_directories(optical_node_runtime_state_codec_test
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src/data_node/optical_node/optical_node_manager
    )
endif()
