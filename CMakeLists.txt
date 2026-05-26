cmake_minimum_required(VERSION 3.16)
project(ZBStorage C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

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
)
set(PROTOBUF_IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/src/msg ${Protobuf_INCLUDE_DIRS})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

add_library(zb_proto ${PROTO_SRCS} ${PROTO_HDRS})
target_include_directories(zb_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(zb_proto PUBLIC ${Protobuf_LIBRARIES})

set(REAL_NODE_SOURCES
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
    src/data_node/virtual_node/config/VirtualNodeConfig.cpp
    src/data_node/common/VirtualObjectStore.cpp
    src/data_node/real_node/service/ArchiveFileMetaStore.cpp
    src/data_node/real_node/service/ArchiveObjectMetaStore.cpp
    src/data_node/virtual_node/service/VirtualStorageServiceImpl.cpp
    src/data_node/virtual_node/service/BrpcVirtualStorageService.cpp
    src/data_node/virtual_node/server/virtual_node_server.cpp
)
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
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(virtual_node_server PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

add_executable(optical_node_server
    src/data_node/optical_node/config/OpticalNodeConfig.cpp
    src/data_node/optical_node/storage/ImageStore.cpp
    src/data_node/optical_node/service/OpticalStorageServiceImpl.cpp
    src/data_node/optical_node/service/BrpcOpticalStorageService.cpp
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
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(system_demo_tool
    PRIVATE
        zb_proto
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

add_executable(module_io_smoke_test
    tests/module_io_smoke_test.cpp
)
target_include_directories(module_io_smoke_test
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
        ${GFLAGS_INCLUDE_PATH}
        ${CMAKE_CURRENT_BINARY_DIR}
)
target_link_libraries(module_io_smoke_test
    PRIVATE
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(module_io_smoke_test PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

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

add_executable(pg_migration_sim_test
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
if(BUILD_TESTING)
    add_test(NAME pg_migration_sim_test COMMAND pg_migration_sim_test --sample_objects=1000)
endif()

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
    src/mds/masstree_meta/MasstreePageLayout.cpp
    src/mds/masstree_meta/MasstreePageReader.cpp
    src/mds/masstree_meta/MasstreeStatsStore.cpp
    src/mds/storage/MetaCodec.cpp
    src/mds/storage/UnifiedInodeRecord.cpp
    src/mds/storage/MetaStoreRouter.cpp
    src/mds/storage/RocksMetaStore.cpp
    src/mds/allocator/NodeStateCache.cpp
    src/mds/allocator/PGManager.cpp
    src/mds/allocator/ObjectAllocator.cpp
    src/mds/archive/ArchiveBatchStager.cpp
    src/mds/archive/ArchiveLeaseManager.cpp
    src/mds/archive/OpticalArchiveManager.cpp
    src/mds/service/MdsServiceImpl.cpp
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

add_executable(scheduler_server
    src/scheduler/config/SchedulerConfig.cpp
    src/scheduler/health/FailureDetector.cpp
    src/scheduler/model/ClusterState.cpp
    src/scheduler/lifecycle/NodeActuator.cpp
    src/scheduler/lifecycle/LifecycleManager.cpp
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
        zb_proto
        brpc-static
        ${GFLAGS_LIBRARY}
        ${CMAKE_THREAD_LIBS_INIT}
)
if(BRPC_DYNAMIC_LIB)
    target_link_libraries(scheduler_server PRIVATE ${BRPC_DYNAMIC_LIB})
endif()

if(UNIX)
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
    target_compile_definitions(zb_fuse_client PRIVATE FUSE_USE_VERSION=35)
    target_link_libraries(zb_fuse_client
        PRIVATE
            zb_proto
            brpc-static
            ${GFLAGS_LIBRARY}
            ${CMAKE_THREAD_LIBS_INIT}
            ${FUSE3_LIBRARIES}
    )
    if(BRPC_DYNAMIC_LIB)
        target_link_libraries(zb_fuse_client PRIVATE ${BRPC_DYNAMIC_LIB})
    endif()
endif()
