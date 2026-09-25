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

add_library(optical_node_manager STATIC
    src/data_node/optical_node/optical_node_manager/optical_node_manager.cpp
    src/data_node/optical_node/optical_node_manager/cd_manager_sim/cd_manager.cpp
    src/data_node/optical_node/optical_node_manager/space_manager/image_dir_manager.cpp
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
    src/data_node/optical_node/config/OpticalNodeConfig.cpp
    src/data_node/optical_node/service/OpticalStorageServiceImpl.cpp
    src/data_node/optical_node/service/BrpcOpticalNodeService.cpp
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
        add_executable(client_io_latency_test tests/client/client_io_latency_test.cpp)
        target_link_libraries(client_io_latency_test PRIVATE zb_client_metrics)
        target_compile_definitions(client_io_latency_test PRIVATE
            ZBSTORAGE_TEST_RESULTS="${CMAKE_CURRENT_SOURCE_DIR}/tests/client/results")
        add_test(NAME client_io_latency_test COMMAND client_io_latency_test)

        add_executable(client_io_latency_integration_test tests/client/client_io_latency_integration_test.cpp)
        target_include_directories(client_io_latency_integration_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/src ${ZBSTORAGE_BRPC_INCLUDE_DIRS}
            ${GFLAGS_INCLUDE_PATH} ${FUSE3_INCLUDE_DIRS} ${CMAKE_CURRENT_BINARY_DIR})
        target_compile_options(client_io_latency_integration_test PRIVATE ${FUSE3_CFLAGS_OTHER})
        target_link_directories(client_io_latency_integration_test PRIVATE ${FUSE3_LIBRARY_DIRS})
        target_compile_definitions(client_io_latency_integration_test PRIVATE
            ZBSTORAGE_SOURCE_ROOT="${CMAKE_CURRENT_SOURCE_DIR}"
            ZBSTORAGE_TEST_RESULTS="${CMAKE_CURRENT_SOURCE_DIR}/tests/client/results")
        target_link_libraries(client_io_latency_integration_test PRIVATE
            zb_client_metrics zb_proto brpc-static ${GFLAGS_LIBRARY} ${FUSE3_LIBRARIES})
        if(BRPC_DYNAMIC_LIB)
            target_link_libraries(client_io_latency_integration_test PRIVATE ${BRPC_DYNAMIC_LIB})
        endif()
        add_test(NAME client_io_latency_integration_test COMMAND client_io_latency_integration_test)
        set_tests_properties(client_io_latency_test client_io_latency_integration_test PROPERTIES TIMEOUT 60)
        find_package(Python3 COMPONENTS Interpreter QUIET)
        if(Python3_Interpreter_FOUND)
            add_test(NAME client_io_latency_workflow_test
                COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/client/client_io_latency_workflow_test.py)
            set_tests_properties(client_io_latency_workflow_test PROPERTIES TIMEOUT 60)
            add_test(NAME client_io_latency_mount_test
                COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/client/client_io_latency_mount_test.py
                    --client $<TARGET_FILE:zb_fuse_client> --fixture $<TARGET_FILE:client_io_latency_integration_test>)
            set_tests_properties(client_io_latency_mount_test PROPERTIES TIMEOUT 60 SKIP_RETURN_CODE 77)
        endif()
    endif()
endif()

# 光存储节点归档集成测试：进程内起 fake real_node / scheduler 与真光节点服务，
# 走完 归档下载 → 压缩 → 卷镜像封装 → 打包汇报 → CD_BURN → 数据面读 / CD_READ 重载 全链路。
if(BUILD_TESTING)
    add_executable(optical_node_archive_integration_test
        tests/optical_node/optical_node_archive_integration_test.cpp
        tests/optical_node/fake_services.cpp
        tests/optical_node/fake_client.cpp
        tests/optical_node/fake_real_node_data.cpp
        src/data_node/optical_node/config/OpticalNodeConfig.cpp
        src/data_node/optical_node/service/OpticalStorageServiceImpl.cpp
        src/data_node/optical_node/service/BrpcOpticalNodeService.cpp
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

    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_Interpreter_FOUND)
        # smoke：单卷最小闭环，分钟级；full：6 卷（容量 5 的 LRU 淘汰 + CD_READ 重载），十分钟级。
        add_test(NAME optical_node_archive_smoke
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/optical_node/run_optical_node_archive_test.py
                --driver $<TARGET_FILE:optical_node_archive_integration_test> --scenario smoke)
        set_tests_properties(optical_node_archive_smoke PROPERTIES TIMEOUT 300 LABELS "optical_node;slow")
        add_test(NAME optical_node_archive_full
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/optical_node/run_optical_node_archive_test.py
                --driver $<TARGET_FILE:optical_node_archive_integration_test> --scenario full)
        set_tests_properties(optical_node_archive_full PROPERTIES TIMEOUT 1500 LABELS "optical_node;slow" SKIP_RETURN_CODE 77)
    endif()
endif()
