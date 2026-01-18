cmake_minimum_required(VERSION 3.15)
project(RaftKV VERSION 1.0)

set(CMAKE_CXX_STANDARD 17)

# --- 1. FIND DEPENDENCIES (Vcpkg Style) ---
# We use 'CONFIG' to tell CMake to look for the vcpkg-provided config files
# rather than just searching raw paths.

find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED) 
find_package(Boost REQUIRED COMPONENTS system) 
find_package(Threads REQUIRED)
find_package(spdlog CONFIG REQUIRED)

# --- 2. GENERATE PROTOS ---
set(PROTO_SRC_DIR "${CMAKE_SOURCE_DIR}/protos")
set(PROTO_BINARY_DIR "${CMAKE_BINARY_DIR}/generated")
file(MAKE_DIRECTORY ${PROTO_BINARY_DIR})

# In Vcpkg, Protobuf targets are named slightly differently sometimes,
# but usually 'protobuf::protoc' is standard.
if(TARGET protobuf::protoc)
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
else()
    set(_PROTOBUF_PROTOC ${PROTOBUF_PROTOC_EXECUTABLE})
endif()

# Same for gRPC plugin
if(TARGET gRPC::grpc_cpp_plugin)
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE ${GRPC_CPP_PLUGIN_EXECUTABLE})
endif()

add_custom_command(
    OUTPUT "${PROTO_BINARY_DIR}/raft.pb.cc" "${PROTO_BINARY_DIR}/raft.pb.h"
           "${PROTO_BINARY_DIR}/raft.grpc.pb.cc" "${PROTO_BINARY_DIR}/raft.grpc.pb.h"
           "${PROTO_BINARY_DIR}/client.pb.cc" "${PROTO_BINARY_DIR}/client.pb.h"
    COMMAND ${_PROTOBUF_PROTOC}
    ARGS --grpc_out="${PROTO_BINARY_DIR}" --cpp_out="${PROTO_BINARY_DIR}"
         --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
         -I "${PROTO_SRC_DIR}"
         "${PROTO_SRC_DIR}/raft.proto" "${PROTO_SRC_DIR}/client.proto"
    DEPENDS "${PROTO_SRC_DIR}/raft.proto" "${PROTO_SRC_DIR}/client.proto"
)

# --- 3. DEFINE SOURCES ---
set(RAFT_SOURCES
    src/raft/node.cpp
    src/raft/election.cpp
    src/raft/replication.cpp
    src/raft/execution.cpp
    src/raft/request_handler.cpp
    src/rpc/grpc_service.cpp
    src/rpc/peer_client.cpp
    src/storage/rocksdb_store.cpp
    src/network/connection.cpp
    src/gateway/server.cpp
    src/gateway/session.cpp
    src/client/raft_client.cpp
    src/benchmark/benchmark.cpp

    src/utils/globals.cpp

    "${PROTO_BINARY_DIR}/raft.pb.cc"
    "${PROTO_BINARY_DIR}/raft.grpc.pb.cc"
    "${PROTO_BINARY_DIR}/client.pb.cc"
)

# --- 4. EXECUTABLE ---
add_executable(raft_kv
    src/main.cpp
    ${RAFT_SOURCES}
)

# --- 5. INCLUDES ---
target_include_directories(raft_kv PUBLIC
    include
    "${PROTO_BINARY_DIR}"
)

# --- 6. LINKING (Vcpkg Target Names) ---
target_link_libraries(raft_kv PUBLIC
    protobuf::libprotobuf   # Vcpkg standard name
    gRPC::grpc++            # Vcpkg standard name
    RocksDB::rocksdb        # Vcpkg standard name
    nlohmann_json::nlohmann_json
    Boost::system
    Threads::Threads
    spdlog::spdlog
)