cmake_minimum_required(VERSION 3.15)
project(RaftBanking)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# -----------------------------------------------------------------------------
# 1. FIND DEPENDENCIES (via vcpkg)
# -----------------------------------------------------------------------------
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(RocksDB CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS system) # Asio relies on system
find_package(Threads REQUIRED)

message(STATUS "Found Protobuf: ${Protobuf_VERSION}")
message(STATUS "Found gRPC: ${gRPC_VERSION}")
message(STATUS "Found RocksDB: ${RocksDB_VERSION}")

# -----------------------------------------------------------------------------
# 2. PROTOBUF & GRPC GENERATION
# -----------------------------------------------------------------------------
# Define where .proto files live
set(PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
file(GLOB PROTO_FILES "${PROTO_DIR}/*.proto")

# Helper to find the gRPC C++ plugin
get_target_property(grpc_cpp_plugin gRPC::grpc_cpp_plugin LOCATION)

# A. Generate Standard Protobuf Sources (.pb.cc / .pb.h)
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})

# B. Generate gRPC Service Sources (.grpc.pb.cc / .grpc.pb.h)
# Only raft.proto needs gRPC services, but running on all doesn't hurt
set(GRPC_SRCS "")
set(GRPC_HDRS "")
foreach(FIL ${PROTO_FILES})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)
    
    list(APPEND GRPC_SRCS "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.grpc.pb.cc")
    list(APPEND GRPC_HDRS "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.grpc.pb.h")
    
    add_custom_command(
        OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.grpc.pb.cc"
               "${CMAKE_CURRENT_BINARY_DIR}/${FIL_WE}.grpc.pb.h"
        COMMAND ${Protobuf_PROTOC_EXECUTABLE}
        ARGS --grpc_out=${CMAKE_CURRENT_BINARY_DIR}
             --cpp_out=${CMAKE_CURRENT_BINARY_DIR}
             --plugin=protoc-gen-grpc=${grpc_cpp_plugin}
             -I ${PROTO_DIR}
             ${ABS_FIL}
        DEPENDS ${ABS_FIL} ${Protobuf_PROTOC_EXECUTABLE} ${grpc_cpp_plugin}
    )
endforeach()

# -----------------------------------------------------------------------------
# 3. INCLUDE DIRECTORIES
# -----------------------------------------------------------------------------
include_directories(
    ${CMAKE_SOURCE_DIR}/include        # Your manual headers
    ${CMAKE_CURRENT_BINARY_DIR}        # Generated Proto headers
    ${Protobuf_INCLUDE_DIRS}
    ${RocksDB_INCLUDE_DIRS}
)

# -----------------------------------------------------------------------------
# 4. SHARED NETWORK LIBRARY (TCP Logic)
# -----------------------------------------------------------------------------
# This lib contains the shared TCP framing logic and the generated Proto objects.
# Both Client and Server link to this.
add_library(network_lib
    src/network/connection.cpp
    ${PROTO_SRCS}
    ${PROTO_HDRS}
    ${GRPC_SRCS}
    ${GRPC_HDRS}
)

target_link_libraries(network_lib
    PUBLIC
    Boost::system
    protobuf::libprotobuf
    gRPC::grpc++
)

# -----------------------------------------------------------------------------
# 5. EXECUTABLE: RAFT SERVER
# -----------------------------------------------------------------------------
add_executable(raft_server
    src/main.cpp
    
    # Raft Modules
    src/raft/node.cpp
    src/raft/request_handler.cpp
    src/raft/election.cpp
    src/raft/replication.cpp
    src/raft/executor.cpp
    
    # Gateway Modules
    src/gateway/server.cpp
    src/gateway/session.cpp
    
    # Storage & RPC
    src/storage/rocksdb_store.cpp
    src/rpc/grpc_service.cpp
    src/rpc/peer_client.cpp
)

target_link_libraries(raft_server
    PRIVATE
    network_lib           # Use shared TCP/Proto code
    RocksDB::rocksdb
    Threads::Threads
)

# -----------------------------------------------------------------------------
# 6. EXECUTABLE: CLIENT CLI
# -----------------------------------------------------------------------------
add_executable(raft_client
    src/client/main.cpp
)

target_link_libraries(raft_client
    PRIVATE
    network_lib           # Use shared TCP/Proto code
    Threads::Threads
)