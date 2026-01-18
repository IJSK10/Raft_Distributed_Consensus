# Raft_Distributed_Consensus

INSTALLATION

~/vcpkg/vcpkg install nlohmann-json grpc protobuf rocksdb boost-system boost-asio:x64-linuxboost-system:x64-linux

or

~/vcpkg/vcpkg install

Commands to BUILD

cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=~/vcpkg/scripts/buildsystems/vcpkg.cmake -DCMAKE_BUILD_TYPE=Release

cmake --build build

COMMANDS TO RUN

./build/raft_kv