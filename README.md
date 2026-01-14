# Raft_Distributed_Consensus

INSTALLATION

~/vcpkg/vcpkg install protobuf:x64-linux grpc:x64-linux rocksdb:x64-linux boost-asio:x64-linux boost-system:x64-linux

Commands to Run

cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=~/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-linux

cmake --build build