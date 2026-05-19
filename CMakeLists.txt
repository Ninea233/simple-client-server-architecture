cmake_minimum_required(VERSION 3.10)
project(ChatServerClient)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -O2")

# 服务端可执行文件
add_executable(server server.c)
target_link_libraries(server pthread)

# 客户端可执行文件
add_executable(client client.c)

add_executable(bench_test bench_test.c)
target_link_libraries(bench_test pthread)