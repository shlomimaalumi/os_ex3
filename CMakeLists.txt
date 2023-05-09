cmake_minimum_required(VERSION 3.16)
project(ex3)

set(CMAKE_CXX_STANDARD 14)

add_executable(ex3 Atomic/atomicdemo.cpp)
target_link_libraries(ex3 pthread)