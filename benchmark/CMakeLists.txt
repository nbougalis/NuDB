cmake_minimum_required (VERSION 3.2)

project (nudb)

############################################################

macro(append_flags name)
  foreach (arg ${ARGN})
    set(${name} "${${name}} ${arg}")
  endforeach()
endmacro()

############################################################

set(DEPS ${CMAKE_SOURCE_DIR}/../deps)
set(ROCKSDB ${DEPS}/rocksdb)
set(ROCKSDB_LIB ${ROCKSDB}/librocksdb.a)

set(Boost_USE_STATIC_LIBS ON)
set(Boost_USE_MULTITHREADED ON)
find_package(Boost REQUIRED COMPONENTS thread filesystem program_options system)
include_directories(${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIR})

add_definitions(
  -DBOOST_ALL_NO_LIB=1
  -DBOOST_SYSTEM_NO_DEPRECATED=1
  -D_XOPEN_SOURCE=600
  -D_GNU_SOURCE=1
  )

append_flags(CMAKE_CXX_FLAGS -std=c++14)

include_directories(
  ../include
  .
  ${DEPS}
  ${DEPS}/rocksdb
  ${DEPS}/rocksdb/include
  )

add_custom_target(librocksdb.a
  COMMAND make static_lib -j6
  WORKING_DIRECTORY ${ROCKSDB})

add_executable(benchmark ${DEPS}/xxHash/xxhash.c benchmark.cpp)

target_link_libraries(benchmark
  ${Boost_LIBRARIES}
  ${ROCKSDB_LIB}
  )

add_dependencies(benchmark librocksdb.a)
