cmake_minimum_required(VERSION 3.8)

include(FindPackageHandleStandardArgs)
include(CheckSymbolExists)

project(untitled3)

set(CMAKE_CXX_STANDARD 11)

set(INC_FILES ${INC_FILES}
        atomic/atomic.hpp
        atomic/atomic_std.hpp
        atomic/atomic_intrinsics.hpp
        atomic/atomic_intrinsics_gcc.hpp
        hdr_histogram/hdr_histogram.cpp
        hdr_histogram/hdr_histogram.hpp
        scoped/scoped_ptr.hpp
        scoped/scoped_lock.hpp
        util/ref_counted.hpp
        util/macros.hpp
        util/utils.hpp
        util/utils.cpp
        metrics/metrics.hpp
        properties/propertyutil.cpp
        properties/propertyutil.h)

set(INC_FILES ${INC_FILES}
        connection_factory.hpp
        connection.hpp
        producer.hpp
        consumer.hpp
        message_delivery.hpp)

set(SOURCE_FILES test_client.cpp ${INC_FILES})

set(_LIBUV_ROOT_PATHS "${_LIBUV_ROOT_PATHS}" "/usr/" "/usr/local/")

set(_LIBUV_ROOT_HINTS_AND_PATHS HINTS
        HINTS ${_LIBUV_ROOT_HINTS}
        PATHS ${_LIBUV_ROOT_PATHS})

# Ensure libuv was found
find_path(LIBUV_INCLUDE_DIR
        NAMES uv.h
        HINTS ${_LIBUV_INCLUDEDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
        PATH_SUFFIXES include)
find_library(LIBUV_LIBRARY
        NAMES uv libuv
        HINTS ${_LIBUV_LIBDIR} ${_LIBUV_ROOT_HINTS_AND_PATHS}
        PATH_SUFFIXES lib)
find_package_handle_standard_args(Libuv "Could NOT find libuv, try to set the path to the libuv root folder in the system variable LIBUV_ROOT_DIR"
        LIBUV_LIBRARY
        LIBUV_INCLUDE_DIR)

# Assign libuv include and libraries
set(IG_INCLUDES ${IG_INCLUDES} ${LIBUV_INCLUDE_DIR})
set(IG_LIBS ${IG_LIBS} ${LIBUV_LIBRARY})



file(GLOB LIBRARIES "/usr/local/lib64/libqpid*.dylib")

message("LIBRARIES = ${LIBRARIES}")

set(IG_LIBS ${IG_LIBS} ${LIBRARIES})


include_directories(${IG_INCLUDES})


add_executable(test_client ${SOURCE_FILES})


target_link_libraries(test_client ${IG_LIBS})
