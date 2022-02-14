cmake_minimum_required(VERSION 3.4)

project(pmedis C)

set(CMAKE_BUILD_TYPE DEBUG)
set(PMEDIS_VERSION_MAJOR 0)
set(PMEDIS_VERSION_MEDIUM 0)
set(PMEDIS_VERSION_MINOR 1)

set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_definitions('-g')
add_definitions('-Wall')
add_definitions('-fPIC')

option(STATIC_COMPILE_ENABLE "Enable STATIC COMPILE" OFF)
if (STATIC_COMPILE_ENABLE)
    set(CMAKE_EXE_LINKER_FLAGS "-static -static-libgcc")
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

include_directories(${PROJECT_SOURCE_DIR}/include)

add_subdirectory(src)