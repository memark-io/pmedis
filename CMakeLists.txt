# Copyright 2022 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.4)

project(pmedis C)

# set(CMAKE_BUILD_TYPE Release)
set(PMEDIS_VERSION_MAJOR 0)
set(PMEDIS_VERSION_MEDIUM 0)
set(PMEDIS_VERSION_MINOR 1)

set(CMAKE_DISABLE_IN_SOURCE_BUILD ON)

add_definitions('-fPIC')

if (CMAKE_BUILD_TYPE STREQUAL "Release")
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
elseif (CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")
elseif (CMAKE_BUILD_TYPE STREQUAL "FastDebug")
    set(CMAKE_CXX_FLAGS "-g -O2 -march=native")
    set(CMAKE_C_FLAGS "-g -O2 -march=native")
else ()
    message(FATAL_ERROR "Invalid build type!")
endif ()

option(STATIC_COMPILE_ENABLE "Enable STATIC COMPILE" OFF)
if (STATIC_COMPILE_ENABLE)
    set(CMAKE_EXE_LINKER_FLAGS "-static -static-libgcc")
endif()

set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

include_directories(${PROJECT_SOURCE_DIR}/include)
include_directories(${PROJECT_SOURCE_DIR}/kvdk/include)

add_subdirectory(src)