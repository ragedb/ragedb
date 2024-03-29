cmake_minimum_required(VERSION 3.15)

project(ragedb VERSION "0.0.1" LANGUAGES CXX)
include(cmake/StandardProjectSettings.cmake)
include(cmake/PreventInSourceBuilds.cmake)

# Link this 'library' to set the c++ standard / compile-time options requested
add_library(project_options INTERFACE)
set(CMAKE_CXX_STANDARD 20)
target_compile_features(project_options INTERFACE cxx_std_20)
set_property(TARGET project_options PROPERTY CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(CMAKE_CXX_COMPILER_ID MATCHES ".*Clang")
  option(ENABLE_BUILD_WITH_TIME_TRACE "Enable -ftime-trace to generate time tracing .json files on clang" OFF)
  if(ENABLE_BUILD_WITH_TIME_TRACE)
    target_compile_options(project_options INTERFACE -ftime-trace)
  endif()
else()
  target_compile_options(project_options INTERFACE -fcoroutines)
endif()

# Add
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-march=native" COMPILER_SUPPORTS_MARCH_NATIVE)
if(COMPILER_SUPPORTS_MARCH_NATIVE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
endif()

# Link this 'library' to use the warnings specified in CompilerWarnings.cmake
add_library(project_warnings INTERFACE)

# enable cache system
include(cmake/Cache.cmake)

# standard compiler warning
include(cmake/CompilerWarnings.cmake)
set_project_warnings(project_warnings)

# sanitizer options if supported by compiler
include(cmake/Sanitizers.cmake)
enable_sanitizers(project_options)

# enable doxygen
include(cmake/Doxygen.cmake)
enable_doxygen()

# allow for static analysis options
include(cmake/StaticAnalyzers.cmake)

option(BUILD_SHARED_LIBS "Enable compilation of shared libraries" OFF)
option(ENABLE_TESTING "Enable Test Builds" ON)
option(ENABLE_FUZZING "Enable Fuzzing Builds" OFF)

# Very basic PCH example
option(ENABLE_PCH "Enable Precompiled Headers" OFF)
if(ENABLE_PCH)
  # This sets a global PCH parameter, each project will build its own PCH, which is a good idea if any #define's change
  #
  # consider breaking this out per project as necessary
  target_precompile_headers(
    project_options
    INTERFACE
    <vector>
    <string>
    <map>
    <utility>)
endif()

# Set up some extra Conan dependencies based on our needs before loading Conan
set(CONAN_EXTRA_REQUIRES "")
set(CONAN_EXTRA_OPTIONS "")
set(ENV{CONAN_REVISIONS_ENABLED} 1)

include(cmake/Conan.cmake)

run_conan()

if(ENABLE_TESTING)
  enable_testing()
  message("Building Tests. Be sure to check out test/constexpr_tests for constexpr testing")
  add_subdirectory(test)
endif()

if(ENABLE_FUZZING)
  message("Building Fuzz Tests, using fuzzing sanitizer https://www.llvm.org/docs/LibFuzzer.html")
  add_subdirectory(fuzz_test)
endif()

include(FetchContent)

find_package(Seastar REQUIRED)

option(ENABLE_UNITY "Enable Unity builds of projects" OFF)
if(ENABLE_UNITY)
  # Add for any project you want to apply unity builds for
  set_target_properties(ragedb PROPERTIES UNITY_BUILD ON)
endif()

file(COPY public DESTINATION bin)

add_subdirectory(src/graph)
include_directories(src/graph)

add_subdirectory(src/benchmark)
include_directories(src/benchmark)



add_executable(ragedb src/main/main.cpp src/main/util/stop_signal.hh
        src/main/handlers/HealthCheck.cpp src/main/handlers/HealthCheck.h
        src/main/handlers/Nodes.cpp src/main/handlers/Nodes.h
        src/main/handlers/Utilities.cpp src/main/handlers/Utilities.h
        src/main/json/JSON.h
        src/main/handlers/Schema.cpp src/main/handlers/Schema.h
        src/main/handlers/NodeProperties.cpp src/main/handlers/NodeProperties.h
        src/main/handlers/Relationships.cpp src/main/handlers/Relationships.h
        src/main/handlers/RelationshipProperties.cpp src/main/handlers/RelationshipProperties.h
        src/main/handlers/Degrees.cpp src/main/handlers/Degrees.h
        src/main/handlers/Neighbors.cpp src/main/handlers/Neighbors.h
        src/main/handlers/Lua.cpp src/main/handlers/Lua.h
        src/main/handlers/Restore.cpp src/main/handlers/Restore.h
        src/main/handlers/Connected.cpp src/main/handlers/Connected.h
        src/main/handlers/Management.cpp src/main/handlers/Management.h
        src/main/Database.cpp src/main/Database.h
        src/main/Databases.cpp src/main/Databases.h)

target_link_libraries(
        Graph
        project_options
        Seastar::seastar
        CONAN_PKG::tsl-sparse-map
        CONAN_PKG::simdjson
        CONAN_PKG::rapidcsv
        CONAN_PKG::vincentlaucsb-csv-parser
        CONAN_PKG::roaring
        CONAN_PKG::sol2
        CONAN_PKG::luajit
        CONAN_PKG::jfalcou-eve
        CONAN_PKG::cppcodec
        CONAN_PKG::cpr
        CONAN_PKG::boost
        CONAN_PKG::unordered_dense
#        CONAN_PKG::reckless # temporarily removing logging until ARM issue is figured out
)

target_link_libraries(
        ragedb
        PRIVATE project_options
        project_warnings
        Graph
)

