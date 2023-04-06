macro(run_conan)
  # Download automatically, you can also just copy the conan.cmake file
  if(NOT EXISTS "${CMAKE_BINARY_DIR}/conan.cmake")
    message(STATUS "Downloading conan.cmake from https://github.com/conan-io/cmake-conan")
    file(DOWNLOAD "https://github.com/conan-io/cmake-conan/raw/v0.15/conan.cmake" "${CMAKE_BINARY_DIR}/conan.cmake")
  endif()

  include(${CMAKE_BINARY_DIR}/conan.cmake)

  conan_add_remote(
    NAME
    bincrafters
    URL
    https://bincrafters.jfrog.io/artifactory/api/conan/public-conan)

  conan_cmake_run(
    REQUIRES
    ${CONAN_EXTRA_REQUIRES}
    catch2/2.13.10
    docopt.cpp/0.6.3
    fmt/9.1.0
    spdlog/1.11.0
#   sol2/3.2.3-luajit
    sol2/3.3.0-exhaustive
    tsl-sparse-map/0.6.2
    simdjson/2.2.2
    rapidcsv/8.64
    vincentlaucsb-csv-parser/2.1.3
    roaring/0.8.0
    jfalcou-eve/v2022.09.1
    cppcodec/0.2
    cpr/1.10.1
    boost/1.81.0
    unordered_dense/3.1.1
#    reckless/3.0.3  # temporarily removing logging until ARM issue is figured out
    benchmark/1.7.1
    OPTIONS
    ${CONAN_EXTRA_OPTIONS}
    BASIC_SETUP
    CMAKE_TARGETS # individual targets to link to
    BUILD
    missing)
endmacro()
