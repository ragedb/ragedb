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
    catch2/2.13.9
    docopt.cpp/0.6.3
    fmt/8.0.1
    spdlog/1.9.2
    luajit/2.1.0-beta3-2022-7-22
    sol2/3.2.3-luajit
    tsl-sparse-map/0.6.2
    simdjson/2.2.0
    roaring/0.5.0
    jfalcou-eve/v2022.03.0
    cppcodec/0.2
    cpr/1.9.0
    reckless/3.0.3
    benchmark/1.6.2
    OPTIONS
    ${CONAN_EXTRA_OPTIONS}
    BASIC_SETUP
    CMAKE_TARGETS # individual targets to link to
    BUILD
    missing)
endmacro()
