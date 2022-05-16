
file(GLOB_RECURSE ALL_BENCH_CPP *.cpp)

foreach(ONE_BENCH_CPP ${ALL_BENCH_CPP})

    get_filename_component(ONE_BENCH_EXEC ${ONE_BENCH_CPP} NAME_WE)
    set(TARGET_NAME Benchmark_${ONE_BENCH_EXEC})

    add_executable(${TARGET_NAME} ${ONE_BENCH_CPP} intersection.cpp multi_set_intersection.cpp)
    set_target_properties(${TARGET_NAME} PROPERTIES OUTPUT_NAME ${ONE_BENCH_EXEC})
    target_include_directories(${TARGET_NAME} PUBLIC ../include/benchmark/include ${CMAKE_SOURCE_DIR}/src)
    target_link_libraries(
    ${TARGET_NAME}
    PUBLIC
    CONAN_PKG::benchmark
    CONAN_PKG::roaring
    ${CMAKE_THREAD_LIBS_INIT}
    )

endforeach()