# Coverage instrumentation scoped to the GQL sources.
#
# The coverage target applies to src/gql only, so there is no reason to instrument the whole binary.
# The test executable statically links seastar, boost, iresearch, ICU, abseil and LuaJIT; instrumenting
# all of that inflates every object and makes the link far heavier than the measurement is worth --
# the stock ENABLE_COVERAGE path is Debug-only for exactly that reason, and a Debug tests link is the
# step that has run out of memory before.
#
# Instrumenting only the GQL translation units means coverage can be collected from an ordinary Release
# build: no Debug tree, and no `-g`, since gcov reads the .gcno files the compiler emits rather than
# DWARF. The instrumented sources drop to -O0 so line counts map to the source; everything else keeps
# the build's normal optimisation.
#
# Usage:  cmake -DGQL_COVERAGE=ON -DCMAKE_BUILD_TYPE=Release ...
#         cmake --build . --target tests && ./bin/tests "[gql_parser]"   # writes .gcda next to .gcno
#         llvm-cov gcov -- <objects>   (or gcovr/lcov filtered to src/gql)

option(GQL_COVERAGE "Instrument only the GQL sources (src/gql) for coverage reporting" OFF)

# Applies the coverage compile options to whichever of the given sources live under src/gql.
# Source-file properties are directory-scoped, so every directory that compiles GQL sources into a
# target has to call this for itself -- the root CMakeLists for `ragedb` and test/ for `tests`.
function(ragedb_instrument_gql_sources)
  if(NOT GQL_COVERAGE)
    return()
  endif()
  foreach(source ${ARGN})
    get_filename_component(absolute "${source}" ABSOLUTE)
    if(absolute MATCHES "/src/gql/" AND absolute MATCHES "\\.cpp$")
      set_source_files_properties("${source}" PROPERTIES COMPILE_OPTIONS "--coverage;-O0")
    endif()
  endforeach()
endfunction()

# Links the gcov runtime into a target that contains instrumented sources.
function(ragedb_link_gql_coverage target)
  if(NOT GQL_COVERAGE)
    return()
  endif()
  target_link_options(${target} PRIVATE --coverage)
endfunction()
