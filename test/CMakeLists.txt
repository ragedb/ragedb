# Automatically enable catch2 to generate ctest targets
if(CONAN_CATCH2_ROOT_DEBUG)
  include(${CONAN_CATCH2_ROOT_DEBUG}/lib/cmake/Catch2/Catch.cmake)
else()
  include(${CONAN_CATCH2_ROOT}/lib/cmake/Catch2/Catch.cmake)
endif()

add_library(catch_main STATIC catch_main.cpp)
target_link_libraries(catch_main PUBLIC CONAN_PKG::catch2)
target_link_libraries(catch_main PRIVATE project_options)

find_package(Seastar REQUIRED)

add_executable(tests tests.cpp shard/ShardIds.cpp shard/Ids.cpp shard/NodeTypes.cpp Properties.cpp shard/Shards.cpp
        shard/Nodes.cpp shard/AllNodes.cpp shard/AllRelationships.cpp shard/NodeDegrees.cpp shard/NodeProperties.cpp
        shard/Relationships.cpp shard/RelationshipTypes.cpp shard/RelationshipProperties.cpp
        Link.cpp Node.cpp Graph.cpp
        types/Date.cpp

        ../src/main/util/stop_signal.hh
        ../src/main/handlers/HealthCheck.cpp ../src/main/handlers/HealthCheck.h
        ../src/main/handlers/Nodes.cpp ../src/main/handlers/Nodes.h
        ../src/main/handlers/Utilities.cpp ../src/main/handlers/Utilities.h
        ../src/main/json/JSON.h
        ../src/main/handlers/Schema.cpp ../src/main/handlers/Schema.h
        ../src/main/handlers/NodeProperties.cpp ../src/main/handlers/NodeProperties.h
        ../src/main/handlers/Relationships.cpp ../src/main/handlers/Relationships.h
        ../src/main/handlers/RelationshipProperties.cpp ../src/main/handlers/RelationshipProperties.h
        ../src/main/handlers/Degrees.cpp ../src/main/handlers/Degrees.h
        ../src/main/handlers/Neighbors.cpp ../src/main/handlers/Neighbors.h
        ../src/main/handlers/Lua.cpp ../src/main/handlers/Lua.h
        ../src/main/handlers/Restore.cpp ../src/main/handlers/Restore.h
        ../src/main/handlers/Connected.cpp ../src/main/handlers/Connected.h
        ../src/main/handlers/Management.cpp ../src/main/handlers/Management.h
        ../src/main/Database.cpp ../src/main/Database.h
        ../src/main/Databases.cpp ../src/main/Databases.h
        )
target_link_libraries(tests PRIVATE project_warnings project_options catch_main Graph Seastar::seastar)
target_include_directories(tests PRIVATE
        ../src/graph
        ../src/graph/eve
        ../src/graph/lua
        ../src/graph/peered
        ../src/graph/shard
        ../src/graph/types
        ../src/main
        ../src/main/handlers
        ../src/main/json
        ../src/main/util
        ../lua)
# automatically discover tests that are defined in catch based test files you can modify the unittests. Set TEST_PREFIX
# to whatever you want, or use different for different binaries
catch_discover_tests(
        tests
        TEST_PREFIX
        "unittests."
        REPORTER
        xml
        OUTPUT_DIR
        .
        OUTPUT_PREFIX
        "unittests."
        OUTPUT_SUFFIX
        .xml)

add_executable(tests2 main/ConnectedHandler.cpp
        ../src/main/util/stop_signal.hh
        ../src/main/handlers/HealthCheck.cpp ../src/main/handlers/HealthCheck.h
        ../src/main/handlers/Nodes.cpp ../src/main/handlers/Nodes.h
        ../src/main/handlers/Utilities.cpp ../src/main/handlers/Utilities.h
        ../src/main/json/JSON.h
        ../src/main/handlers/Schema.cpp ../src/main/handlers/Schema.h
        ../src/main/handlers/NodeProperties.cpp ../src/main/handlers/NodeProperties.h
        ../src/main/handlers/Relationships.cpp ../src/main/handlers/Relationships.h
        ../src/main/handlers/RelationshipProperties.cpp ../src/main/handlers/RelationshipProperties.h
        ../src/main/handlers/Degrees.cpp ../src/main/handlers/Degrees.h
        ../src/main/handlers/Neighbors.cpp ../src/main/handlers/Neighbors.h
        ../src/main/handlers/Lua.cpp ../src/main/handlers/Lua.h
        ../src/main/handlers/Restore.cpp ../src/main/handlers/Restore.h
        ../src/main/handlers/Connected.cpp ../src/main/handlers/Connected.h
        ../src/main/handlers/Management.cpp ../src/main/handlers/Management.h
        ../src/main/Database.cpp ../src/main/Database.h
        ../src/main/Databases.cpp ../src/main/Databases.h
        )

target_compile_definitions (tests2 PRIVATE SEASTAR_TESTING_MAIN)
target_link_libraries(tests2 PRIVATE project_warnings project_options Graph Seastar::seastar Boost::unit_test_framework Seastar::seastar_testing)
target_include_directories(tests2 PRIVATE
        ../src/graph
        ../src/graph/eve
        ../src/graph/lua
        ../src/graph/peered
        ../src/graph/shard
        ../src/graph/types
        ../src/main
        ../src/main/handlers
        ../src/main/json
        ../src/main/util
        ../lua)