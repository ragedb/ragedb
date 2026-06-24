find_package(Catch2 REQUIRED)
include(Catch)

add_library(catch_main STATIC catch_main.cpp)
target_link_libraries(catch_main PUBLIC Catch2::Catch2 Seastar::seastar)
target_link_libraries(catch_main PRIVATE project_options)

find_package(Seastar REQUIRED)

add_executable(tests tests.cpp shard/ShardIds.cpp shard/Ids.cpp shard/NodeTypes.cpp Properties.cpp shard/Shards.cpp
        shard/Nodes.cpp shard/AllNodes.cpp shard/AllRelationships.cpp shard/NodeDegrees.cpp shard/NodeProperties.cpp
        shard/Relationships.cpp shard/RelationshipTypes.cpp shard/RelationshipProperties.cpp shard/Index.cpp shard/ShortestPath.cpp shard/KHop.cpp
        shard/algorithms/Connectivity.cpp
        shard/algorithms/Centrality.cpp
        shard/algorithms/Similarity.cpp
        shard/algorithms/Community.cpp
        shard/algorithms/Clustering.cpp
        shard/algorithms/Distance.cpp
        Link.cpp Node.cpp Graph.cpp
        types/Date.cpp
        gql/GqlTokenizerTests.cpp
        gql/GqlParserTests.cpp
        gql/GqlOptimizerTests.cpp
        gql/GqlOptimizerPerformanceTests.cpp
        gql/GqlExecutorReadTests.cpp
        gql/GqlExecutorComplexTests.cpp
        gql/GqlExecutorSubqueryTests.cpp
        gql/GqlExecutorExplainProfileTests.cpp
        gql/GqlExecutorHoneycombTests.cpp
        gql/GqlExecutorFtsTests.cpp
        gql/GqlExecutorShortestPathTests.cpp
        gql/GqlExecutorKHopTests.cpp
        gql/GqlExecutorPatternTests.cpp
        gql/GqlExecutorWriteTests.cpp
        gql/GqlExecutorAggregationTests.cpp
        gql/GqlExecutorSchemaTests.cpp
        gql/GqlTypecheckerTests.cpp
        gql/GqlQueryCacheTests.cpp

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
        ../src/main/handlers/Gql.cpp ../src/main/handlers/Gql.h
        ../src/gql/GqlLexer.cpp ../src/gql/GqlLexer.h
        ../src/gql/GqlParser.cpp ../src/gql/GqlParser.h
        ../src/gql/GqlOptimizer.cpp ../src/gql/GqlOptimizer.h
        ../src/gql/GqlExecutor.cpp ../src/gql/GqlExecutor.h
        ../src/gql/GqlQueryCache.cpp ../src/gql/GqlQueryCache.h
        ../src/gql/executor/FactorNode.cpp ../src/gql/executor/FactorNode.h
        ../src/gql/executor/JoinHelpers.cpp ../src/gql/executor/JoinHelpers.h
        ../src/gql/executor/ExpressionEvaluator.cpp ../src/gql/executor/ExpressionEvaluator.h
        ../src/gql/executor/PathTraverser.cpp ../src/gql/executor/PathTraverser.h
        ../src/gql/executor/ProjectionPruner.cpp ../src/gql/executor/ProjectionPruner.h
        ../src/gql/executor/StarJoinRewriter.cpp ../src/gql/executor/StarJoinRewriter.h
        ../src/gql/executor/PlanBuilder.cpp ../src/gql/executor/PlanBuilder.h
        ../src/gql/executor/HoneycombExecutor.cpp ../src/gql/executor/HoneycombExecutor.h
        ../src/gql/executor/LftjExecutor.cpp ../src/gql/executor/LftjExecutor.h
        ../src/gql/GqlCatalog.cpp ../src/gql/GqlCatalog.h
        ../src/gql/GqlTypechecker.cpp ../src/gql/GqlTypechecker.h
        ../src/gql/GqlValue.cpp ../src/gql/GqlValue.h
        ../src/gql/GqlWriter.cpp ../src/gql/GqlWriter.h
        ../src/gql/GqlAst.h
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
        ../src/gql
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
        ../src/main/handlers/Gql.cpp ../src/main/handlers/Gql.h
        ../src/gql/GqlLexer.cpp ../src/gql/GqlLexer.h
        ../src/gql/GqlParser.cpp ../src/gql/GqlParser.h
        ../src/gql/GqlOptimizer.cpp ../src/gql/GqlOptimizer.h
        ../src/gql/GqlExecutor.cpp ../src/gql/GqlExecutor.h
        ../src/gql/GqlQueryCache.cpp ../src/gql/GqlQueryCache.h
        ../src/gql/executor/FactorNode.cpp ../src/gql/executor/FactorNode.h
        ../src/gql/executor/JoinHelpers.cpp ../src/gql/executor/JoinHelpers.h
        ../src/gql/executor/ExpressionEvaluator.cpp ../src/gql/executor/ExpressionEvaluator.h
        ../src/gql/executor/PathTraverser.cpp ../src/gql/executor/PathTraverser.h
        ../src/gql/executor/ProjectionPruner.cpp ../src/gql/executor/ProjectionPruner.h
        ../src/gql/executor/StarJoinRewriter.cpp ../src/gql/executor/StarJoinRewriter.h
        ../src/gql/executor/PlanBuilder.cpp ../src/gql/executor/PlanBuilder.h
        ../src/gql/executor/HoneycombExecutor.cpp ../src/gql/executor/HoneycombExecutor.h
        ../src/gql/executor/LftjExecutor.cpp ../src/gql/executor/LftjExecutor.h
        ../src/gql/GqlCatalog.cpp ../src/gql/GqlCatalog.h
        ../src/gql/GqlTypechecker.cpp ../src/gql/GqlTypechecker.h
        ../src/gql/GqlValue.cpp ../src/gql/GqlValue.h
        ../src/gql/GqlWriter.cpp ../src/gql/GqlWriter.h
        ../src/gql/GqlAst.h
        )

target_compile_definitions (tests2 PRIVATE SEASTAR_TESTING_MAIN)
find_library(SYSTEM_BOOST_TEST NAMES boost_unit_test_framework PATHS /usr/lib/x86_64-linux-gnu /usr/lib NO_DEFAULT_PATH)
target_link_libraries(tests2 PRIVATE project_warnings project_options Graph Seastar::seastar Seastar::seastar_testing ${SYSTEM_BOOST_TEST})
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
        ../src/gql
        ../lua)