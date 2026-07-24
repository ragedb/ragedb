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
        shard/algorithms/Propagate.cpp
        Link.cpp Node.cpp Graph.cpp
        types/Date.cpp
        types/NTriplesParserTests.cpp
        types/NTriplesLoaderTests.cpp
        gql/GqlTokenizerTests.cpp
        gql/GqlValueTests.cpp
        gql/GqlValueFunctionsTests.cpp
        gql/GqlParserTests.cpp
        gql/GqlParserLinearQueryTests.cpp
        gql/GqlParserCallTests.cpp
        gql/GqlParserDdlTests.cpp
        gql/GqlParserScalarCaseTests.cpp
        gql/GqlParserNotPrecedenceTests.cpp
        gql/GqlParserQuantifierTests.cpp
        gql/optimizer/BasicOptimizerTests.cpp
        gql/optimizer/ContradictionPrunerTests.cpp
        gql/optimizer/JoinEliminatorTests.cpp
        gql/optimizer/LimitPushdownOptimizerTests.cpp
        gql/optimizer/DisjointConceptPrunerTests.cpp
        gql/optimizer/OptimizerUtilsTests.cpp
        gql/optimizer/OptimizerUtilsAnalysisTests.cpp
        gql/optimizer/EqualityJoinEliminatorTests.cpp
        gql/optimizer/AlgebraicRewriterTests.cpp
        gql/optimizer/CardinalityShortCircuiterTests.cpp
        gql/optimizer/SubsumptionPrunerTests.cpp
        gql/optimizer/AntiSemiJoinPromoterTests.cpp
        gql/optimizer/AntisymmetricLoopCollapserTests.cpp
        gql/optimizer/IrreflexiveContradictionPrunerTests.cpp
        gql/optimizer/EdgeContradictionPrunerTests.cpp
        gql/optimizer/OptionalNoOpPruningTests.cpp
        gql/optimizer/TransitiveFilterPropagatorTests.cpp
        gql/optimizer/DomainConstraintReasonerTests.cpp
        gql/optimizer/TransitiveReachabilityPrunerTests.cpp
        gql/optimizer/FunctionalDependencyPrunerTests.cpp
        gql/optimizer/AutomorphicSymmetryOptimizerTests.cpp
        gql/optimizer/Phase11_15OptimizerTests.cpp
        gql/optimizer_performance/ContradictionPerformanceTests.cpp
        gql/optimizer_performance/JoinEliminationPerformanceTests.cpp
        gql/optimizer_performance/AlgebraicPerformanceTests.cpp
        gql/optimizer_performance/CardinalityPerformanceTests.cpp
        gql/optimizer_performance/SubsumptionPerformanceTests.cpp
        gql/optimizer_performance/CompositeDomainPerformanceTests.cpp
        gql/optimizer_performance/TransitiveReachabilityPerformanceTests.cpp
        gql/optimizer_performance/FunctionalDependencyPerformanceTests.cpp
        gql/optimizer_performance/AutomorphicSymmetryPerformanceTests.cpp
        gql/optimizer_performance/SchemaReachabilityPerformanceTests.cpp
        gql/optimizer_performance/OptionalMatchPerformanceTests.cpp
        gql/optimizer_performance/DegreeConstraintPerformanceTests.cpp
        gql/optimizer_performance/UniqueJoinPerformanceTests.cpp
        gql/optimizer_performance/LimitPushdownPerformanceTests.cpp
        gql/optimizer/Phase16_21OptimizerTests.cpp
        gql/optimizer/AlglibOptimizerTests.cpp
        gql/optimizer/DistinctAggregateGuardTests.cpp
        gql/optimizer/SemanticDifferentialTests.cpp
        gql/optimizer_performance/TransitiveFilterPerformanceTests.cpp
        gql/optimizer_performance/EdgeContradictionPerformanceTests.cpp
        gql/optimizer_performance/AntiSemiJoinPerformanceTests.cpp
        gql/optimizer_performance/EqualityJoinPerformanceTests.cpp
        gql/optimizer_performance/DisjointConceptPerformanceTests.cpp
        gql/optimizer_performance/DirectionSwapPerformanceTests.cpp
        gql/optimizer_performance/AlglibPerformanceTests.cpp
        gql/GqlExecutorReadTests.cpp
        gql/GqlExecutorWithPipelineTests.cpp
        gql/GqlExecutorLimitPushdownTests.cpp
        gql/GqlExecutorComplexTests.cpp
        gql/GqlExecutorSubqueryTests.cpp
        gql/GqlExecutorExplainProfileTests.cpp
        gql/GqlExecutorHoneycombTests.cpp
        gql/StarJoinCandidateTests.cpp
        gql/GqlExecutorStarJoinTests.cpp
        gql/GqlExecutorFtsTests.cpp
        gql/GqlExecutorShortestPathTests.cpp
        gql/GqlExecutorKHopTests.cpp
        gql/GqlExecutorPatternTests.cpp
        gql/GqlExecutorWriteTests.cpp
        gql/GqlExecutorAggregationTests.cpp
        gql/GqlExecutorGroupedExpansionTests.cpp
        gql/GqlExecutorPagingTests.cpp
        gql/GqlExecutorCastLabelTests.cpp
        gql/GqlExecutorForTests.cpp
        gql/GqlExecutorScalarFunctionTests.cpp
        gql/GqlExecutorSchemaTests.cpp
        gql/GqlTypecheckerTests.cpp
        gql/GqlQueryCacheTests.cpp
        gql/GqlCacheInvalidationTests.cpp

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
        ../src/gql/GqlParserPrimary.cpp
        ../src/gql/GqlParserPatterns.cpp
        ../src/gql/GqlParserExpressions.cpp
        ../src/gql/GqlParserStatements.cpp
        ../src/gql/GqlOptimizer.cpp ../src/gql/GqlOptimizer.h
        ../src/gql/optimizer/OptimizerUtils.cpp ../src/gql/optimizer/OptimizerUtils.h
        ../src/gql/optimizer/OptimizerRewrites.cpp
        ../src/gql/optimizer/ContradictionPruner.cpp ../src/gql/optimizer/ContradictionPruner.h
        ../src/gql/optimizer/DomainConstraintReasoner.cpp ../src/gql/optimizer/DomainConstraintReasoner.h
        ../src/gql/optimizer/TransitiveReachabilityPruner.cpp ../src/gql/optimizer/TransitiveReachabilityPruner.h
        ../src/gql/optimizer/FunctionalDependencyPruner.cpp ../src/gql/optimizer/FunctionalDependencyPruner.h
        ../src/gql/optimizer/AutomorphicSymmetryOptimizer.cpp ../src/gql/optimizer/AutomorphicSymmetryOptimizer.h
        ../src/gql/optimizer/SchemaReachabilityPruner.cpp ../src/gql/optimizer/SchemaReachabilityPruner.h
        ../src/gql/optimizer/OptionalMatchPromoter.cpp ../src/gql/optimizer/OptionalMatchPromoter.h
        ../src/gql/optimizer/DegreeConstraintPruner.cpp ../src/gql/optimizer/DegreeConstraintPruner.h
        ../src/gql/optimizer/UniqueJoinEliminator.cpp ../src/gql/optimizer/UniqueJoinEliminator.h
        ../src/gql/optimizer/LimitPushdownOptimizer.cpp ../src/gql/optimizer/LimitPushdownOptimizer.h
        ../src/gql/optimizer/JoinEliminator.cpp ../src/gql/optimizer/JoinEliminator.h
        ../src/gql/optimizer/SubsumptionPruner.cpp ../src/gql/optimizer/SubsumptionPruner.h
        ../src/gql/optimizer/CardinalityShortCircuiter.cpp ../src/gql/optimizer/CardinalityShortCircuiter.h
        ../src/gql/optimizer/AlgebraicRewriter.cpp ../src/gql/optimizer/AlgebraicRewriter.h
        ../src/gql/optimizer/TransitiveFilterPropagator.cpp ../src/gql/optimizer/TransitiveFilterPropagator.h
        ../src/gql/optimizer/EdgeContradictionPruner.cpp ../src/gql/optimizer/EdgeContradictionPruner.h
        ../src/gql/optimizer/AntiSemiJoinPromoter.cpp ../src/gql/optimizer/AntiSemiJoinPromoter.h
        ../src/gql/optimizer/EqualityJoinEliminator.cpp ../src/gql/optimizer/EqualityJoinEliminator.h
        ../src/gql/optimizer/DisjointConceptPruner.cpp ../src/gql/optimizer/DisjointConceptPruner.h
        ../src/gql/optimizer/DirectionSwapOptimizer.cpp ../src/gql/optimizer/DirectionSwapOptimizer.h
        ../src/gql/optimizer/TransitivePathOptimizer.cpp ../src/gql/optimizer/TransitivePathOptimizer.h
        ../src/gql/optimizer/IrreflexiveContradictionPruner.cpp ../src/gql/optimizer/IrreflexiveContradictionPruner.h
        ../src/gql/optimizer/AntisymmetricLoopCollapser.cpp ../src/gql/optimizer/AntisymmetricLoopCollapser.h
        ../src/gql/optimizer/EquivalenceClassOptimizer.cpp ../src/gql/optimizer/EquivalenceClassOptimizer.h
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
        ../src/gql/executor/StreamingAggregates.cpp ../src/gql/executor/ExecutorInternal.h
        ../src/gql/GqlCatalog.cpp ../src/gql/GqlCatalog.h
        ../src/gql/GqlTypechecker.cpp ../src/gql/GqlTypechecker.h
        ../src/gql/GqlValue.cpp ../src/gql/GqlValue.h
        ../src/gql/GqlValueFunctions.cpp
        ../src/gql/GqlValueEvaluate.cpp
        ../src/gql/GqlValueSerialize.cpp
        ../src/gql/GqlWriter.cpp ../src/gql/GqlWriter.h
        ../src/gql/GqlAst.h
        )
target_link_libraries(tests PRIVATE project_warnings project_options catch_main Graph Seastar::seastar)
# lld links the tests binary in a fraction of GNU ld's peak memory (149 TUs statically linked against
# seastar + iresearch), keeping the link under a 16GB box's budget where GNU ld OOM-killed it.
target_link_options(tests PRIVATE -fuse-ld=lld)
# Coverage is measured for the GQL sources only, so only those are instrumented -- the test binary
# compiles them from this directory, which is a separate source-property scope from the root list.
file(GLOB_RECURSE RAGEDB_TEST_GQL_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/../src/gql/*.cpp")
ragedb_instrument_gql_sources(${RAGEDB_TEST_GQL_SOURCES})
ragedb_link_gql_coverage(tests)
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
        ../src/gql/GqlParserPrimary.cpp
        ../src/gql/GqlParserPatterns.cpp
        ../src/gql/GqlParserExpressions.cpp
        ../src/gql/GqlParserStatements.cpp
        ../src/gql/GqlOptimizer.cpp ../src/gql/GqlOptimizer.h
        ../src/gql/optimizer/OptimizerUtils.cpp ../src/gql/optimizer/OptimizerUtils.h
        ../src/gql/optimizer/OptimizerRewrites.cpp
        ../src/gql/optimizer/ContradictionPruner.cpp ../src/gql/optimizer/ContradictionPruner.h
        ../src/gql/optimizer/DomainConstraintReasoner.cpp ../src/gql/optimizer/DomainConstraintReasoner.h
        ../src/gql/optimizer/TransitiveReachabilityPruner.cpp ../src/gql/optimizer/TransitiveReachabilityPruner.h
        ../src/gql/optimizer/FunctionalDependencyPruner.cpp ../src/gql/optimizer/FunctionalDependencyPruner.h
        ../src/gql/optimizer/AutomorphicSymmetryOptimizer.cpp ../src/gql/optimizer/AutomorphicSymmetryOptimizer.h
        ../src/gql/optimizer/SchemaReachabilityPruner.cpp ../src/gql/optimizer/SchemaReachabilityPruner.h
        ../src/gql/optimizer/OptionalMatchPromoter.cpp ../src/gql/optimizer/OptionalMatchPromoter.h
        ../src/gql/optimizer/DegreeConstraintPruner.cpp ../src/gql/optimizer/DegreeConstraintPruner.h
        ../src/gql/optimizer/UniqueJoinEliminator.cpp ../src/gql/optimizer/UniqueJoinEliminator.h
        ../src/gql/optimizer/LimitPushdownOptimizer.cpp ../src/gql/optimizer/LimitPushdownOptimizer.h
        ../src/gql/optimizer/JoinEliminator.cpp ../src/gql/optimizer/JoinEliminator.h
        ../src/gql/optimizer/SubsumptionPruner.cpp ../src/gql/optimizer/SubsumptionPruner.h
        ../src/gql/optimizer/CardinalityShortCircuiter.cpp ../src/gql/optimizer/CardinalityShortCircuiter.h
        ../src/gql/optimizer/AlgebraicRewriter.cpp ../src/gql/optimizer/AlgebraicRewriter.h
        ../src/gql/optimizer/TransitiveFilterPropagator.cpp ../src/gql/optimizer/TransitiveFilterPropagator.h
        ../src/gql/optimizer/EdgeContradictionPruner.cpp ../src/gql/optimizer/EdgeContradictionPruner.h
        ../src/gql/optimizer/AntiSemiJoinPromoter.cpp ../src/gql/optimizer/AntiSemiJoinPromoter.h
        ../src/gql/optimizer/EqualityJoinEliminator.cpp ../src/gql/optimizer/EqualityJoinEliminator.h
        ../src/gql/optimizer/DisjointConceptPruner.cpp ../src/gql/optimizer/DisjointConceptPruner.h
        ../src/gql/optimizer/DirectionSwapOptimizer.cpp ../src/gql/optimizer/DirectionSwapOptimizer.h
        ../src/gql/optimizer/TransitivePathOptimizer.cpp ../src/gql/optimizer/TransitivePathOptimizer.h
        ../src/gql/optimizer/IrreflexiveContradictionPruner.cpp ../src/gql/optimizer/IrreflexiveContradictionPruner.h
        ../src/gql/optimizer/AntisymmetricLoopCollapser.cpp ../src/gql/optimizer/AntisymmetricLoopCollapser.h
        ../src/gql/optimizer/EquivalenceClassOptimizer.cpp ../src/gql/optimizer/EquivalenceClassOptimizer.h
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
        ../src/gql/executor/StreamingAggregates.cpp ../src/gql/executor/ExecutorInternal.h
        ../src/gql/GqlCatalog.cpp ../src/gql/GqlCatalog.h
        ../src/gql/GqlTypechecker.cpp ../src/gql/GqlTypechecker.h
        ../src/gql/GqlValue.cpp ../src/gql/GqlValue.h
        ../src/gql/GqlValueFunctions.cpp
        ../src/gql/GqlValueEvaluate.cpp
        ../src/gql/GqlValueSerialize.cpp
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