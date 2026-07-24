/*
 * Copyright RageDB Contributors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef RAGEDB_PATH_TRAVERSER_H
#define RAGEDB_PATH_TRAVERSER_H

#include <vector>
#include <string>
#include <set>
#include <map>
#include <optional>
#include <seastar/core/future.hh>
#include <functional>
#include "../graph/Graph.h"
#include "GqlAst.h"
#include "FactorNode.h"
#include "ProjectionPruner.h"

/**
 * @brief Handles vertex and edge traversals, start node index lookups, and factorized match execution.
 * 
 * Example Queries utilizing PathTraverser:
 * 
 * 1. Path Traversals and Variable Length patterns:
 *    MATCH (a:Person {name: 'Alice'})-[:FRIEND*1..3]->(b)
 *    RETURN b
 *    PathTraverser looks up start nodes (Alice) via indices, and performs
 *    multi-hop variable-length traversals matching constraints along the way.
 */
namespace ragedb::gql {

/**
 * @brief Page size for chunked start-node scans: edge patterns scan the label in pages of this
 *        many nodes instead of materialising the whole label up front. Tests shrink it
 *        to exercise chunk boundaries on small graphs.
 */
inline size_t gql_scan_chunk_size = 65536;

/**
 * @brief Retained tuning knob for streamed traversals. The streamed drive is row-by-row (each
 *        frontier row's expansion drains to the sink through all remaining hops before the next
 *        row expands), so this no longer bounds anything by itself; it is kept for tests and as
 *        the batching knob if bounded-concurrency expansion is added later.
 */
inline size_t gql_stream_chunk_size = 256;

// Max incoming rows driven concurrently through a streamed traversal: the per-row drives
// await graph I/O independently, so bounding the in-flight count keeps memory O(concurrency x chunk)
// while overlapping the latency of a large piped frontier (FoF expansion) instead of serialising it.
inline size_t gql_stream_concurrency = 32;

// Max rows driven concurrently at each INNER step of a multi-match chain: a chain step's
// output (e.g. a friend's forums) fans into the next match (each forum's posts) concurrently. Kept
// small so the nested product with gql_stream_concurrency stays bounded (independent semaphores per
// level, so no cross-level deadlock).
inline size_t gql_stream_inner_concurrency = 8;

// Max (start, end) pairs searched concurrently by a shortest-path selector (ANY/ALL SHORTEST and the
// CHEAPEST forms). Each pair runs its own BFS and holds its own frontier plus result paths, so an
// unbounded fan-out over the candidate cartesian product makes peak memory O(pairs) -- a pattern with
// many candidate endpoints (one person to every Person of a given first name) exhausts the heap.
// Bounding the in-flight searches keeps it O(concurrency).
inline size_t gql_shortest_path_concurrency = 8;

// Max relationship branches expanded concurrently at a single node of a var-length traversal. Every
// branch carries its own copy of the path built so far (the nodes and relationships along it, with
// their property maps), so expanding all of a high-degree node's branches at once made live memory the
// product of that degree and the path length.
inline size_t gql_var_len_branch_concurrency = 16;

// Completed var-length paths handed to a streaming consumer per batch. Bounds how many finished paths
// are held before the consumer folds them away.
inline size_t gql_var_len_hop_batch = 1024;

// Two guards for an UNBOUNDED var-length pattern ({n,} or *) under the WALK path mode, which has no finite
// result on cyclic data (a walk may re-cross an edge). Both apply ONLY to that case -- a bounded quantifier
// is stopped by its own upper bound, and a restrictor (TRAIL / ACYCLIC / SIMPLE) makes the path set finite,
// so neither guard touches them. Tests shrink these to force the limits.
//
//  - depth cap: catches a low-branching cycle (e.g. a 2-node ring walking A-B-A-B...), which recurses
//    forever at ~one path per depth without exploding in count.
inline uint64_t gql_var_len_walk_depth_cap = 100;
//  - path budget: catches an exponential blow-up (a dense/clique-like cycle), where the path COUNT
//    explodes at shallow depths and would exhaust the heap long before the depth cap. Bounds the total
//    paths recorded across the whole expansion.
inline uint64_t gql_var_len_walk_path_budget = 5'000'000;

/**
 * @brief Consumer for streamed traversal output: when passed to traverse_path_pattern, matched
 *        rows are handed to consume() in bounded batches instead of being collected and returned
 *        (the traversal then resolves with an empty vector). Batches arrive sequentially.
 */
struct GqlRowSink {
    std::function<seastar::future<>(std::vector<GqlRow>)> consume;
};

bool satisfies_match_path_modes(const GqlRow& row, MatchMode match_mode, PathMode path_mode, const PathPattern& pattern);

seastar::future<std::vector<Node>> get_start_nodes(
    ragedb::Graph& graph,
    const PatternNode& node,
    size_t limit = 0,
    const ProjectionPruner& pruner = {},
    std::string sort_property = "",
    bool sort_ascending = true,
    bool sort_by_id = false
);

seastar::future<std::vector<GqlRow>> traverse_path_pattern(
    ragedb::Graph& graph,
    const PathPattern& pattern,
    const GqlRow& base_row,
    size_t limit = 0,
    const ProjectionPruner& pruner = {},
    std::string sort_property = "",
    bool sort_ascending = true,
    bool sort_by_id = false,
    PathMode path_mode = PathMode::WALK,   // GQL's default: no path filtering
    GqlRowSink* sink = nullptr
);

seastar::future<std::vector<GqlRow>> traverse_match_statement(
    ragedb::Graph& graph,
    const MatchStatement& stmt,
    const GqlRow& row,
    size_t limit = 0,
    const ProjectionPruner& pruner = {},
    std::string sort_property = "",
    bool sort_ascending = true,
    bool sort_by_id = false
);

} // namespace ragedb::gql

#endif // RAGEDB_PATH_TRAVERSER_H
