/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

#include "../Shard.h"

namespace ragedb {

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(const std::string& type, const std::string& key){
        return sol::as_table(NodeGetNeighborIdsPeered(type, key).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(const std::string& type, const std::string& key, Direction direction){
        return sol::as_table(NodeGetNeighborIdsPeered(type, key, direction).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type){
        return sol::as_table(NodeGetNeighborIdsPeered(type, key, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types){
        return sol::as_table(NodeGetNeighborIdsPeered(type, key, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(uint64_t id){
        return sol::as_table(NodeGetNeighborIdsPeered(id).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(uint64_t id, Direction direction){
        return sol::as_table(NodeGetNeighborIdsPeered(id, direction).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(uint64_t id, Direction direction, const std::string& rel_type){
        return sol::as_table(NodeGetNeighborIdsPeered(id, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types){
        return sol::as_table(NodeGetNeighborIdsPeered(id, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(Node node){
        return sol::as_table(NodeGetNeighborIdsPeered(node.getId()).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(Node node, Direction direction){
        return sol::as_table(NodeGetNeighborIdsPeered(node.getId(), direction).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(Node node, Direction direction, const std::string& rel_type){
        return sol::as_table(NodeGetNeighborIdsPeered(node.getId(), direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<uint64_t>> Shard::NodeGetNeighborIdsViaLua(Node node, Direction direction, const std::vector<std::string> &rel_types){
        return sol::as_table(NodeGetNeighborIdsPeered(node.getId(), direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdViaLua(uint64_t id) {
        return sol::as_table(NodeGetNeighborsPeered(id).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdViaLua(uint64_t id, Direction direction) {
        return sol::as_table(NodeGetNeighborsPeered(id, direction).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetNeighborsPeered(id, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsByIdViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetNeighborsPeered(id, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsViaLua(const std::string& type, const std::string& key) {
        return sol::as_table(NodeGetNeighborsPeered(type, key).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsViaLua(const std::string& type, const std::string& key, Direction direction) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, direction).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Node>> Shard::NodeGetNeighborsViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetNeighborsPeered(type, key, direction, rel_types).get0());
    }

    sol::table Shard::NodeIdsGetNeighborIdsViaLua(std::vector<uint64_t> ids) {
        sol::table neighbors = lua.create_table(0, ids.size());
        for (const auto& [node_id, neighbor_ids] : NodeIdsGetNeighborIdsPeered(ids).get0()) {
            neighbors.set(node_id, neighbor_ids);
        }
        return neighbors;
    }

    sol::table Shard::NodeIdsGetNeighborIdsViaLua(std::vector<uint64_t> ids, Direction direction) {
        sol::table neighbors = lua.create_table(0, ids.size());
        for (const auto& [node_id, neighbor_ids] : NodeIdsGetNeighborIdsPeered(ids, direction).get0()) {
            neighbors.set(node_id, neighbor_ids);
        }
        return neighbors;
    }

    sol::table Shard::NodeIdsGetNeighborIdsViaLua(std::vector<uint64_t> ids, Direction direction, const std::string& rel_type) {
        sol::table neighbors = lua.create_table(0, ids.size());
        for (const auto& [node_id, neighbor_ids] : NodeIdsGetNeighborIdsPeered(ids, direction, rel_type).get0()) {
            neighbors.set(node_id, neighbor_ids);
        }
        return neighbors;
    }

    sol::table Shard::NodeIdsGetNeighborIdsViaLua(std::vector<uint64_t> ids, Direction direction, const std::vector<std::string> &rel_types) {
        sol::table neighbors = lua.create_table(0, ids.size());
        for (const auto& [node_id, neighbor_ids] : NodeIdsGetNeighborIdsPeered(ids, direction, rel_types).get0()) {
            neighbors.set(node_id, neighbor_ids);
        }
        return neighbors;
    }

// TODO: Fix all these to return a map with Node ID - Neighbors
// TODO: Add NodesGetNeighbors
    sol::table Shard::NodesGetNeighborIdsViaLua(std::vector<Node> nodes) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborIdsViaLua(ids);
    }

    sol::table Shard::NodesGetNeighborIdsViaLua(std::vector<Node> nodes, Direction direction) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborIdsViaLua(ids, direction);
    }

    sol::table Shard::NodesGetNeighborIdsViaLua(std::vector<Node> nodes, Direction direction, const std::string& rel_type) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborIdsViaLua(ids, direction, rel_type);
    }

    sol::table Shard::NodesGetNeighborIdsViaLua(std::vector<Node> nodes, Direction direction, const std::vector<std::string> &rel_types) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborIdsViaLua(ids, direction, rel_types);
    }

    sol::table Shard::NodeIdsGetNeighborsViaLua(std::vector<uint64_t> ids) {
        sol::table neighbors = lua.create_table(0, ids.size());
        for (const auto& [node_id, neighbor_ids] : NodeIdsGetNeighborsPeered(ids).get0()) {
            sol::table my_neighbors = lua.create_table(neighbor_ids.size(), 0);
            for (const auto& node : neighbor_ids) {
                my_neighbors.add(node);
            }
            neighbors.set(node_id, my_neighbors);
        }
        return neighbors;
    }

    sol::table Shard::NodeIdsGetNeighborsViaLua(std::vector<uint64_t> ids, Direction direction) {
        sol::table neighbors = lua.create_table(0, ids.size());
        for (const auto& [node_id, neighbor_ids] : NodeIdsGetNeighborsPeered(ids, direction).get0()) {
            sol::table my_neighbors = lua.create_table(neighbor_ids.size(), 0);
            for (const auto& node : neighbor_ids) {
                my_neighbors.add(node);
            }
            neighbors.set(node_id, my_neighbors);
        }
        return neighbors;
    }

    sol::table Shard::NodeIdsGetNeighborsViaLua(std::vector<uint64_t> ids, Direction direction, const std::string& rel_type) {
        sol::table neighbors = lua.create_table(0, ids.size());
        for (const auto& [node_id, neighbor_ids] : NodeIdsGetNeighborsPeered(ids, direction, rel_type).get0()) {
            sol::table my_neighbors = lua.create_table(neighbor_ids.size(), 0);
            for (const auto& node : neighbor_ids) {
                my_neighbors.add(node);
            }
            neighbors.set(node_id, my_neighbors);
        }
        return neighbors;
    }

    sol::table Shard::NodeIdsGetNeighborsViaLua(std::vector<uint64_t> ids, Direction direction, const std::vector<std::string> &rel_types) {
        sol::table neighbors = lua.create_table(0, ids.size());
        for (const auto& [node_id, neighbor_ids] : NodeIdsGetNeighborsPeered(ids, direction, rel_types).get0()) {
            sol::table my_neighbors = lua.create_table(neighbor_ids.size(), 0);
            for (const auto& node : neighbor_ids) {
                my_neighbors.add(node);
            }
            neighbors.set(node_id, my_neighbors);
        }
        return neighbors;
    }

    sol::table Shard::NodesGetNeighborsViaLua(std::vector<Node> nodes) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborsViaLua(ids);
    }

    sol::table Shard::NodesGetNeighborsViaLua(std::vector<Node> nodes, Direction direction) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborsViaLua(ids, direction);
    }

    sol::table Shard::NodesGetNeighborsViaLua(std::vector<Node> nodes, Direction direction, const std::string& rel_type) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborsViaLua(ids, direction, rel_type);
    }

    sol::table Shard::NodesGetNeighborsViaLua(std::vector<Node> nodes, Direction direction, const std::vector<std::string> &rel_types) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborsViaLua(ids, direction, rel_types);
    }
}