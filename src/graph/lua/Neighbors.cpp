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

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodeIdsGetNeighborIdsViaLua(const std::vector<uint64_t> &ids) {
        return NodeIdsGetNeighborIdsPeered(ids).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodeIdsGetNeighborIdsViaLua(const std::vector<uint64_t> &ids, Direction direction) {
        return NodeIdsGetNeighborIdsPeered(ids, direction).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodeIdsGetNeighborIdsViaLua(const std::vector<uint64_t> &ids, Direction direction, const std::string& rel_type) {
        return NodeIdsGetNeighborIdsPeered(ids, direction, rel_type).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodeIdsGetNeighborIdsViaLua(const std::vector<uint64_t> &ids, Direction direction, const std::vector<std::string> &rel_types) {
        return NodeIdsGetNeighborIdsPeered(ids, direction, rel_types).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodeIdsGetNeighborIdsViaLua(const std::vector<uint64_t> &ids, const std::vector<uint64_t> &ids2) {
        return NodeIdsGetNeighborIdsPeered(ids, ids2).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodeIdsGetNeighborIdsViaLua(const std::vector<uint64_t> &ids, Direction direction, const std::vector<uint64_t> &ids2) {
        return NodeIdsGetNeighborIdsPeered(ids, direction, ids2).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodeIdsGetNeighborIdsViaLua(const std::vector<uint64_t> &ids, Direction direction, const std::string& rel_type, const std::vector<uint64_t> &ids2) {
        return NodeIdsGetNeighborIdsPeered(ids, direction, rel_type, ids2).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodeIdsGetNeighborIdsViaLua(const std::vector<uint64_t>& ids, Direction direction, const std::vector<std::string> &rel_types, const std::vector<uint64_t> &ids2) {
        return NodeIdsGetNeighborIdsPeered(ids, direction, rel_types, ids2).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodesGetNeighborIdsViaLua(std::vector<Node> nodes) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborIdsViaLua(ids);
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodesGetNeighborIdsViaLua(std::vector<Node> nodes, Direction direction) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborIdsViaLua(ids, direction);
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodesGetNeighborIdsViaLua(std::vector<Node> nodes, Direction direction, const std::string& rel_type) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborIdsViaLua(ids, direction, rel_type);
    }

    sol::nested<std::map<uint64_t, std::vector<uint64_t>>> Shard::NodesGetNeighborIdsViaLua(std::vector<Node> nodes, Direction direction, const std::vector<std::string> &rel_types) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborIdsViaLua(ids, direction, rel_types);
    }

    sol::nested<std::map<uint64_t, std::vector<Node>>> Shard::NodeIdsGetNeighborsViaLua(std::vector<uint64_t> ids) {
        return NodeIdsGetNeighborsPeered(ids).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<Node>>> Shard::NodeIdsGetNeighborsViaLua(std::vector<uint64_t> ids, Direction direction) {
       return NodeIdsGetNeighborsPeered(ids, direction).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<Node>>> Shard::NodeIdsGetNeighborsViaLua(std::vector<uint64_t> ids, Direction direction, const std::string& rel_type) {
       return NodeIdsGetNeighborsPeered(ids, direction, rel_type).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<Node>>> Shard::NodeIdsGetNeighborsViaLua(std::vector<uint64_t> ids, Direction direction, const std::vector<std::string> &rel_types) {
        return NodeIdsGetNeighborsPeered(ids, direction, rel_types).get0();
    }

    sol::nested<std::map<uint64_t, std::vector<Node>>> Shard::NodesGetNeighborsViaLua(std::vector<Node> nodes) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborsViaLua(ids);
    }

    sol::nested<std::map<uint64_t, std::vector<Node>>> Shard::NodesGetNeighborsViaLua(std::vector<Node> nodes, Direction direction) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborsViaLua(ids, direction);
    }

    sol::nested<std::map<uint64_t, std::vector<Node>>>Shard::NodesGetNeighborsViaLua(std::vector<Node> nodes, Direction direction, const std::string& rel_type) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborsViaLua(ids, direction, rel_type);
    }

    sol::nested<std::map<uint64_t, std::vector<Node>>> Shard::NodesGetNeighborsViaLua(std::vector<Node> nodes, Direction direction, const std::vector<std::string> &rel_types) {
        std::vector<uint64_t> ids;
        ids.reserve(nodes.size());
        std::transform(begin(nodes), end(nodes), back_inserter(ids), std::mem_fn(&Node::getId));
        return NodeIdsGetNeighborsViaLua(ids, direction, rel_types);
    }
}