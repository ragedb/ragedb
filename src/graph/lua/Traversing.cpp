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

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(const std::string& type, const std::string& key) {
        return sol::as_table(NodeGetLinksPeered(type, key).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksForDirectionViaLua(const std::string& type, const std::string& key, Direction direction) {
        return sol::as_table(NodeGetLinksPeered(type, key, direction).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(type, key, direction, rel_type).get0());
    }

     sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(type, key, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(type, key, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(type, key, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdViaLua(uint64_t id) {
        return sol::as_table(NodeGetLinksPeered(id).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForDirectionViaLua(uint64_t id, Direction direction) {
        return sol::as_table(NodeGetLinksPeered(id, direction).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(id, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(id, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForTypeViaLua(uint64_t id, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(id, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(id, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdViaLua(Node node) {
        return sol::as_table(NodeGetLinksPeered(node.getId()).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForDirectionViaLua(Node node, Direction direction) {
        return sol::as_table(NodeGetLinksPeered(node.getId(), direction).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForDirectionForTypeViaLua(Node node, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(node.getId(), direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForDirectionForTypesViaLua(Node node, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(node.getId(), direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForTypeViaLua(Node node, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(node.getId(), rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksByIdForTypesViaLua(Node node, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(node.getId(), rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(const std::string& type, const std::string& key) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForTypeViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForTypesViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, rel_types).get0());
    }


    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionViaLua(const std::string& type, const std::string& key, Direction direction) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionForTypeViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsForDirectionForTypesViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdViaLua(uint64_t id) {
        return sol::as_table(NodeGetRelationshipsPeered(id).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypeViaLua(uint64_t id, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(id, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypesViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(id, rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionViaLua(uint64_t id, Direction direction) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypeViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypesViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdViaLua(Node node) {
        return sol::as_table(NodeGetRelationshipsPeered(node.getId()).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypeViaLua(Node node, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(node.getId(), rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForTypesViaLua(Node node, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(node.getId(), rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionViaLua(Node node, Direction direction) {
        return sol::as_table(NodeGetRelationshipsPeered(node.getId(), direction).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypeViaLua(Node node, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(node.getId(), direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsByIdForDirectionForTypesViaLua(Node node, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(node.getId(), direction, rel_types).get0());
    }
}
