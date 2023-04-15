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

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(const std::string& type, const std::string& key, Direction direction) {
        return sol::as_table(NodeGetLinksPeered(type, key, direction).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(type, key, direction, rel_type).get0());
    }

     sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(type, key, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(type, key, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(type, key, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(uint64_t id) {
        return sol::as_table(NodeGetLinksPeered(id).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(uint64_t id, Direction direction) {
        return sol::as_table(NodeGetLinksPeered(id, direction).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(id, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(id, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(uint64_t id, const std::string& rel_type) {
        return sol::as_table(NodeGetLinksPeered(id, rel_type).get0());
    }

    sol::as_table_t<std::vector<Link>> Shard::NodeGetLinksViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetLinksPeered(id, rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(const std::string& type, const std::string& key) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(const std::string& type, const std::string& key, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(const std::string& type, const std::string& key, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, rel_types).get0());
    }


    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(const std::string& type, const std::string& key, Direction direction) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(type, key, direction, rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(uint64_t id) {
        return sol::as_table(NodeGetRelationshipsPeered(id).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(uint64_t id, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(id, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(uint64_t id, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(id, rel_types).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(uint64_t id, Direction direction) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(uint64_t id, Direction direction, const std::string& rel_type) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction, rel_type).get0());
    }

    sol::as_table_t<std::vector<Relationship>> Shard::NodeGetRelationshipsViaLua(uint64_t id, Direction direction, const std::vector<std::string> &rel_types) {
        return sol::as_table(NodeGetRelationshipsPeered(id, direction, rel_types).get0());
    }
}
