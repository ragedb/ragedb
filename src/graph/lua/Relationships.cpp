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

    sol::table Shard::RelationshipsGetViaLua(std::vector<uint64_t> ids) {
        sol::table rels = lua.create_table(0, ids.size());
        for (const auto& rel : RelationshipsGetPeered(ids).get0()) {
            rels.set(rel.getId(), rel);
        }

        return rels;
    }

    sol::table Shard::RelationshipsGetByLinksViaLua(std::vector<Link> links) {
        sol::table rels = lua.create_table(0, links.size());
        for (const auto& rel : RelationshipsGetPeered(links).get0()) {
            rels.set(rel.getId(), rel);
        }

        return rels;
    }

    sol::table Shard::RelationshipsGetTypeViaLua(std::vector<uint64_t> ids) {
        sol::table properties = lua.create_table(0, ids.size());
        for (const auto& [id, value] : RelationshipsGetTypePeered(ids).get0()) {
            properties.set(id, value);
        }
        return properties;
    }

    sol::table Shard::RelationshipsGetTypeByLinksViaLua(std::vector<Link> links) {
        sol::table properties = lua.create_table(0, links.size());
        for (const auto& [id, value] : RelationshipsGetTypePeered(links).get0()) {
            properties.set(id.rel_id, value);
        }
        return properties;
    }

    sol::table Shard::RelationshipsGetPropertyViaLua(std::vector<uint64_t> ids, const std::string& property) {
        sol::table properties = lua.create_table(0, ids.size());
        for (const auto& [id, value] : RelationshipsGetPropertyPeered(ids, property).get0()) {
            properties.set(id, value);
        }
        return properties;
    }

    sol::table Shard::RelationshipsGetPropertyByLinksViaLua(std::vector<Link> links, const std::string& property) {
        sol::table properties = lua.create_table(0, links.size());
        for (const auto& [id, value] : RelationshipsGetPropertyPeered(links, property).get0()) {
            properties.set(id.rel_id, value);
        }
        return properties;
    }

    sol::table Shard::RelationshipsGetPropertiesViaLua(std::vector<uint64_t> ids) {
        sol::table properties = lua.create_table(0, ids.size());
        for (const auto& [id, rel_properties] : RelationshipsGetPropertiesPeered(ids).get0()) {
            properties.set(id, PropertiesToSolObject(rel_properties));
        }
        return properties;
    }

    sol::table Shard::RelationshipsGetPropertiesByLinksViaLua(std::vector<Link> links) {
        sol::table properties = lua.create_table(0, links.size());
        for (const auto& [id, rel_properties] : RelationshipsGetPropertiesPeered(links).get0()) {
            properties.set(id.rel_id, PropertiesToSolObject(rel_properties));
        }
        return properties;
    }
}