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

    sol::as_table_t<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIdsByIdViaLua(std::vector<Link> links) {
      return sol::as_table(LinksGetRelationshipsIDsPeered(links).get0());
    }

    sol::as_table_t<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIdsByIdForDirectionViaLua(std::vector<Link> links, Direction direction) {
      return sol::as_table(LinksGetRelationshipsIDsPeered(links, direction).get0());
    }

    sol::as_table_t<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIdsByIdForDirectionForTypeViaLua(std::vector<Link> links, Direction direction, const std::string& rel_type) {
      return sol::as_table(LinksGetRelationshipsIDsPeered(links, direction, rel_type).get0());
    }

    sol::as_table_t<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIdsByIdForDirectionForTypeIdViaLua(std::vector<Link> links, Direction direction, uint16_t type_id) {
      return sol::as_table(LinksGetRelationshipsIDsPeered(links, direction, type_id).get0());
    }

    sol::as_table_t<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIdsByIdForDirectionForTypesViaLua(std::vector<Link> links, Direction direction, const std::vector<std::string> &rel_types) {
      return sol::as_table(LinksGetRelationshipsIDsPeered(links, direction, rel_types).get0());
    }

    sol::as_table_t<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIdsByIdForTypeViaLua(std::vector<Link> links, const std::string& rel_type) {
      return sol::as_table(LinksGetRelationshipsIDsPeered(links, rel_type).get0());
    }

    sol::as_table_t<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIdsByIdForTypeIdViaLua(std::vector<Link> links, uint16_t type_id) {
      return sol::as_table(LinksGetRelationshipsIDsPeered(links, type_id).get0());
    }

    sol::as_table_t<std::map<Link, std::vector<Link>>> Shard::LinksGetRelationshipsIdsByIdForTypesViaLua(std::vector<Link> links, const std::vector<std::string> &rel_types) {
      return sol::as_table(LinksGetRelationshipsIDsPeered(links, rel_types).get0());
    }

}