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

#include "Shard.h"
#include "Roar.h"
#include "Lua.h"
#include "Sandbox.h"
#include "types/Date.h"

unsigned int SHARD_BITS;
unsigned int SHARD_MASK;

using namespace roaring;
namespace ragedb {

    Shard::Shard(uint _cpus) : cpus(_cpus) {
        // Set the Shard Bits and Mask
        SHARD_BITS = std::bit_width(_cpus);
        SHARD_MASK = (1 << SHARD_BITS) - 1;

        lua.open_libraries(sol::lib::base, sol::lib::package, sol::lib::math, sol::lib::string, sol::lib::table, sol::lib::io, sol::lib::os, sol::lib::coroutine);
        lua.require_script("sort", Lua::sorting_script);
        lua.require_script("json", Lua::json_script);
        lua.require_script("ftcsv", Lua::ftcsv_script);
        lua.require_script("date", Lua::date_script);
        lua.require_script("template", Lua::template_script);
        lua.require_script("html", Lua::html_script);

        lua.new_enum("Direction",
          "BOTH", Direction::BOTH,
          "IN", Direction::IN,
          "OUT", Direction::OUT
        );

        lua.new_enum("Sort",
          "NONE", Sort::NONE,
          "ASC", Sort::ASC,
          "DESC", Sort::DESC
        );

        lua.new_enum("Operation",
          "EQ", Operation::EQ,              // A == B
          "NEQ", Operation::NEQ,             // A != B
          "GT", Operation::GT,              // A  > B
          "GTE", Operation::GTE,             // A >= B
          "LT", Operation::LT,              // A  < B
          "LTE", Operation::LTE,             // A <= B
          "IS_NULL", Operation::IS_NULL,         // A is Null
          "STARTS_WITH", Operation::STARTS_WITH,     // A starts with B
          "CONTAINS", Operation::CONTAINS,        // A contains B
          "ENDS_WITH", Operation::ENDS_WITH,       // A ends with B
          "NOT_IS_NULL", Operation::NOT_IS_NULL,     // A is not Null
          "NOT_STARTS_WITH", Operation::NOT_STARTS_WITH, // A does not start with B
          "NOT_CONTAINS", Operation::NOT_CONTAINS,    // A does not contain B
          "NOT_ENDS_WITH", Operation::NOT_ENDS_WITH,   // A does not end with B
          "UNKNOWN", Operation::UNKNOWN
        );

        lua.new_usertype<Roar>("Roar", sol::constructors<Roar()>(),
          "addIds", &Roar::addIds,
          "addNodeIds", &Roar::addNodeIds,
          "addRelationshipIds", &Roar::addRelationshipIds,
          "addValues", &Roar::addValues,
          "getIds", &Roar::getIdsLua,
          "getNodeHalfLinks", &Roar::getNodeHalfLinksLua,
          "getRelationshipHalfLinks", &Roar::getRelationshipHalfLinksLua,
          "add", &Roar::add,
          "remove", &Roar::remove,
          "addChecked", &Roar::addChecked,
          "removeChecked", &Roar::removeChecked,
          "clear", &Roar::clear,
          "maximum", &Roar::maximum,
          "minimum", &Roar::minimum,
          "contains", &Roar::contains,
          "inplace_intersection", &Roar::operator&=,
          "inplace_difference", &Roar::operator-=,
          "inplace_union", &Roar::operator|=,
          "inplace_xor", &Roar::operator^=,
          "swap", &Roar::swap,
          "cardinality", &Roar::cardinality,
          "isEmpty", &Roar::isEmpty,
          "isFull", &Roar::isFull,
          "isSubset", &Roar::isSubset,
          "isStrictSubset", &Roar::isStrictSubset,
          "sol::meta_function::equal_to", &Roar::operator==,
          "flip", &Roar::flip,
          "removeRunCompression", &Roar::removeRunCompression,
          "runOptimize", &Roar::runOptimize,
          "shrinkToFit", &Roar::shrinkToFit,
          "rank", &Roar::rank,
          "intersection", &Roar::operator&,
          "difference", &Roar::operator-,
          "union", &Roar::operator|,
          "xor", &Roar::operator^,
          "toString", &Roar::toString
          );

        lua.new_usertype<Node>("Node",
                // 3 constructors
                                 sol::constructors<
                                         Node(),
                                         Node(uint64_t, std::string, std::string),
                                         Node(uint64_t, std::string, std::string, std::map<std::string, property_type_t>)>(),
                                 "getId", &Node::getId,
                                 "getType", &Node::getType,
                                 "getKey", &Node::getKey,
                                 "getProperties", &Node::getPropertiesLua,
                                 "getProperty", &Node::getPropertyLua);

        lua.new_usertype<Relationship>("Relationship",
                // 3 constructors
                                         sol::constructors<
                                                 Relationship(),
                                                 Relationship(uint64_t, std::string, uint64_t, uint16_t),
                                                 Relationship(uint64_t, std::string, uint64_t, uint16_t, std::map<std::string, property_type_t>)>(),
                                         "getId", &Relationship::getId,
                                         "getType", &Relationship::getType,
                                         "getStartingNodeId", &Relationship::getStartingNodeId,
                                         "getEndingNodeId", &Relationship::getEndingNodeId,
                                         "getProperties", &Relationship::getPropertiesLua,
                                         "getProperty", &Relationship::getPropertyLua);

        lua.new_usertype<Link>("Link",
                                sol::constructors<Link(uint64_t, uint64_t)>(),
                                "getNodeId", &Link::getNodeId,
                                "getRelationshipId", &Link::getRelationshipId);

        // Relationship Types
        lua.set_function("RelationshipTypesGetCount", sol::overload(
            [this]() { return this->RelationshipTypesGetCountViaLua(); },
            [this](const std::string& type) { return this->RelationshipTypesGetCountByTypeViaLua(type); }
           ));
        lua.set_function("RelationshipTypesGet", &Shard::RelationshipTypesGetViaLua, this);
        lua.set_function("RelationshipTypeGet", &Shard::RelationshipTypeGetViaLua, this);

        // Relationship Type
        lua.set_function("RelationshipTypeInsert", &Shard::RelationshipTypeInsertViaLua, this);

        // Node Types
        lua.set_function("NodeTypesGetCount", sol::overload(
            [this]() { return this->NodeTypesGetCountViaLua(); },
            [this](const std::string& type) { return this->NodeTypesGetCountByTypeViaLua(type); }
           ));
        lua.set_function("NodeTypesGet", &Shard::NodeTypesGetViaLua, this);
        lua.set_function("NodeTypeGet", &Shard::NodeTypeGetViaLua, this);

        // Node Type
        lua.set_function("NodeTypeInsert", &Shard::NodeTypeInsertViaLua, this);

        // Property Types
        lua.set_function("NodePropertyTypeAdd", &Shard::NodePropertyTypeAddViaLua, this);
        lua.set_function("RelationshipPropertyTypeAdd", &Shard::RelationshipPropertyTypeAddViaLua, this);
        lua.set_function("NodePropertyTypeDelete", &Shard::NodePropertyTypeDeleteViaLua, this);
        lua.set_function("RelationshipPropertyTypeDelete", &Shard::RelationshipPropertyTypeDeleteViaLua, this);

        //Node
        lua.set_function("NodeAdd", sol::overload(
            [this](const std::string& type, const std::string& key) { return this->NodeAddEmptyViaLua(type, key); },
            [this](const std::string& type, const std::string& key, const std::string& properties) { return this->NodeAddViaLua(type, key, properties); },
            [this](const std::string& type, const std::vector<std::string>& keys, const std::vector<std::string>& properties) { return this->NodeAddManyPeeredViaLua(type, keys, properties); }
           ));

        lua.set_function("NodeGetId", &Shard::NodeGetIdViaLua, this);

        lua.set_function("NodeRemove", sol::overload(
            [this](const std::string& type, const std::string& key) { return this->NodeRemoveViaLua(type, key); },
            [this](uint64_t id) { return this->NodeRemoveViaLua(id); }
           ));

        // Node Properties
        lua.set_function("NodeSetProperty", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& property, sol::object value) { return this->NodeSetPropertyViaLua(type, key, property, value); },
           [this](uint64_t id, const std::string& property, sol::object value) { return this->NodeSetPropertyViaLua(id, property, value); },
           [this](Node node, const std::string& property, sol::object value) { return this->NodeSetPropertyViaLua(node.getId(), property, value); }
           ));
        lua.set_function("NodeSetPropertiesFromJson", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& properties) { return this->NodeSetPropertiesFromJsonViaLua(type, key, properties); },
           [this](uint64_t id, const std::string& properties) { return this->NodeSetPropertiesFromJsonViaLua(id, properties); },
           [this](Node node, const std::string& properties) { return this->NodeSetPropertiesFromJsonViaLua(node.getId(), properties); }
           ));
        lua.set_function("NodeSetPropertyFromJson", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& property, const std::string& value) { return this->NodeSetPropertyFromJsonViaLua(type, key, property, value); },
           [this](uint64_t id, const std::string& property, const std::string& value) { return this->NodeSetPropertyFromJsonViaLua(id, property, value); },
           [this](Node node, const std::string& property, const std::string& value) { return this->NodeSetPropertyFromJsonViaLua(node.getId(), property, value); }
           ));
        lua.set_function("NodeResetPropertiesFromJson", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& properties) { return this->NodeResetPropertiesFromJsonViaLua(type, key, properties); },
           [this](uint64_t id, const std::string& properties) { return this->NodeResetPropertiesFromJsonViaLua(id, properties); },
           [this](Node node, const std::string& properties) { return this->NodeResetPropertiesFromJsonViaLua(node.getId(), properties); }
           ));
        lua.set_function("NodeDeleteProperty", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& property) { return this->NodeDeletePropertyViaLua(type, key, property); },
           [this](uint64_t id, const std::string& property) { return this->NodeDeletePropertyViaLua(id, property); },
           [this](Node node, const std::string& property) { return this->NodeDeletePropertyViaLua(node.getId(), property); }
           ));
        lua.set_function("NodeDeleteProperties", sol::overload(
           [this](const std::string& type, const std::string& key) { return this->NodeDeletePropertiesViaLua(type, key); },
           [this](uint64_t id) { return this->NodeDeletePropertiesViaLua(id); },
           [this](Node node) { return this->NodeDeletePropertiesViaLua(node.getId()); }
           ));

        // Relationship
        lua.set_function("RelationshipAdd", sol::overload(
           [this](const std::string& rel_type, const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2) { return this->RelationshipAddEmptyViaLua(rel_type, type1, key1, type2, key2); },
           [this](const std::string& rel_type, uint64_t id1, uint64_t id2) { return this->RelationshipAddEmptyViaLua(rel_type, id1, id2); },
           [this](const std::string& rel_type, const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2, const std::string& properties) { return this->RelationshipAddViaLua(rel_type, type1, key1, type2, key2, properties); },
           [this](const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties) { return this->RelationshipAddViaLua(rel_type, id1, id2, properties); }
           ));

        lua.set_function("RelationshipRemove", &Shard::RelationshipRemoveViaLua, this);
        lua.set_function("RelationshipGetType", &Shard::RelationshipGetTypeViaLua, this);
        lua.set_function("RelationshipGetStartingNodeId", &Shard::RelationshipGetStartingNodeIdViaLua, this);
        lua.set_function("RelationshipGetEndingNodeId", &Shard::RelationshipGetEndingNodeIdViaLua, this);


        // Relationship Properties
        lua.set_function("RelationshipSetProperty", &Shard::RelationshipSetPropertyViaLua, this);
        // TODO: Add Bulk methods for Writes
        lua.set_function("RelationshipSetPropertyFromJson", &Shard::RelationshipSetPropertyFromJsonViaLua, this);
        lua.set_function("RelationshipDeleteProperty", &Shard::RelationshipDeletePropertyViaLua, this);
        lua.set_function("RelationshipSetPropertiesFromJson", &Shard::RelationshipSetPropertiesFromJsonViaLua, this);
        lua.set_function("RelationshipResetPropertiesFromJson", &Shard::RelationshipResetPropertiesFromJsonViaLua, this);
        lua.set_function("RelationshipDeleteProperties", &Shard::RelationshipDeletePropertiesViaLua, this);



        // Cannot overload on vectors due to Sol Issue https://github.com/ThePhD/sol2/issues/1357
        lua.set_function("NodeIdsGetNeighborIds", sol::overload(

            [this](std::vector<uint64_t> ids, std::vector<uint64_t> ids2) { return this->NodeIdsGetNeighborIdsViaLua(ids, ids2); },
            [this](std::vector<uint64_t> ids, Direction direction, std::vector<uint64_t> ids2) { return this->NodeIdsGetNeighborIdsViaLua(ids, direction, ids2); },
            [this](std::vector<uint64_t> ids, Direction direction, const std::string& rel_type, std::vector<uint64_t> ids2) { return this->NodeIdsGetNeighborIdsViaLua(ids, direction, rel_type, ids2); },
            [this](std::vector<uint64_t> ids, Direction direction, const std::vector<std::string> rel_types, std::vector<uint64_t> ids2) { return this->NodeIdsGetNeighborIdsViaLua(ids, direction, rel_types, ids2); },
            [this](std::vector<uint64_t> ids, const std::string& rel_type, std::vector<uint64_t> ids2) { return this->NodeIdsGetNeighborIdsViaLua(ids, Direction::BOTH, rel_type, ids2); },
            [this](std::vector<uint64_t> ids, const std::vector<std::string> rel_types, std::vector<uint64_t> ids2) { return this->NodeIdsGetNeighborIdsViaLua(ids, Direction::BOTH, rel_types, ids2); }
            ));


        // Unique Neighbors
        lua.set_function("NodeIdsGetUniqueNeighborIds", sol::overload(
            [this](std::vector<uint64_t> ids) { return this->NodeIdsGetUniqueNeighborIdsViaLua(ids); },
            [this](std::vector<uint64_t> ids, Direction direction) { return this->NodeIdsGetUniqueNeighborIdsViaLua(ids, direction); },
            [this](std::vector<uint64_t> ids, Direction direction, const std::string& rel_type) { return this->NodeIdsGetUniqueNeighborIdsViaLua(ids, direction, rel_type); },
            [this](std::vector<uint64_t> ids, Direction direction, const std::vector<std::string> rel_types) { return this->NodeIdsGetUniqueNeighborIdsViaLua(ids, direction, rel_types); },
            [this](std::vector<uint64_t> ids, const std::string& rel_type) { return this->NodeIdsGetUniqueNeighborIdsViaLua(ids, Direction::BOTH, rel_type); },
            [this](std::vector<uint64_t> ids, const std::vector<std::string> rel_types) { return this->NodeIdsGetUniqueNeighborIdsViaLua(ids, Direction::BOTH, rel_types); }
            ));

        // Traversing
        lua.set_function("NodeGetLinks", sol::overload(
           [this](Node node) { return this->NodeGetLinksViaLua(node.getId()); },
           [this](Node node, Direction direction) { return this->NodeGetLinksViaLua(node.getId(), direction); },
           [this](Node node, Direction direction, const std::string& rel_type) { return this->NodeGetLinksViaLua(node.getId(), direction, rel_type); },
           [this](Node node, Direction direction, const std::vector<std::string> rel_types) { return this->NodeGetLinksViaLua(node.getId(), direction, rel_types); },
           [this](Node node, const std::string& rel_type) { return this->NodeGetLinksViaLua(node.getId(), rel_type); },
           [this](Node node, const std::vector<std::string> rel_types) { return this->NodeGetLinksViaLua(node.getId(), rel_types); },
           [this](uint64_t id) { return this->NodeGetLinksViaLua(id); },
           [this](uint64_t id, Direction direction) { return this->NodeGetLinksViaLua(id, direction); },
           [this](uint64_t id, Direction direction, const std::string& rel_type) { return this->NodeGetLinksViaLua(id, direction, rel_type); },
           [this](uint64_t id, Direction direction, const std::vector<std::string> rel_types) { return this->NodeGetLinksViaLua(id, direction, rel_types); },
           [this](uint64_t id, const std::string& rel_type) { return this->NodeGetLinksViaLua(id, rel_type); },
           [this](uint64_t id, const std::vector<std::string> rel_types) { return this->NodeGetLinksViaLua(id, rel_types); },
           [this](const std::string& type, const std::string& key) { return this->NodeGetLinksViaLua(type, key); },
           [this](const std::string& type, const std::string& key, Direction direction) { return this->NodeGetLinksViaLua(type, key, direction); },
           [this](const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) { return this->NodeGetLinksViaLua(type, key, direction, rel_type); },
           [this](const std::string& type, const std::string& key, Direction direction, const std::vector<std::string> rel_types) { return this->NodeGetLinksViaLua(type, key, direction, rel_types); },
           [this](const std::string& type, const std::string& key, const std::string& rel_type) { return this->NodeGetLinksViaLua(type, key, rel_type); },
           [this](const std::string& type, const std::string& key, const std::vector<std::string> rel_types) { return this->NodeGetLinksViaLua(type, key, rel_types); }
           ));

        //Bulk
        // TODO: Nodes Get Links

        lua.set_function("LinksGetLinks", sol::overload(
            [this](std::vector<Link> links) { return this->LinksGetLinksViaLua(links); },
            [this](std::vector<Link> links, Direction direction) { return this->LinksGetLinksViaLua(links, direction); },
            [this](std::vector<Link> links, Direction direction, const std::string& rel_type) { return this->LinksGetLinksViaLua(links, direction, rel_type); },
            [this](std::vector<Link> links, Direction direction, const std::vector<std::string> rel_types) { return this->LinksGetLinksViaLua(links, direction, rel_types); },
            [this](std::vector<Link> links, const std::string& rel_type) { return this->LinksGetLinksViaLua(links, Direction::BOTH, rel_type); },
            [this](std::vector<Link> links, const std::vector<std::string> rel_types) { return this->LinksGetLinksViaLua(links, Direction::BOTH, rel_types); }
            ));


        // Connected
        lua.set_function("Connected", sol::overload(
           [this](uint64_t id, uint64_t id2) { return this->NodeGetConnectedViaLua(id, id2); },
           [this](uint64_t id, uint64_t id2, Direction direction) { return this->NodeGetConnectedViaLua(id, id2, direction); },
           [this](uint64_t id, uint64_t id2, Direction direction, const std::string& rel_type) { return this->NodeGetConnectedViaLua(id, id2, direction, rel_type); },
           [this](uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string> rel_types) { return this->NodeGetConnectedViaLua(id, id2, direction, rel_types); },
           [this](uint64_t id, uint64_t id2, const std::string& rel_type) { return this->NodeGetConnectedViaLua(id, id2, Direction::BOTH, rel_type); },
           [this](uint64_t id, uint64_t id2, const std::vector<std::string> rel_types) { return this->NodeGetConnectedViaLua(id, id2, Direction::BOTH, rel_types); },
           [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2) { return this->NodeGetConnectedViaLua(type, key, type2, key2); },
           [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction) { return this->NodeGetConnectedViaLua(type, key, type2, key2, direction); },
           [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::string& rel_type) { return this->NodeGetConnectedViaLua(type, key, type2, key2, direction, rel_type); },
           [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::vector<std::string> rel_types) { return this->NodeGetConnectedViaLua(type, key, type2, key2, direction, rel_types); },
           [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, const std::string& rel_type) { return this->NodeGetConnectedViaLua(type, key, type2, key2, Direction::BOTH, rel_type); },
           [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, const std::vector<std::string> rel_types) { return this->NodeGetConnectedViaLua(type, key, type2, key2, Direction::BOTH, rel_types); },
           [this](Node node, Node node2) { return this->NodeGetConnectedViaLua(node.getId(), node2.getId()); },
           [this](Node node, Node node2, Direction direction) { return this->NodeGetConnectedViaLua(node.getId(), node2.getId(), direction); },
           [this](Node node, Node node2, Direction direction, const std::string& rel_type) { return this->NodeGetConnectedViaLua(node.getId(), node2.getId(), direction, rel_type); },
           [this](Node node, Node node2, Direction direction, const std::vector<std::string> rel_types) { return this->NodeGetConnectedViaLua(node.getId(), node2.getId(), direction, rel_types); },
           [this](Node node, Node node2, const std::string& rel_type) { return this->NodeGetConnectedViaLua(node.getId(), node2.getId(), Direction::BOTH, rel_type); },
           [this](Node node, Node node2, const std::vector<std::string> rel_types) { return this->NodeGetConnectedViaLua(node.getId(), node2.getId(), Direction::BOTH, rel_types); }
           ));

        // All
        lua.set_function("AllNodesCounts", &Shard::AllNodesCountsViaLua, this);
        lua.set_function("AllNodesCount", sol::overload(
             [this]() { return this->AllNodesCountViaLua(); },
             [this](const std::string& type) { return this->AllNodesCountViaLua(type); }
            ));
        lua.set_function("AllRelationshipsCounts", &Shard::AllRelationshipsCountsViaLua, this);
        lua.set_function("AllRelationshipsCount", sol::overload(
            [this]() { return this->AllRelationshipsCountViaLua(); },
            [this](const std::string& type) { return this->AllRelationshipsCountViaLua(type); }
           ));
        lua.set_function("AllNodeIds", sol::overload(
            [this](sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) { return this->AllNodeIdsViaLua(skip, limit); },
            [this](const std::string& type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) { return this->AllNodeIdsForTypeViaLua(type, skip, limit); }
           ));
        lua.set_function("AllRelationshipIds", sol::overload(
            [this](sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) { return this->AllRelationshipIdsViaLua(skip, limit); },
            [this](const std::string& type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) { return this->AllRelationshipIdsForTypeViaLua(type, skip, limit); }
           ));
        lua.set_function("AllNodes", sol::overload(
            [this](sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) { return this->AllNodesViaLua(skip, limit); },
            [this](const std::string& type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) { return this->AllNodesForTypeViaLua(type, skip, limit); }
          ));
        lua.set_function("AllRelationships", sol::overload(
            [this](sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) { return this->AllRelationshipsViaLua(skip, limit); },
            [this](const std::string& type, sol::optional<uint64_t> skip, sol::optional<uint64_t> limit) { return this->AllRelationshipsForTypeViaLua(type, skip, limit); }
           ));

        // Find
        lua.set_function("FindNodeCount", &Shard::FindNodeCountViaLua, this);
        lua.set_function("FindNodeIds", &Shard::FindNodeIdsViaLua, this);
        lua.set_function("FindNodes", &Shard::FindNodesViaLua, this);

        // TODO: overload these with equal as default
        lua.set_function("FindRelationshipCount", &Shard::FindRelationshipCountViaLua, this);
        lua.set_function("FindRelationshipIds", &Shard::FindRelationshipIdsViaLua, this);
        lua.set_function("FindRelationships", &Shard::FindRelationshipsViaLua, this);

        // Filter
        lua.set_function("FilterNodeCount", &Shard::FilterNodeCountViaLua, this);
        lua.set_function("FilterNodeIds", &Shard::FilterNodeIdsViaLua, this);
        lua.set_function("FilterNodes", &Shard::FilterNodesViaLua, this);
        lua.set_function("FilterNodeProperties", &Shard::FilterNodePropertiesViaLua, this);
        lua.set_function("FilterRelationshipCount", &Shard::FilterRelationshipCountViaLua, this);
        lua.set_function("FilterRelationshipIds", &Shard::FilterRelationshipIdsViaLua, this);
        lua.set_function("FilterRelationships", &Shard::FilterRelationshipsViaLua, this);
        lua.set_function("FilterRelationshipProperties", &Shard::FilterRelationshipPropertiesViaLua, this);

        // Invert
        lua.set_function("Invert", &Shard::InvertViaLua, this);

        // Difference
        lua.set_function("DifferenceIds", &Shard::DifferenceIdsViaLua, this);
        lua.set_function("DifferenceNodes", &Shard::DifferenceNodesViaLua, this);
        lua.set_function("DifferenceRelationships", &Shard::DifferenceRelationshipsViaLua, this);
        lua.set_function("DifferenceIdsCount", &Shard::DifferenceIdsCountViaLua, this);

        // Join
        lua.set_function("JoinCount", &Shard::JoinCountViaLua, this);
        lua.set_function("Join", &Shard::JoinViaLua, this);

        // Date
        lua.set_function("DateToISO", &Date::toISO);
        lua.set_function("DateToDouble", &Date::value);

        // Load CSV
        lua.set_function("LoadCSV", &Shard::LoadCSVViaLua, this);
        lua.set_function("StreamCSV", &Shard::StreamCSVViaLua, this);

        lua.set_function("KHopIds", sol::overload(
            [this](Node node, uint64_t hops) { return this->KHopIdsViaLua(node.getId(), hops); },
            [this](Node node, uint64_t hops, Direction direction) { return this->KHopIdsViaLua(node.getId(), hops, direction); },
            [this](Node node, uint64_t hops, Direction direction, const std::string& rel_type) { return this->KHopIdsViaLua(node.getId(), hops, direction, rel_type); },
            [this](Node node, uint64_t hops, Direction direction, const std::vector<std::string> rel_types) { return this->KHopIdsViaLua(node.getId(), hops, direction, rel_types); },
            [this](Node node, uint64_t hops, const std::string& rel_type) { return this->KHopIdsViaLua(node.getId(), hops, Direction::BOTH, rel_type); },
            [this](Node node, uint64_t hops, const std::vector<std::string> rel_types) { return this->KHopIdsViaLua(node.getId(), hops, Direction::BOTH, rel_types); },
            [this](uint64_t id, uint64_t hops) { return this->KHopIdsViaLua(id, hops); },
            [this](uint64_t id, uint64_t hops, Direction direction) { return this->KHopIdsViaLua(id, hops, direction); },
            [this](uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) { return this->KHopIdsViaLua(id, hops, direction, rel_type); },
            [this](uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> rel_types) { return this->KHopIdsViaLua(id, hops, direction, rel_types); },
            [this](uint64_t id, uint64_t hops, const std::string& rel_type) { return this->KHopIdsViaLua(id, hops, Direction::BOTH, rel_type); },
            [this](uint64_t id, uint64_t hops, const std::vector<std::string> rel_types) { return this->KHopIdsViaLua(id, hops, Direction::BOTH, rel_types); },
            [this](const std::string& type, const std::string& key, uint64_t hops) { return this->KHopIdsViaLua(type, key, hops); },
            [this](const std::string& type, const std::string& key, uint64_t hops, Direction direction) { return this->KHopIdsViaLua(type, key, hops, direction); },
            [this](const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type) { return this->KHopIdsViaLua(type, key, hops, direction, rel_type); },
            [this](const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> rel_types) { return this->KHopIdsViaLua(type, key, hops, direction, rel_types); },
            [this](const std::string& type, const std::string& key, uint64_t hops, const std::string& rel_type) { return this->KHopIdsViaLua(type, key, hops, Direction::BOTH, rel_type); },
            [this](const std::string& type, const std::string& key, uint64_t hops, const std::vector<std::string> rel_types) { return this->KHopIdsViaLua(type, key, hops, Direction::BOTH, rel_types); }
            ));

        lua.set_function("KHopNodes", sol::overload(
            [this](Node node, uint64_t hops) { return this->KHopNodesViaLua(node.getId(), hops); },
            [this](Node node, uint64_t hops, Direction direction) { return this->KHopNodesViaLua(node.getId(), hops, direction); },
            [this](Node node, uint64_t hops, Direction direction, const std::string& rel_type) { return this->KHopNodesViaLua(node.getId(), hops, direction, rel_type); },
            [this](Node node, uint64_t hops, Direction direction, const std::vector<std::string> rel_types) { return this->KHopNodesViaLua(node.getId(), hops, direction, rel_types); },
            [this](Node node, uint64_t hops, const std::string& rel_type) { return this->KHopNodesViaLua(node.getId(), hops, Direction::BOTH, rel_type); },
            [this](Node node, uint64_t hops, const std::vector<std::string> rel_types) { return this->KHopNodesViaLua(node.getId(), hops, Direction::BOTH, rel_types); },
            [this](uint64_t id, uint64_t hops) { return this->KHopNodesViaLua(id, hops); },
            [this](uint64_t id, uint64_t hops, Direction direction) { return this->KHopNodesViaLua(id, hops, direction); },
            [this](uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) { return this->KHopNodesViaLua(id, hops, direction, rel_type); },
            [this](uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> rel_types) { return this->KHopNodesViaLua(id, hops, direction, rel_types); },
            [this](uint64_t id, uint64_t hops, const std::string& rel_type) { return this->KHopNodesViaLua(id, hops, Direction::BOTH, rel_type); },
            [this](uint64_t id, uint64_t hops, const std::vector<std::string> rel_types) { return this->KHopNodesViaLua(id, hops, Direction::BOTH, rel_types); },
            [this](const std::string& type, const std::string& key, uint64_t hops) { return this->KHopNodesViaLua(type, key, hops); },
            [this](const std::string& type, const std::string& key, uint64_t hops, Direction direction) { return this->KHopNodesViaLua(type, key, hops, direction); },
            [this](const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type) { return this->KHopNodesViaLua(type, key, hops, direction, rel_type); },
            [this](const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> rel_types) { return this->KHopNodesViaLua(type, key, hops, direction, rel_types); },
            [this](const std::string& type, const std::string& key, uint64_t hops, const std::string& rel_type) { return this->KHopNodesViaLua(type, key, hops, Direction::BOTH, rel_type); },
            [this](const std::string& type, const std::string& key, uint64_t hops, const std::vector<std::string> rel_types) { return this->KHopNodesViaLua(type, key, hops, Direction::BOTH, rel_types); }
            ));

        lua.set_function("KHopCount", sol::overload(
            [this](Node node, uint64_t hops) { return this->KHopCountViaLua(node.getId(), hops); },
            [this](Node node, uint64_t hops, Direction direction) { return this->KHopCountViaLua(node.getId(), hops, direction); },
            [this](Node node, uint64_t hops, Direction direction, const std::string& rel_type) { return this->KHopCountViaLua(node.getId(), hops, direction, rel_type); },
            [this](Node node, uint64_t hops, Direction direction, const std::vector<std::string> rel_types) { return this->KHopCountViaLua(node.getId(), hops, direction, rel_types); },
            [this](Node node, uint64_t hops, const std::string& rel_type) { return this->KHopCountViaLua(node.getId(), hops, Direction::BOTH, rel_type); },
            [this](Node node, uint64_t hops, const std::vector<std::string> rel_types) { return this->KHopCountViaLua(node.getId(), hops, Direction::BOTH, rel_types); },
            [this](uint64_t id, uint64_t hops) { return this->KHopCountViaLua(id, hops); },
            [this](uint64_t id, uint64_t hops, Direction direction) { return this->KHopCountViaLua(id, hops, direction); },
            [this](uint64_t id, uint64_t hops, Direction direction, const std::string& rel_type) { return this->KHopCountViaLua(id, hops, direction, rel_type); },
            [this](uint64_t id, uint64_t hops, Direction direction, const std::vector<std::string> rel_types) { return this->KHopCountViaLua(id, hops, direction, rel_types); },
            [this](uint64_t id, uint64_t hops, const std::string& rel_type) { return this->KHopCountViaLua(id, hops, Direction::BOTH, rel_type); },
            [this](uint64_t id, uint64_t hops, const std::vector<std::string> rel_types) { return this->KHopCountViaLua(id, hops, Direction::BOTH, rel_types); },
            [this](const std::string& type, const std::string& key, uint64_t hops) { return this->KHopCountViaLua(type, key, hops); },
            [this](const std::string& type, const std::string& key, uint64_t hops, Direction direction) { return this->KHopCountViaLua(type, key, hops, direction); },
            [this](const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::string& rel_type) { return this->KHopCountViaLua(type, key, hops, direction, rel_type); },
            [this](const std::string& type, const std::string& key, uint64_t hops, Direction direction, const std::vector<std::string> rel_types) { return this->KHopCountViaLua(type, key, hops, direction, rel_types); },
            [this](const std::string& type, const std::string& key, uint64_t hops, const std::string& rel_type) { return this->KHopCountViaLua(type, key, hops, Direction::BOTH, rel_type); },
            [this](const std::string& type, const std::string& key, uint64_t hops, const std::vector<std::string> rel_types) { return this->KHopCountViaLua(type, key, hops, Direction::BOTH, rel_types); }
            ));

        lua.set_function("TriangleCount", sol::overload(
            [this](const std::string& type) { return this->TriangleCount(type); },
            [this](const std::vector<std::string> rel_types) { return this->TriangleCount(rel_types); }
        ));

        // Nodes
        lua.set_function("Node", &Shard::NodeOverload, this);
        lua.set_function("Key", &Shard::NodeKeyOverload, this);
        lua.set_function("Type", &Shard::NodeTypeOverload, this);
        lua.set_function("Property", &Shard::NodePropertyOverload, this);
        lua.set_function("Properties", &Shard::NodePropertiesOverload, this);

        // Relationships
        lua.set_function("Relationship", &Shard::RelationshipOverload, this);
        lua.set_function("RelationshipType", &Shard::RelationshipTypeOverload, this);
        lua.set_function("RelationshipProperty", &Shard::RelationshipPropertyOverload, this);
        lua.set_function("RelationshipProperties", &Shard::RelationshipPropertiesOverload, this);


        // Traversing
        lua.set_function("Degree", &Shard::DegreeOverload, this);
        lua.set_function("Relationships", &Shard::RelationshipsOverload, this);
        lua.set_function("Neighbors", &Shard::NeighborsOverload, this);

        // Intersect
        lua.set_function("Intersect", &Shard::IntersectOverload, this);
        lua.set_function("IntersectCount", &Shard::IntersectCountOverload, this);

        // Create a sanitized environment to restrict the user's Lua code
        lua.set_function("loadstring", &Shard::loadstring, this);
        lua.set_function("loadfile", &Shard::loadfile, this);
        lua.set_function("dofile", &Shard::dofile, this);

        auto sandbox = Sandbox(lua, Permission::ADMIN);
        env = sandbox.getEnvironment();

        auto rw_sandbox = Sandbox(lua, Permission::WRITE);
        read_write_env = rw_sandbox.getEnvironment();

        auto ro_sandbox = Sandbox(lua, Permission::READ);
        read_only_env = ro_sandbox.getEnvironment();
    }

    template <typename Range, typename Value = typename Range::value_type>
    std::string join(Range const& elements, const char *const delimiter) {
        std::ostringstream os;
        auto b = begin(elements);
        auto e = end(elements);

        if (b != e) {
            std::copy(b, prev(e), std::ostream_iterator<Value>(os, delimiter));
            b = prev(e);
        }
        if (b != e) {
            os << *b;
        }

        return os.str();
    }

    /**
     * Stop the Shard - Required method by Seastar for peering_sharded_service
     *
     * @return future
     */
    seastar::future<> Shard::stop() {
        std::stringstream ss;
        ss << "Stopping Shard " << seastar::this_shard_id() << '\n';
        std::cout << ss.str();
        return seastar::make_ready_future<>();
    }

    /**
     * Empty the Shard of all data
     */
    void Shard::Clear() {
        node_types.Clear();
        relationship_types.Clear();
    }

    seastar::future<std::string> Shard::RunLua(const std::string &script, const sol::environment& environment) {

      return seastar::async([script, environment, this] () {
        // Inject json encoding
        std::stringstream ss(script);
        std::string line;
        std::vector<std::string> lines;
        while(std::getline(ss,line,'\n')){
          lines.emplace_back(line);
        }

        // We can have a single item, or multiple items returned. If single item pass through, if multiple capture in an array
        // Tricky part is comma in a string "Tom, Jerry and I" is a single item

        bool single = true;
        const char *startOfString=lines.back().c_str();    // prepare to parse the line - start is position of begin of last line
        bool inString = false;
        for (const char* p=startOfString; *p; p++) {  // iterate through the last line
          if (*p == '"')                        // toggle flag if we're btw double quotes
            inString = !inString;
          else if (*p == ',' && !inString) {    // if comma OUTSIDE double quote, we have multiple values being returned
            single = false;
            break;
          }
        }

        if (single) {
          lines.back() = "return json.encode(" + lines.back() + ")";
        } else {
          lines.back() = "return json.encode({" + lines.back() + "})";
        }

        std::string executable = join(lines, "\n");
        sol::protected_function_result script_result;
        // We only have one Lua VM for each Core, so lock it during use.
        this->lua_lock.for_write().lock().get();
        try {
          script_result = lua.safe_script(executable, environment,[] (const lua_State *, sol::protected_function_result pfr) {
            return pfr;
          });
          this->lua_lock.for_write().unlock();
          if (script_result.valid()) {
            return script_result.get<std::string>();
          }

          sol::error err = script_result;
          std::string what = err.what();

          return EXCEPTION + what;
        } catch (...) {
          sol::error err = script_result;
          std::string what = err.what();
          // Unlock if we get an exception.
          this->lua_lock.for_write().unlock();
          return EXCEPTION + what;
        }
      });
    }

    seastar::future<std::string> Shard::RunAdminLua(const std::string &script){
      return RunLua(script, env);
    }

    seastar::future<std::string> Shard::RunRWLua(const std::string &script) {
      return RunLua(script, read_write_env);
    }

    seastar::future<std::string> Shard::RunROLua(const std::string &script) {
      return RunLua(script, read_only_env);
    }

    seastar::future<std::string> Shard::HealthCheck() {
        std::stringstream message;
        message << "Shard " << seastar::this_shard_id() << " is OK";
        return seastar::make_ready_future<std::string>(message.str());
    }

    seastar::future<std::vector<std::string>> Shard::HealthCheckPeered() {
        return container().map([](Shard &local_shard) {
            return local_shard.HealthCheck();
        });
    }

}