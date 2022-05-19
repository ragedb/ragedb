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

unsigned int SHARD_BITS;
unsigned int SHARD_MASK;

using namespace roaring;
namespace ragedb {

    Shard::Shard(uint _cpus) : cpus(_cpus) {
        // Set the Shard Bits and Mask
        SHARD_BITS = std::bit_width(_cpus);
        SHARD_MASK = (1 << SHARD_BITS) - 1;

        lua.open_libraries(sol::lib::base, sol::lib::package, sol::lib::math, sol::lib::string, sol::lib::table, sol::lib::io, sol::lib::os, sol::lib::coroutine);
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
            [this](const std::string& type) { return this->RelationshipTypesGetCountByTypeViaLua(type); },
            [this](uint16_t id) { return this->RelationshipTypesGetCountByIdViaLua(id); }
           ));
        lua.set_function("RelationshipTypesGet", &Shard::RelationshipTypesGetViaLua, this);
        lua.set_function("RelationshipTypeGet", &Shard::RelationshipTypeGetViaLua, this);

        // Relationship Type
        lua.set_function("RelationshipTypeGetType", &Shard::RelationshipTypeGetTypeViaLua, this);
        lua.set_function("RelationshipTypeInsert", &Shard::RelationshipTypeInsertViaLua, this);

        // Node Types
        lua.set_function("NodeTypesGetCount", sol::overload(
            [this]() { return this->NodeTypesGetCountViaLua(); },
            [this](const std::string& type) { return this->NodeTypesGetCountByTypeViaLua(type); },
            [this](uint16_t id) { return this->NodeTypesGetCountByIdViaLua(id); }
           ));
        lua.set_function("NodeTypesGet", &Shard::NodeTypesGetViaLua, this);
        lua.set_function("NodeTypeGet", &Shard::NodeTypeGetViaLua, this);

        // Node Type
        lua.set_function("NodeTypeGetType", &Shard::NodeTypeGetTypeViaLua, this);
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
        lua.set_function("NodeGet", sol::overload(
            [this](const std::string& type, const std::string& key) { return this->NodeGetViaLua(type, key); },
            [this](uint64_t id) { return this->NodeGetByIdViaLua(id); }
           ));
        lua.set_function("NodeRemove", sol::overload(
            [this](const std::string& type, const std::string& key) { return this->NodeRemoveViaLua(type, key); },
            [this](uint64_t id) { return this->NodeRemoveByIdViaLua(id); }
           ));
        lua.set_function("NodeGetType", &Shard::NodeGetTypeViaLua, this);
        lua.set_function("NodeGetKey", &Shard::NodeGetKeyViaLua, this);

        // Nodes
        lua.set_function("NodesGet", sol::overload(
            [this](const std::vector<uint64_t>& ids) { return this->NodesGetViaLua(ids); },
            [this](const std::vector<Link>& links) { return this->NodesGetByLinksViaLua(links); }
           ));
        lua.set_function("NodesGetKey", sol::overload(
            [this](const std::vector<uint64_t>& ids) { return this->NodesGetKeyViaLua(ids); },
            [this](const std::vector<Link>& links) { return this->NodesGetKeyByLinksViaLua(links); }
           ));
        lua.set_function("NodesGetType", sol::overload(
            [this](const std::vector<uint64_t>& ids) { return this->NodesGetTypeViaLua(ids); },
            [this](const std::vector<Link>& links) { return this->NodesGetTypeByLinksViaLua(links); }
           ));
        lua.set_function("NodesGetProperty", sol::overload(
            [this](const std::vector<uint64_t>& ids, const std::string& property) { return this->NodesGetPropertyViaLua(ids, property); },
            [this](const std::vector<Link>& links, const std::string& property) { return this->NodesGetPropertyByLinksViaLua(links, property); }
           ));
        lua.set_function("NodesGetProperties", sol::overload(
            [this](const std::vector<uint64_t>& ids) { return this->NodesGetPropertiesViaLua(ids); },
            [this](const std::vector<Link>& links) { return this->NodesGetPropertiesByLinksViaLua(links); }
           ));

        // Node Properties
        lua.set_function("NodeGetProperty", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& property) { return this->NodeGetPropertyViaLua(type, key, property); },
           [this](uint64_t id, const std::string& property) { return this->NodeGetPropertyByIdViaLua(id, property); }
           ));
        lua.set_function("NodeGetProperties", sol::overload(
           [this](const std::string& type, const std::string& key) { return this->NodeGetPropertiesViaLua(type, key); },
           [this](uint64_t id) { return this->NodeGetPropertiesByIdViaLua(id); }
           ));
        lua.set_function("NodeSetProperty", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& property, sol::object value) { return this->NodeSetPropertyViaLua(type, key, property, value); },
           [this](uint64_t id, const std::string& property, sol::object value) { return this->NodeSetPropertyByIdViaLua(id, property, value); }
           ));
        lua.set_function("NodeSetPropertiesFromJson", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& properties) { return this->NodeSetPropertiesFromJsonViaLua(type, key, properties); },
           [this](uint64_t id, const std::string& properties) { return this->NodeSetPropertiesFromJsonByIdViaLua(id, properties); }
           ));
        lua.set_function("NodeSetPropertyFromJson", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& property, const std::string& value) { return this->NodeSetPropertyFromJsonViaLua(type, key, property, value); },
           [this](uint64_t id, const std::string& property, const std::string& value) { return this->NodeSetPropertyFromJsonByIdViaLua(id, property, value); }
           ));
        lua.set_function("NodeResetPropertiesFromJson", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& properties) { return this->NodeResetPropertiesFromJsonViaLua(type, key, properties); },
           [this](uint64_t id, const std::string& properties) { return this->NodeResetPropertiesFromJsonByIdViaLua(id, properties); }
           ));
        lua.set_function("NodeDeleteProperty", sol::overload(
           [this](const std::string& type, const std::string& key, const std::string& property) { return this->NodeDeletePropertyViaLua(type, key, property); },
           [this](uint64_t id, const std::string& property) { return this->NodeDeletePropertyByIdViaLua(id, property); }
           ));
        lua.set_function("NodeDeleteProperties", sol::overload(
           [this](const std::string& type, const std::string& key) { return this->NodeDeletePropertiesViaLua(type, key); },
           [this](uint64_t id) { return this->NodeDeletePropertiesByIdViaLua(id); }
           ));

        // Relationship
        lua.set_function("RelationshipAdd", sol::overload(
           [this](const std::string& rel_type, const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2) { return this->RelationshipAddEmptyViaLua(rel_type, type1, key1, type2, key2); },
           [this](const std::string& rel_type, uint64_t id1, uint64_t id2) { return this->RelationshipAddEmptyByIdsViaLua(rel_type, id1, id2); },
           [this](const std::string& rel_type, const std::string& type1, const std::string& key1, const std::string& type2, const std::string& key2, const std::string& properties) { return this->RelationshipAddViaLua(rel_type, type1, key1, type2, key2, properties); },
           [this](const std::string& rel_type, uint64_t id1, uint64_t id2, const std::string& properties) { return this->RelationshipAddByIdsViaLua(rel_type, id1, id2, properties); }
           ));
        lua.set_function("RelationshipGet", &Shard::RelationshipGetViaLua, this);
        lua.set_function("RelationshipRemove", &Shard::RelationshipRemoveViaLua, this);
        lua.set_function("RelationshipGetType", &Shard::RelationshipGetTypeViaLua, this);
        lua.set_function("RelationshipGetStartingNodeId", &Shard::RelationshipGetStartingNodeIdViaLua, this);
        lua.set_function("RelationshipGetEndingNodeId", &Shard::RelationshipGetEndingNodeIdViaLua, this);

        // Relationships
        lua.set_function("RelationshipsGet", sol::overload(
                                                 [this](const std::vector<uint64_t>& ids) { return this->RelationshipsGetViaLua(ids); },
                                                 [this](const std::vector<Link>& links) { return this->RelationshipsGetByLinksViaLua(links); }
                                                 ));
        lua.set_function("RelationshipsGetType", sol::overload(
                                                 [this](const std::vector<uint64_t>& ids) { return this->RelationshipsGetTypeViaLua(ids); },
                                                 [this](const std::vector<Link>& links) { return this->RelationshipsGetTypeByLinksViaLua(links); }
                                                 ));
        lua.set_function("RelationshipsGetProperty", sol::overload(
                                                 [this](const std::vector<uint64_t>& ids, const std::string& property) { return this->RelationshipsGetPropertyViaLua(ids, property); },
                                                 [this](const std::vector<Link>& links, const std::string& property) { return this->RelationshipsGetPropertyByLinksViaLua(links, property); }
                                                 ));
        lua.set_function("RelationshipsGetProperties", sol::overload(
                                                 [this](const std::vector<uint64_t>& ids) { return this->RelationshipsGetPropertiesViaLua(ids); },
                                                 [this](const std::vector<Link>& links) { return this->RelationshipsGetPropertiesByLinksViaLua(links); }
                                                 ));

        // Relationship Properties
        lua.set_function("RelationshipGetProperty", &Shard::RelationshipGetPropertyViaLua, this);
        lua.set_function("RelationshipGetProperties", &Shard::RelationshipGetPropertiesViaLua, this);
        lua.set_function("RelationshipSetProperty", &Shard::RelationshipSetPropertyViaLua, this);
        lua.set_function("RelationshipSetPropertyFromJson", &Shard::RelationshipSetPropertyFromJsonViaLua, this);
        lua.set_function("RelationshipDeleteProperty", &Shard::RelationshipDeletePropertyViaLua, this);
        lua.set_function("RelationshipSetPropertiesFromJson", &Shard::RelationshipSetPropertiesFromJsonViaLua, this);
        lua.set_function("RelationshipResetPropertiesFromJson", &Shard::RelationshipResetPropertiesFromJsonViaLua, this);
        lua.set_function("RelationshipDeleteProperties", &Shard::RelationshipDeletePropertiesViaLua, this);

        // Node Degree
        lua.set_function("NodeGetDegree", sol::overload(
            [this](uint64_t id) { return this->NodeGetDegreeByIdViaLua(id); },
            [this](uint64_t id, Direction direction) { return this->NodeGetDegreeByIdForDirectionViaLua(id, direction); },
            [this](uint64_t id, Direction direction, const std::string& rel_type) { return this->NodeGetDegreeByIdForDirectionForTypeViaLua(id, direction, rel_type); },
            [this](uint64_t id, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetDegreeByIdForDirectionForTypesViaLua(id, direction, rel_types); },
            [this](uint64_t id, const std::string& rel_type) { return this->NodeGetDegreeByIdForTypeViaLua(id, rel_type); },
            [this](uint64_t id, const std::vector<std::string>& rel_types) { return this->NodeGetDegreeByIdForTypesViaLua(id, rel_types); },
            [this](const std::string& type, const std::string& key) { return this->NodeGetDegreeViaLua(type, key); },
            [this](const std::string& type, const std::string& key, Direction direction) { return this->NodeGetDegreeForDirectionViaLua(type, key, direction); },
            [this](const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) { return this->NodeGetDegreeForDirectionForTypeViaLua(type, key, direction, rel_type); },
            [this](const std::string& type, const std::string& key, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetDegreeForDirectionForTypesViaLua(type, key, direction, rel_types); },
            [this](const std::string& type, const std::string& key, const std::string& rel_type) { return this->NodeGetDegreeForTypeViaLua(type, key, rel_type); },
            [this](const std::string& type, const std::string& key, const std::vector<std::string>& rel_types) { return this->NodeGetDegreeForTypesViaLua(type, key, rel_types); }
            ));

        // Traversing
        //TODO: Node Get Neighbor Ids
        lua.set_function("NodeGetLinks", sol::overload(
           [this](uint64_t id) { return this->NodeGetLinksByIdViaLua(id); },
           [this](uint64_t id, Direction direction) { return this->NodeGetLinksByIdForDirectionViaLua(id, direction); },
           [this](uint64_t id, Direction direction, const std::string& rel_type) { return this->NodeGetLinksByIdForDirectionForTypeViaLua(id, direction, rel_type); },
           [this](uint64_t id, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetLinksByIdForDirectionForTypesViaLua(id, direction, rel_types); },
           [this](uint64_t id, const std::string& rel_type) { return this->NodeGetLinksByIdForTypeViaLua(id, rel_type); },
           [this](uint64_t id, const std::vector<std::string>& rel_types) { return this->NodeGetLinksByIdForTypesViaLua(id, rel_types); },
           [this](const std::string& type, const std::string& key) { return this->NodeGetLinksViaLua(type, key); },
           [this](const std::string& type, const std::string& key, Direction direction) { return this->NodeGetLinksForDirectionViaLua(type, key, direction); },
           [this](const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) { return this->NodeGetLinksForDirectionForTypeViaLua(type, key, direction, rel_type); },
           [this](const std::string& type, const std::string& key, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetLinksForDirectionForTypesViaLua(type, key, direction, rel_types); },
           [this](const std::string& type, const std::string& key, const std::string& rel_type) { return this->NodeGetLinksForTypeViaLua(type, key, rel_type); },
           [this](const std::string& type, const std::string& key, const std::vector<std::string>& rel_types) { return this->NodeGetLinksForTypesViaLua(type, key, rel_types); }
           ));

        lua.set_function("NodeGetRelationships", sol::overload(
           [this](uint64_t id) { return this->NodeGetRelationshipsByIdViaLua(id); },
           [this](uint64_t id, Direction direction) { return this->NodeGetRelationshipsByIdForDirectionViaLua(id, direction); },
           [this](uint64_t id, Direction direction, const std::string& rel_type) { return this->NodeGetRelationshipsByIdForDirectionForTypeViaLua(id, direction, rel_type); },
           [this](uint64_t id, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetRelationshipsByIdForDirectionForTypesViaLua(id, direction, rel_types); },
           [this](uint64_t id, const std::string& rel_type) { return this->NodeGetRelationshipsByIdForTypeViaLua(id, rel_type); },
           [this](uint64_t id, const std::vector<std::string>& rel_types) { return this->NodeGetRelationshipsByIdForTypesViaLua(id, rel_types); },
           [this](const std::string& type, const std::string& key) { return this->NodeGetRelationshipsViaLua(type, key); },
           [this](const std::string& type, const std::string& key, Direction direction) { return this->NodeGetRelationshipsForDirectionViaLua(type, key, direction); },
           [this](const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) { return this->NodeGetRelationshipsForDirectionForTypeViaLua(type, key, direction, rel_type); },
           [this](const std::string& type, const std::string& key, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetRelationshipsForDirectionForTypesViaLua(type, key, direction, rel_types); },
           [this](const std::string& type, const std::string& key, const std::string& rel_type) { return this->NodeGetRelationshipsForTypeViaLua(type, key, rel_type); },
           [this](const std::string& type, const std::string& key, const std::vector<std::string>& rel_types) { return this->NodeGetRelationshipsForTypesViaLua(type, key, rel_types); }
           ));

        lua.set_function("NodeGetNeighbors", sol::overload(
           [this](uint64_t id) { return this->NodeGetNeighborsByIdViaLua(id); },
           [this](uint64_t id, Direction direction) { return this->NodeGetNeighborsByIdForDirectionViaLua(id, direction); },
           [this](uint64_t id, Direction direction, const std::string& rel_type) { return this->NodeGetNeighborsByIdForDirectionForTypeViaLua(id, direction, rel_type); },
           [this](uint64_t id, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetNeighborsByIdForDirectionForTypesViaLua(id, direction, rel_types); },
           [this](uint64_t id, const std::string& rel_type) { return this->NodeGetNeighborsByIdForTypeViaLua(id, rel_type); },
           [this](uint64_t id, const std::vector<std::string>& rel_types) { return this->NodeGetNeighborsByIdForTypesViaLua(id, rel_types); },
           [this](const std::string& type, const std::string& key) { return this->NodeGetNeighborsViaLua(type, key); },
           [this](const std::string& type, const std::string& key, Direction direction) { return this->NodeGetNeighborsForDirectionViaLua(type, key, direction); },
           [this](const std::string& type, const std::string& key, Direction direction, const std::string& rel_type) { return this->NodeGetNeighborsForDirectionForTypeViaLua(type, key, direction, rel_type); },
           [this](const std::string& type, const std::string& key, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetNeighborsForDirectionForTypesViaLua(type, key, direction, rel_types); },
           [this](const std::string& type, const std::string& key, const std::string& rel_type) { return this->NodeGetNeighborsForTypeViaLua(type, key, rel_type); },
           [this](const std::string& type, const std::string& key, const std::vector<std::string>& rel_types) { return this->NodeGetNeighborsForTypesViaLua(type, key, rel_types); }
           ));

        //Bulk
        // TODO: Nodes Get Links
        lua.set_function("LinksGetLinks", sol::overload(
            [this](const std::vector<Link>& links) { return this->LinksGetLinksViaLua(links); },
            [this](const std::vector<Link>& links, Direction direction) { return this->LinksGetLinksForDirectionViaLua(links, direction); },
            [this](const std::vector<Link>& links, Direction direction, const std::string& rel_type) { return this->LinksGetLinksForDirectionForTypeViaLua(links, direction, rel_type); },
            [this](const std::vector<Link>& links, Direction direction, const std::vector<std::string>& rel_types) { return this->LinksGetLinksForDirectionForTypesViaLua(links, direction, rel_types); },
            [this](const std::vector<Link>& links, const std::string& rel_type) { return this->LinksGetLinksForTypeViaLua(links, rel_type); },
            [this](const std::vector<Link>& links, const std::vector<std::string>& rel_types) { return this->LinksGetLinksForTypesViaLua(links, rel_types); }
            ));

        lua.set_function("LinksGetRelationships", sol::overload(
            [this](const std::vector<Link>& links) { return this->LinksGetRelationshipsViaLua(links); },
            [this](const std::vector<Link>& links, Direction direction) { return this->LinksGetRelationshipsForDirectionViaLua(links, direction); },
            [this](const std::vector<Link>& links, Direction direction, const std::string& rel_type) { return this->LinksGetRelationshipsForDirectionForTypeViaLua(links, direction, rel_type); },
            [this](const std::vector<Link>& links, Direction direction, const std::vector<std::string>& rel_types) { return this->LinksGetRelationshipsForDirectionForTypesViaLua(links, direction, rel_types); },
            [this](const std::vector<Link>& links, const std::string& rel_type) { return this->LinksGetRelationshipsForTypeViaLua(links, rel_type); },
            [this](const std::vector<Link>& links, const std::vector<std::string>& rel_types) { return this->LinksGetRelationshipsForTypesViaLua(links, rel_types); }
            ));

        lua.set_function("LinksGetNeighbors", sol::overload(
            [this](const std::vector<Link>& links) { return this->LinksGetNeighborsViaLua(links); },
            [this](const std::vector<Link>& links, Direction direction) { return this->LinksGetNeighborsForDirectionViaLua(links, direction); },
            [this](const std::vector<Link>& links, Direction direction, const std::string& rel_type) { return this->LinksGetNeighborsForDirectionForTypeViaLua(links, direction, rel_type); },
            [this](const std::vector<Link>& links, Direction direction, const std::vector<std::string>& rel_types) { return this->LinksGetNeighborsForDirectionForTypesViaLua(links, direction, rel_types); },
            [this](const std::vector<Link>& links, const std::string& rel_type) { return this->LinksGetNeighborsForTypeViaLua(links, rel_type); },
            [this](const std::vector<Link>& links, const std::vector<std::string>& rel_types) { return this->LinksGetNeighborsForTypesViaLua(links, rel_types); }
            ));

        // Connected
        lua.set_function("NodeGetConnected", sol::overload(
                                               [this](uint64_t id, uint64_t id2) { return this->NodeGetConnectedViaLua(id, id2); },
                                               [this](uint64_t id, uint64_t id2, Direction direction) { return this->NodeGetConnectedViaLua(id, id2, direction); },
                                               [this](uint64_t id, uint64_t id2, Direction direction, const std::string& rel_type) { return this->NodeGetConnectedViaLua(id, id2, direction, rel_type); },
                                               [this](uint64_t id, uint64_t id2, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetConnectedViaLua(id, id2, direction, rel_types); },
                                               [this](uint64_t id, uint64_t id2, const std::string& rel_type) { return this->NodeGetConnectedViaLua(id, id2, rel_type); },
                                               [this](uint64_t id, uint64_t id2, const std::vector<std::string>& rel_types) { return this->NodeGetConnectedViaLua(id, id2, rel_types); },
                                               [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2) { return this->NodeGetConnectedViaLua(type, key, type2, key2); },
                                               [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction) { return this->NodeGetConnectedViaLua(type, key, type2, key2, direction); },
                                               [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::string& rel_type) { return this->NodeGetConnectedViaLua(type, key, type2, key2, direction, rel_type); },
                                               [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, Direction direction, const std::vector<std::string>& rel_types) { return this->NodeGetConnectedViaLua(type, key, type2, key2, direction, rel_types); },
                                               [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, const std::string& rel_type) { return this->NodeGetConnectedViaLua(type, key, type2, key2, rel_type); },
                                               [this](const std::string& type, const std::string& key, const std::string& type2, const std::string& key2, const std::vector<std::string>& rel_types) { return this->NodeGetConnectedViaLua(type, key, type2, key2, rel_types); }
                                               ));

        // All
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
        lua.set_function("FindRelationshipCount", &Shard::FindRelationshipCountViaLua, this);
        lua.set_function("FindRelationshipIds", &Shard::FindRelationshipIdsViaLua, this);
        lua.set_function("FindRelationships", &Shard::FindRelationshipsViaLua, this);

        // Filter
        lua.set_function("FilterNodeCount", &Shard::FilterNodeCountViaLua, this);
        lua.set_function("FilterNodeIds", &Shard::FilterNodeIdsViaLua, this);
        lua.set_function("FilterNodes", &Shard::FilterNodesViaLua, this);
        lua.set_function("FilterRelationshipCount", &Shard::FilterRelationshipCountViaLua, this);
        lua.set_function("FilterRelationshipIds", &Shard::FilterRelationshipIdsViaLua, this);
        lua.set_function("FilterRelationships", &Shard::FilterRelationshipsViaLua, this);

        // Intersect
        lua.set_function("IntersectIds", &Shard::IntersectIdsViaLua, this);
        lua.set_function("IntersectNodes", &Shard::IntersectNodesViaLua, this);
        lua.set_function("IntersectRelationships", &Shard::IntersectRelationshipsViaLua, this);
        lua.set_function("IntersectIdsCount", &Shard::IntersectIdsCountViaLua, this);

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