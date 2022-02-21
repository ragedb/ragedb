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

unsigned int SHARD_BITS;
unsigned int SHARD_MASK;

using namespace roaring;
namespace ragedb {

    Shard::Shard(uint _cpus) : cpus(_cpus), shard_id(seastar::this_shard_id()) {
        // Set the Shard Bits and Mask
        SHARD_BITS = std::bit_width(_cpus);
        SHARD_MASK = (1 << SHARD_BITS) - 1;

        lua.open_libraries(sol::lib::base, sol::lib::package, sol::lib::math, sol::lib::string, sol::lib::table, sol::lib::io, sol::lib::os);
        lua.require_script("json", Lua::json_script);
        lua.require_script("ftcsv", Lua::ftcsv_script);
        lua.require_script("date", Lua::date_script);

        // TODO: Create a sanitized environment to sandbox the user's Lua code, and put these user types there.

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
          "getIds", &Roar::getIds,
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
            [this](std::string type) { return this->RelationshipTypesGetCountByTypeViaLua(type); },
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
            [this](std::string type) { return this->NodeTypesGetCountByTypeViaLua(type); },
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
        lua.set_function("NodeAddEmpty", &Shard::NodeAddEmptyViaLua, this);
        lua.set_function("NodeAdd", &Shard::NodeAddViaLua, this);
        lua.set_function("NodeGetId", &Shard::NodeGetIdViaLua, this);
        lua.set_function("NodeGet", sol::overload(
            [this](std::string type, std::string key) { return this->NodeGetViaLua(type, key); },
            [this](uint64_t id) { return this->NodeGetByIdViaLua(id); }
            ));
        lua.set_function("NodeRemove", sol::overload(
            [this](std::string type, std::string key) { return this->NodeRemoveViaLua(type, key); },
            [this](uint64_t id) { return this->NodeRemoveByIdViaLua(id); }
            ));
        lua.set_function("NodeGetType", &Shard::NodeGetTypeViaLua, this);
        lua.set_function("NodeGetKey", &Shard::NodeGetKeyViaLua, this);

        // Nodes
        lua.set_function("NodesGet", &Shard::NodesGetViaLua, this);
        lua.set_function("NodesGetByLinks", &Shard::NodesGetByLinksViaLua, this);
        lua.set_function("NodesGetKey", &Shard::NodesGetKeyViaLua, this);
        lua.set_function("NodesGetKeyByLinks", &Shard::NodesGetKeyByLinksViaLua, this);
        lua.set_function("NodesGetType", &Shard::NodesGetTypeViaLua, this);
        lua.set_function("NodesGetTypeByLinks", &Shard::NodesGetTypeByLinksViaLua, this);
        lua.set_function("NodesGetProperty", &Shard::NodesGetPropertyViaLua, this);
        lua.set_function("NodesGetPropertyByLinks", &Shard::NodesGetPropertyByLinksViaLua, this);
        lua.set_function("NodesGetProperties", &Shard::NodesGetPropertiesViaLua, this);
        lua.set_function("NodesGetPropertiesByLinks", &Shard::NodesGetPropertiesByLinksViaLua, this);

        // Node Properties
        lua.set_function("NodeGetProperty", sol::overload(
           [this](std::string type, std::string key, std::string property) { return this->NodeGetPropertyViaLua(type, key, property); },
           [this](uint64_t id, std::string property) { return this->NodeGetPropertyByIdViaLua(id, property); }
           ));
        lua.set_function("NodeGetProperties", sol::overload(
           [this](std::string type, std::string key) { return this->NodeGetProperties(type, key); },
           [this](uint64_t id) { return this->NodeGetPropertiesByIdViaLua(id); }
           ));
        lua.set_function("NodeSetProperty", sol::overload(
           [this](std::string type, std::string key, std::string property, sol::object value) { return this->NodeSetPropertyViaLua(type, key, property, value); },
           [this](uint64_t id, std::string property, sol::object value) { return this->NodeSetPropertyByIdViaLua(id, property, value); }
           ));
        lua.set_function("NodeSetPropertiesFromJson", sol::overload(
           [this](std::string type, std::string key, std::string properties) { return this->NodeSetPropertiesFromJsonViaLua(type, key, properties); },
           [this](uint64_t id, std::string properties) { return this->NodeSetPropertiesFromJsonByIdViaLua(id, properties); }
           ));
        lua.set_function("NodeResetPropertiesFromJson", sol::overload(
           [this](std::string type, std::string key, std::string properties) { return this->NodeResetPropertiesFromJsonViaLua(type, key, properties); },
           [this](uint64_t id, std::string properties) { return this->NodeResetPropertiesFromJsonByIdViaLua(id, properties); }
           ));
        lua.set_function("NodeDeleteProperty", sol::overload(
           [this](std::string type, std::string key, std::string property) { return this->NodeDeletePropertyViaLua(type, key, property); },
           [this](uint64_t id, std::string property) { return this->NodeDeletePropertyByIdViaLua(id, property); }
           ));
        lua.set_function("NodeDeleteProperties", sol::overload(
           [this](std::string type, std::string key) { return this->NodeDeletePropertiesViaLua(type, key); },
           [this](uint64_t id) { return this->NodeDeletePropertiesByIdViaLua(id); }
           ));

        // Relationship
        lua.set_function("RelationshipAddEmpty", &Shard::RelationshipAddEmptyViaLua, this);
        lua.set_function("RelationshipAddEmptyByIds", &Shard::RelationshipAddEmptyByIdsViaLua, this);
        lua.set_function("RelationshipAdd", &Shard::RelationshipAddViaLua, this);
        lua.set_function("RelationshipAddByIds", &Shard::RelationshipAddByIdsViaLua, this);
        lua.set_function("RelationshipGet", &Shard::RelationshipGetViaLua, this);
        lua.set_function("RelationshipRemove", &Shard::RelationshipRemoveViaLua, this);
        lua.set_function("RelationshipGetType", &Shard::RelationshipGetTypeViaLua, this);
        lua.set_function("RelationshipGetStartingNodeId", &Shard::RelationshipGetStartingNodeIdViaLua, this);
        lua.set_function("RelationshipGetEndingNodeId", &Shard::RelationshipGetEndingNodeIdViaLua, this);

        // Relationships
        lua.set_function("RelationshipsGet", &Shard::RelationshipsGetViaLua, this);
        lua.set_function("RelationshipsGetByLinks", &Shard::RelationshipsGetByLinksViaLua, this);
        lua.set_function("RelationshipsGetType", &Shard::RelationshipsGetTypeViaLua, this);
        lua.set_function("RelationshipsGetTypeByLinks", &Shard::RelationshipsGetTypeByLinksViaLua, this);
        lua.set_function("RelationshipsGetProperty", &Shard::RelationshipsGetPropertyViaLua, this);
        lua.set_function("RelationshipsGetPropertyByLinks", &Shard::RelationshipsGetPropertyByLinksViaLua, this);
        lua.set_function("RelationshipsGetProperties", &Shard::RelationshipsGetPropertiesViaLua, this);
        lua.set_function("RelationshipsGetPropertiesByLinks", &Shard::RelationshipsGetPropertiesByLinksViaLua, this);

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
            [this](uint64_t id, Direction direction, std::string rel_type) { return this->NodeGetDegreeByIdForDirectionForTypeViaLua(id, direction, rel_type); },
            [this](uint64_t id, Direction direction, std::vector<std::string> rel_types) { return this->NodeGetDegreeByIdForDirectionForTypesViaLua(id, direction, rel_types); },
            [this](uint64_t id, std::string rel_type) { return this->NodeGetDegreeByIdForTypeViaLua(id, rel_type); },
            [this](uint64_t id, std::vector<std::string> rel_types) { return this->NodeGetDegreeByIdForTypesViaLua(id, rel_types); },
            [this](std::string type, std::string key) { return this->NodeGetDegreeViaLua(type, key); },
            [this](std::string type, std::string key, Direction direction) { return this->NodeGetDegreeForDirectionViaLua(type, key, direction); },
            [this](std::string type, std::string key, Direction direction, std::string rel_type) { return this->NodeGetDegreeForDirectionForTypeViaLua(type, key, direction, rel_type); },
            [this](std::string type, std::string key, Direction direction, std::vector<std::string> rel_types) { return this->NodeGetDegreeForDirectionForTypesViaLua(type, key, direction, rel_types); },
            [this](std::string type, std::string key, std::string rel_type) { return this->NodeGetDegreeForTypeViaLua(type, key, rel_type); },
            [this](std::string type, std::string key, std::vector<std::string> rel_types) { return this->NodeGetDegreeForTypesViaLua(type, key, rel_types); }
            ));

        // Traversing
        //TODO: Node Get Neighbor Ids
        lua.set_function("NodeGetLinks", sol::overload(
           [this](uint64_t id) { return this->NodeGetLinksByIdViaLua(id); },
           [this](uint64_t id, Direction direction) { return this->NodeGetLinksByIdForDirectionViaLua(id, direction); },
           [this](uint64_t id, Direction direction, std::string rel_type) { return this->NodeGetLinksByIdForDirectionForTypeViaLua(id, direction, rel_type); },
           [this](uint64_t id, Direction direction, std::vector<std::string> rel_types) { return this->NodeGetLinksByIdForDirectionForTypesViaLua(id, direction, rel_types); },
           [this](uint64_t id, std::string rel_type) { return this->NodeGetLinksByIdForTypeViaLua(id, rel_type); },
           [this](uint64_t id, std::vector<std::string> rel_types) { return this->NodeGetLinksByIdForTypesViaLua(id, rel_types); },
           [this](std::string type, std::string key) { return this->NodeGetLinksViaLua(type, key); },
           [this](std::string type, std::string key, Direction direction) { return this->NodeGetLinksForDirectionViaLua(type, key, direction); },
           [this](std::string type, std::string key, Direction direction, std::string rel_type) { return this->NodeGetLinksForDirectionForTypeViaLua(type, key, direction, rel_type); },
           [this](std::string type, std::string key, Direction direction, std::vector<std::string> rel_types) { return this->NodeGetLinksForDirectionForTypesViaLua(type, key, direction, rel_types); },
           [this](std::string type, std::string key, std::string rel_type) { return this->NodeGetLinksForTypeViaLua(type, key, rel_type); },
           [this](std::string type, std::string key, std::vector<std::string> rel_types) { return this->NodeGetLinksForTypesViaLua(type, key, rel_types); }
           ));

        lua.set_function("NodeGetRelationships", sol::overload(
           [this](uint64_t id) { return this->NodeGetRelationshipsByIdViaLua(id); },
           [this](uint64_t id, Direction direction) { return this->NodeGetRelationshipsByIdForDirectionViaLua(id, direction); },
           [this](uint64_t id, Direction direction, std::string rel_type) { return this->NodeGetRelationshipsByIdForDirectionForTypeViaLua(id, direction, rel_type); },
           [this](uint64_t id, Direction direction, std::vector<std::string> rel_types) { return this->NodeGetRelationshipsByIdForDirectionForTypesViaLua(id, direction, rel_types); },
           [this](uint64_t id, std::string rel_type) { return this->NodeGetRelationshipsByIdForTypeViaLua(id, rel_type); },
           [this](uint64_t id, std::vector<std::string> rel_types) { return this->NodeGetRelationshipsByIdForTypesViaLua(id, rel_types); },
           [this](std::string type, std::string key) { return this->NodeGetRelationshipsViaLua(type, key); },
           [this](std::string type, std::string key, Direction direction) { return this->NodeGetRelationshipsForDirectionViaLua(type, key, direction); },
           [this](std::string type, std::string key, Direction direction, std::string rel_type) { return this->NodeGetRelationshipsForDirectionForTypeViaLua(type, key, direction, rel_type); },
           [this](std::string type, std::string key, Direction direction, std::vector<std::string> rel_types) { return this->NodeGetRelationshipsForDirectionForTypesViaLua(type, key, direction, rel_types); },
           [this](std::string type, std::string key, std::string rel_type) { return this->NodeGetRelationshipsForTypeViaLua(type, key, rel_type); },
           [this](std::string type, std::string key, std::vector<std::string> rel_types) { return this->NodeGetRelationshipsForTypesViaLua(type, key, rel_types); }
           ));

        lua.set_function("NodeGetNeighbors", sol::overload(
           [this](uint64_t id) { return this->NodeGetNeighborsByIdViaLua(id); },
           [this](uint64_t id, Direction direction) { return this->NodeGetNeighborsByIdForDirectionViaLua(id, direction); },
           [this](uint64_t id, Direction direction, std::string rel_type) { return this->NodeGetNeighborsByIdForDirectionForTypeViaLua(id, direction, rel_type); },
           [this](uint64_t id, Direction direction, std::vector<std::string> rel_types) { return this->NodeGetNeighborsByIdForDirectionForTypesViaLua(id, direction, rel_types); },
           [this](uint64_t id, std::string rel_type) { return this->NodeGetNeighborsByIdForTypeViaLua(id, rel_type); },
           [this](uint64_t id, std::vector<std::string> rel_types) { return this->NodeGetNeighborsByIdForTypesViaLua(id, rel_types); },
           [this](std::string type, std::string key) { return this->NodeGetNeighborsViaLua(type, key); },
           [this](std::string type, std::string key, Direction direction) { return this->NodeGetNeighborsForDirectionViaLua(type, key, direction); },
           [this](std::string type, std::string key, Direction direction, std::string rel_type) { return this->NodeGetNeighborsForDirectionForTypeViaLua(type, key, direction, rel_type); },
           [this](std::string type, std::string key, Direction direction, std::vector<std::string> rel_types) { return this->NodeGetNeighborsForDirectionForTypesViaLua(type, key, direction, rel_types); },
           [this](std::string type, std::string key, std::string rel_type) { return this->NodeGetNeighborsForTypeViaLua(type, key, rel_type); },
           [this](std::string type, std::string key, std::vector<std::string> rel_types) { return this->NodeGetNeighborsForTypesViaLua(type, key, rel_types); }
           ));

        //Bulk
        // TODO: Nodes Get Links
        lua.set_function("LinksGetLinks",sol::overload(
            [this](std::vector<Link> links) { return this->LinksGetLinksViaLua(links); },
            [this](std::vector<Link> links, Direction direction) { return this->LinksGetLinksForDirectionViaLua(links, direction); },
            [this](std::vector<Link> links, Direction direction, std::string rel_type) { return this->LinksGetLinksForDirectionForTypeViaLua(links, direction, rel_type); },
            [this](std::vector<Link> links, Direction direction, std::vector<std::string> rel_types) { return this->LinksGetLinksForDirectionForTypesViaLua(links, direction, rel_types); },
            [this](std::vector<Link> links, std::string rel_type) { return this->LinksGetLinksForTypeViaLua(links, rel_type); },
            [this](std::vector<Link> links, std::vector<std::string> rel_types) { return this->LinksGetLinksForTypesViaLua(links, rel_types); }
            ));

        lua.set_function("LinksGetRelationships",sol::overload(
            [this](std::vector<Link> links) { return this->LinksGetRelationshipsViaLua(links); },
            [this](std::vector<Link> links, Direction direction) { return this->LinksGetRelationshipsForDirectionViaLua(links, direction); },
            [this](std::vector<Link> links, Direction direction, std::string rel_type) { return this->LinksGetRelationshipsForDirectionForTypeViaLua(links, direction, rel_type); },
            [this](std::vector<Link> links, Direction direction, std::vector<std::string> rel_types) { return this->LinksGetRelationshipsForDirectionForTypesViaLua(links, direction, rel_types); },
            [this](std::vector<Link> links, std::string rel_type) { return this->LinksGetRelationshipsForTypeViaLua(links, rel_type); },
            [this](std::vector<Link> links, std::vector<std::string> rel_types) { return this->LinksGetRelationshipsForTypesViaLua(links, rel_types); }
            ));

        lua.set_function("LinksGetNeighbors",sol::overload(
            [this](std::vector<Link> links) { return this->LinksGetNeighborsViaLua(links); },
            [this](std::vector<Link> links, Direction direction) { return this->LinksGetNeighborsForDirectionViaLua(links, direction); },
            [this](std::vector<Link> links, Direction direction, std::string rel_type) { return this->LinksGetNeighborsForDirectionForTypeViaLua(links, direction, rel_type); },
            [this](std::vector<Link> links, Direction direction, std::vector<std::string> rel_types) { return this->LinksGetNeighborsForDirectionForTypesViaLua(links, direction, rel_types); },
            [this](std::vector<Link> links, std::string rel_type) { return this->LinksGetNeighborsForTypeViaLua(links, rel_type); },
            [this](std::vector<Link> links, std::vector<std::string> rel_types) { return this->LinksGetNeighborsForTypesViaLua(links, rel_types); }
            ));

        // All
        lua.set_function("AllNodeIds", &Shard::AllNodeIdsViaLua, this);
        lua.set_function("AllNodeIdsForType", &Shard::AllNodeIdsForTypeViaLua, this);
        lua.set_function("AllRelationshipIds", &Shard::AllRelationshipIdsViaLua, this);
        lua.set_function("AllRelationshipIdsForType", &Shard::AllRelationshipIdsForTypeViaLua, this);
        lua.set_function("AllNodes", &Shard::AllNodesViaLua, this);
        lua.set_function("AllNodesForType", &Shard::AllNodesForTypeViaLua, this);
        lua.set_function("AllRelationships", &Shard::AllRelationshipsViaLua, this);
        lua.set_function("AllRelationshipsForType", &Shard::AllRelationshipsForTypeViaLua, this);

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

    seastar::future<std::string> Shard::RunLua(const std::string &script) {

        return seastar::async([script, this] () {
            // Inject json encoding
            std::stringstream ss(script);
            std::string line;
            std::vector<std::string> lines;
            while(std::getline(ss,line,'\n')){
                lines.emplace_back(line);
            }

            std::string json_function = "local json = require('json')";
            lines.back() = "return json.encode({" + lines.back() + "})";

            std::string executable = json_function + join(lines, " ");
            sol::protected_function_result script_result;
            // We only have one Lua VM for each Core, so lock it during use.
            this->lua_lock.for_write().lock().get();
            try {
                script_result = lua.safe_script(executable, [] (lua_State *, sol::protected_function_result pfr) {
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

    // Helpers
    std::pair <uint16_t, uint64_t> Shard::RelationshipRemoveGetIncoming(uint64_t external_id) {

        uint16_t rel_type_id = externalToTypeId(external_id);
        uint64_t internal_id = externalToInternal(external_id);

        uint64_t id1 = relationship_types.getStartingNodeId(rel_type_id, internal_id);
        uint64_t id2 = relationship_types.getEndingNodeId(rel_type_id, internal_id);
        uint16_t id1_type_id = externalToTypeId(id1);

        // Add to deleted relationships bitmap
        relationship_types.removeId(rel_type_id, internal_id);

        uint64_t internal_id1 = externalToInternal(id1);

        // Remove relationship from Node 1
        auto group = find_if(std::begin(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)),
                             std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1)),
                             [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

        if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
            auto rel_to_delete = find_if(std::begin(group->links), std::end(group->links), [external_id](Link entry) {
                return entry.rel_id == external_id;
            });
            if (rel_to_delete != std::end(group->links)) {
                group->links.erase(rel_to_delete);
            }
        }

        // Clear the relationship
        relationship_types.setStartingNodeId(rel_type_id, internal_id, 0);
        relationship_types.setEndingNodeId(rel_type_id, internal_id, 0);
        relationship_types.deleteProperties(rel_type_id, internal_id);

        // Return the rel_type and other node Id
        return std::pair <uint16_t ,uint64_t> (rel_type_id, id2);
    }

    bool Shard::RelationshipRemoveIncoming(uint16_t rel_type_id, uint64_t external_id, uint64_t node_id) {
        // Remove relationship from Node 2
        uint64_t internal_id2 = externalToInternal(node_id);
        uint16_t id2_type_id = externalToTypeId(node_id);

        auto group = find_if(std::begin(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)),
                             std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2)),
                             [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );

        auto rel_to_delete = find_if(std::begin(group->links), std::end(group->links), [external_id](Link entry) {
            return entry.rel_id == external_id;
        });
        if (rel_to_delete != std::end(group->links)) {
            group->links.erase(rel_to_delete);
        }

        return true;
    }

}