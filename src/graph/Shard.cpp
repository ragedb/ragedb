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

namespace ragedb {

    Shard::Shard(uint _cpus) : cpus(_cpus), shard_id(seastar::this_shard_id()) {

        lua.open_libraries(sol::lib::base, sol::lib::package, sol::lib::math, sol::lib::string, sol::lib::table, sol::lib::io);
        lua.require_file("json", "./lua/json.lua");
        lua.require_file("ftcsv", "./lua/ftcsv.lua");

        // TODO: Create a sanitized environment to sandbox the user's Lua code, and put these user types there.

        lua.new_enum("Direction",
          "BOTH", Direction::BOTH,
          "IN", Direction::IN,
          "OUT", Direction::OUT
        );

        lua.new_usertype<Node>("Node",
                // 3 constructors
                                 sol::constructors<
                                         Node(),
                                         Node(uint64_t, std::string, std::string),
                                         Node(uint64_t, std::string, std::string, std::map<std::string, std::any>)>(),
                                 "getId", &Node::getId,
                                 "getTypeId", &Node::getTypeId,
                                 "getType", &Node::getType,
                                 "getKey", &Node::getKey,
                                 "getProperties", &Node::getPropertiesLua,
                                 "getProperty", &Node::getProperty);

        lua.new_usertype<Relationship>("Relationship",
                // 3 constructors
                                         sol::constructors<
                                                 Relationship(),
                                                 Relationship(uint64_t, std::string, uint64_t, uint16_t),
                                                 Relationship(uint64_t, std::string, uint64_t, uint16_t, std::map<std::string, std::any>)>(),
                                         "getId", &Relationship::getId,
                                         "getTypeId", &Relationship::getTypeId,
                                         "getType", &Relationship::getType,
                                         "getStartingNodeId", &Relationship::getStartingNodeId,
                                         "getEndingNodeId", &Relationship::getEndingNodeId,
                                         "getProperties", &Relationship::getPropertiesLua,
                                         "getProperty", &Relationship::getProperty);

        lua.new_usertype<Link>("Link",
                                sol::constructors<Link(uint64_t, uint64_t)>(),
                                "getNodeId", &Link::getNodeId,
                                "getRelationshipId", &Link::getRelationshipId);

        // Lua does not like overloading, Sol warns about performance problems if we overload, so overloaded methods have been renamed.

        // Relationship Types
        lua.set_function("RelationshipTypesGetCount", &Shard::RelationshipTypesGetCountViaLua, this);
        lua.set_function("RelationshipTypesGetCountByType", &Shard::RelationshipTypesGetCountByTypeViaLua, this);
        lua.set_function("RelationshipTypesGetCountById", &Shard::RelationshipTypesGetCountByIdViaLua, this);
        lua.set_function("RelationshipTypesGet", &Shard::RelationshipTypesGetViaLua, this);
        lua.set_function("RelationshipTypeGet", &Shard::RelationshipTypeGetViaLua, this);

        // Relationship Type
        lua.set_function("RelationshipTypeGetType", &Shard::RelationshipTypeGetTypeViaLua, this);
        lua.set_function("RelationshipTypeGetTypeId", &Shard::RelationshipTypeGetTypeIdViaLua, this);
        lua.set_function("RelationshipTypeInsert", &Shard::RelationshipTypeInsertViaLua, this);

        // Node Types
        lua.set_function("NodeTypesGetCount", &Shard::NodeTypesGetCountViaLua, this);
        lua.set_function("NodeTypesGetCountByType", &Shard::NodeTypesGetCountByTypeViaLua, this);
        lua.set_function("NodeTypesGetCountById", &Shard::NodeTypesGetCountByIdViaLua, this);
        lua.set_function("NodeTypesGet", &Shard::NodeTypesGetViaLua, this);
        lua.set_function("NodeTypeGet", &Shard::NodeTypeGetViaLua, this);

        // Node Type
        lua.set_function("NodeTypeGetType", &Shard::NodeTypeGetTypeViaLua, this);
        lua.set_function("NodeTypeGetTypeId", &Shard::NodeTypeGetTypeIdViaLua, this);
        lua.set_function("NodeTypeInsert", &Shard::NodeTypeInsertViaLua, this);

        // Property Types
        lua.set_function("NodePropertyTypeAdd", &Shard::NodePropertyTypeAddViaLua, this);
        lua.set_function("RelationshipPropertyTypeAdd", &Shard::RelationshipPropertyTypeAddViaLua, this);
        lua.set_function("NodePropertyTypeDelete", &Shard::NodePropertyTypeDeleteViaLua, this);
        lua.set_function("RelationshipPropertyTypeDelete", &Shard::RelationshipPropertyTypeDeleteViaLua, this);

        //Nodes
        lua.set_function("NodeAddEmpty", &Shard::NodeAddEmptyViaLua, this);
        lua.set_function("NodeAdd", &Shard::NodeAddViaLua, this);
        lua.set_function("NodeGetId", &Shard::NodeGetIdViaLua, this);
        lua.set_function("NodesGet", &Shard::NodesGetViaLua, this);
        lua.set_function("NodeGet", &Shard::NodeGetViaLua, this);
        lua.set_function("NodeGetById", &Shard::NodeGetByIdViaLua, this);
        lua.set_function("NodeRemove", &Shard::NodeRemoveViaLua, this);
        lua.set_function("NodeRemoveById", &Shard::NodeRemoveByIdViaLua, this);
        lua.set_function("NodeGetTypeId", &Shard::NodeGetTypeIdViaLua, this);
        lua.set_function("NodeGetType", &Shard::NodeGetTypeViaLua, this);
        lua.set_function("NodeGetKey", &Shard::NodeGetKeyViaLua, this);

        // Node Properties
        lua.set_function("NodePropertyGet", &Shard::NodePropertyGetViaLua, this);
        lua.set_function("NodePropertyGetById", &Shard::NodePropertyGetByIdViaLua, this);
        lua.set_function("NodePropertiesGet", &Shard::NodePropertiesGetViaLua, this);
        lua.set_function("NodePropertiesGetById", &Shard::NodePropertiesGetByIdViaLua, this);
        lua.set_function("NodePropertySet", &Shard::NodePropertySetViaLua, this);
        lua.set_function("NodePropertySetById", &Shard::NodePropertySetByIdViaLua, this);
        lua.set_function("NodePropertiesSetFromJson", &Shard::NodePropertiesSetFromJsonViaLua, this);
        lua.set_function("NodePropertiesSetFromJsonById", &Shard::NodePropertiesSetFromJsonByIdViaLua, this);
        lua.set_function("NodePropertiesResetFromJson", &Shard::NodePropertiesResetFromJsonViaLua, this);
        lua.set_function("NodePropertiesResetFromJsonById", &Shard::NodePropertiesResetFromJsonByIdViaLua, this);
        lua.set_function("NodePropertyDelete", &Shard::NodePropertyDeleteViaLua, this);
        lua.set_function("NodePropertyDeleteById", &Shard::NodePropertyDeleteByIdViaLua, this);
        lua.set_function("NodePropertiesDelete", &Shard::NodePropertiesDeleteViaLua, this);
        lua.set_function("NodePropertiesDeleteById", &Shard::NodePropertiesDeleteByIdViaLua, this);

        // Relationships
        lua.set_function("RelationshipAddEmpty", &Shard::RelationshipAddEmptyViaLua, this);
        lua.set_function("RelationshipAddEmptyByTypeIdByIds", &Shard::RelationshipAddEmptyByTypeIdByIdsViaLua, this);
        lua.set_function("RelationshipAddEmptyByIds", &Shard::RelationshipAddEmptyByIdsViaLua, this);
        lua.set_function("RelationshipAdd", &Shard::RelationshipAddViaLua, this);
        lua.set_function("RelationshipAddByTypeIdByIds", &Shard::RelationshipAddByTypeIdByIdsViaLua, this);
        lua.set_function("RelationshipAddByIds", &Shard::RelationshipAddByIdsViaLua, this);
        lua.set_function("RelationshipGet", &Shard::RelationshipGetViaLua, this);
        lua.set_function("RelationshipsGet", &Shard::RelationshipsGetViaLua, this);
        lua.set_function("RelationshipRemove", &Shard::RelationshipRemoveViaLua, this);
        lua.set_function("RelationshipGetType", &Shard::RelationshipGetTypeViaLua, this);
        lua.set_function("RelationshipGetTypeId", &Shard::RelationshipGetTypeIdViaLua, this);
        lua.set_function("RelationshipGetStartingNodeId", &Shard::RelationshipGetStartingNodeIdViaLua, this);
        lua.set_function("RelationshipGetEndingNodeId", &Shard::RelationshipGetEndingNodeIdViaLua, this);

        // Relationship Properties
        lua.set_function("RelationshipPropertyGet", &Shard::RelationshipPropertyGetViaLua, this);
        lua.set_function("RelationshipPropertiesGet", &Shard::RelationshipPropertiesGetViaLua, this);
        lua.set_function("RelationshipPropertySet", &Shard::RelationshipPropertySetViaLua, this);
        lua.set_function("RelationshipPropertySetFromJson", &Shard::RelationshipPropertySetFromJsonViaLua, this);
        lua.set_function("RelationshipPropertyDelete", &Shard::RelationshipPropertyDeleteViaLua, this);
        lua.set_function("RelationshipPropertiesSetFromJson", &Shard::RelationshipPropertiesSetFromJsonViaLua, this);
        lua.set_function("RelationshipPropertiesResetFromJson", &Shard::RelationshipPropertiesResetFromJsonViaLua, this);
        lua.set_function("RelationshipPropertiesDelete", &Shard::RelationshipPropertiesDeleteViaLua, this);

        // Node Degree
        lua.set_function("NodeGetDegree", &Shard::NodeGetDegreeViaLua, this);
        lua.set_function("NodeGetDegreeForDirection", &Shard::NodeGetDegreeForDirectionViaLua, this);
        lua.set_function("NodeGetDegreeForDirectionForType", &Shard::NodeGetDegreeForDirectionForTypeViaLua, this);
        lua.set_function("NodeGetDegreeForType", &Shard::NodeGetDegreeForTypeViaLua, this);
        lua.set_function("NodeGetDegreeForDirectionForTypes", &Shard::NodeGetDegreeForDirectionForTypesViaLua, this);
        lua.set_function("NodeGetDegreeForTypes", &Shard::NodeGetDegreeForTypesViaLua, this);
        lua.set_function("NodeGetDegreeById", &Shard::NodeGetDegreeByIdViaLua, this);
        lua.set_function("NodeGetDegreeByIdForDirection", &Shard::NodeGetDegreeByIdForDirectionViaLua, this);
        lua.set_function("NodeGetDegreeByIdForDirectionForType", &Shard::NodeGetDegreeByIdForDirectionForTypeViaLua, this);
        lua.set_function("NodeGetDegreeByIdForType", &Shard::NodeGetDegreeByIdForTypeViaLua, this);
        lua.set_function("NodeGetDegreeByIdForDirectionForTypes", &Shard::NodeGetDegreeByIdForDirectionForTypesViaLua, this);
        lua.set_function("NodeGetDegreeByIdForTypes", &Shard::NodeGetDegreeByIdForTypesViaLua, this);

        // Traversing
        lua.set_function("NodeGetRelationshipsIds", &Shard::NodeGetRelationshipsIdsViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsForDirection", &Shard::NodeGetRelationshipsIdsForDirectionViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsForDirectionForType", &Shard::NodeGetRelationshipsIdsForDirectionForTypeViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsForDirectionForTypeId", &Shard::NodeGetRelationshipsIdsForDirectionForTypeIdViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsForDirectionForTypes", &Shard::NodeGetRelationshipsIdsForDirectionForTypesViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsForType", &Shard::NodeGetRelationshipsIdsForTypeViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsForTypeId", &Shard::NodeGetRelationshipsIdsForTypeIdViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsForTypes", &Shard::NodeGetRelationshipsIdsForTypesViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsById", &Shard::NodeGetRelationshipsIdsByIdViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsByIdForDirection", &Shard::NodeGetRelationshipsIdsByIdForDirectionViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsByIdForDirectionForType", &Shard::NodeGetRelationshipsIdsByIdForDirectionForTypeViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsByIdForDirectionForTypeId", &Shard::NodeGetRelationshipsIdsByIdForDirectionForTypeIdViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsByIdForDirectionForTypes", &Shard::NodeGetRelationshipsIdsByIdForDirectionForTypesViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsByIdForType", &Shard::NodeGetRelationshipsIdsByIdForTypeViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsByIdForTypeId", &Shard::NodeGetRelationshipsIdsByIdForTypeIdViaLua, this);
        lua.set_function("NodeGetRelationshipsIdsByIdForTypes", &Shard::NodeGetRelationshipsIdsByIdForTypesViaLua, this);

        lua.set_function("NodeGetRelationships", &Shard::NodeGetRelationshipsViaLua, this);
        lua.set_function("NodeGetRelationshipsForType", &Shard::NodeGetRelationshipsForTypeViaLua, this);
        lua.set_function("NodeGetRelationshipsForTypeId", &Shard::NodeGetRelationshipsForTypeIdViaLua, this);
        lua.set_function("NodeGetRelationshipsForTypes", &Shard::NodeGetRelationshipsForTypesViaLua, this);
        lua.set_function("NodeGetRelationshipsById", &Shard::NodeGetRelationshipsByIdViaLua, this);
        lua.set_function("NodeGetRelationshipsByIdForType", &Shard::NodeGetRelationshipsByIdForTypeViaLua, this);
        lua.set_function("NodeGetRelationshipsByIdForTypeId", &Shard::NodeGetRelationshipsByIdForTypeIdViaLua, this);
        lua.set_function("NodeGetRelationshipsByIdForTypes", &Shard::NodeGetRelationshipsByIdForTypesViaLua, this);
        lua.set_function("NodeGetRelationshipsForDirection", &Shard::NodeGetRelationshipsForDirectionViaLua, this);
        lua.set_function("NodeGetRelationshipsForDirectionForType", &Shard::NodeGetRelationshipsForDirectionForTypeViaLua, this);
        lua.set_function("NodeGetRelationshipsForDirectionForTypeId", &Shard::NodeGetRelationshipsForDirectionForTypeIdViaLua, this);
        lua.set_function("NodeGetRelationshipsForDirectionForTypes", &Shard::NodeGetRelationshipsForDirectionForTypesViaLua, this);
        lua.set_function("NodeGetRelationshipsByIdForDirection", &Shard::NodeGetRelationshipsByIdForDirectionViaLua, this);
        lua.set_function("NodeGetRelationshipsByIdForDirectionForType", &Shard::NodeGetRelationshipsByIdForDirectionForTypeViaLua, this);
        lua.set_function("NodeGetRelationshipsByIdForDirectionForTypeId", &Shard::NodeGetRelationshipsByIdForDirectionForTypeIdViaLua, this);
        lua.set_function("NodeGetRelationshipsByIdForDirectionForTypes", &Shard::NodeGetRelationshipsByIdForDirectionForTypesViaLua, this);

        lua.set_function("NodeGetNeighbors", &Shard::NodeGetNeighborsViaLua, this);
        lua.set_function("NodeGetNeighborsForType", &Shard::NodeGetNeighborsForTypeViaLua, this);
        lua.set_function("NodeGetNeighborsForTypeId", &Shard::NodeGetNeighborsForTypeIdViaLua, this);
        lua.set_function("NodeGetNeighborsForTypes", &Shard::NodeGetNeighborsForTypesViaLua, this);
        lua.set_function("NodeGetNeighborsById", &Shard::NodeGetNeighborsByIdViaLua, this);
        lua.set_function("NodeGetNeighborsByIdForType", &Shard::NodeGetNeighborsByIdForTypeViaLua, this);
        lua.set_function("NodeGetNeighborsByIdForTypeId", &Shard::NodeGetNeighborsByIdForTypeIdViaLua, this);
        lua.set_function("NodeGetNeighborsByIdForTypes", &Shard::NodeGetNeighborsByIdForTypesViaLua, this);
        lua.set_function("NodeGetNeighborsForDirection", &Shard::NodeGetNeighborsForDirectionViaLua, this);
        lua.set_function("NodeGetNeighborsForDirectionForType", &Shard::NodeGetNeighborsForDirectionForTypeViaLua, this);
        lua.set_function("NodeGetNeighborsForDirectionForTypeId", &Shard::NodeGetNeighborsForDirectionForTypeIdViaLua, this);
        lua.set_function("NodeGetNeighborsForDirectionForTypes", &Shard::NodeGetNeighborsForDirectionForTypesViaLua, this);
        lua.set_function("NodeGetNeighborsByIdForDirection", &Shard::NodeGetNeighborsByIdForDirectionViaLua, this);
        lua.set_function("NodeGetNeighborsByIdForDirectionForType", &Shard::NodeGetNeighborsByIdForDirectionForTypeViaLua, this);
        lua.set_function("NodeGetNeighborsByIdForDirectionForTypeId", &Shard::NodeGetNeighborsByIdForDirectionForTypeIdViaLua, this);
        lua.set_function("NodeGetNeighborsByIdForDirectionForTypes", &Shard::NodeGetNeighborsByIdForDirectionForTypesViaLua, this);

        //Bulk
        lua.set_function("LinksGetRelationshipsIds", &Shard::LinksGetRelationshipsIdsViaLua, this);
        lua.set_function("LinksGetRelationshipsIdsForDirection", &Shard::LinksGetRelationshipsIdsForDirectionViaLua, this);
        lua.set_function("LinksGetRelationshipsIdsForDirectionForType", &Shard::LinksGetRelationshipsIdsForDirectionForTypeViaLua, this);
        lua.set_function("LinksGetRelationshipsIdsForDirectionForTypeId", &Shard::LinksGetRelationshipsIdsForDirectionForTypeIdViaLua, this);
        lua.set_function("LinksGetRelationshipsIdsForDirectionForTypes", &Shard::LinksGetRelationshipsIdsForDirectionForTypesViaLua, this);
        lua.set_function("LinksGetRelationshipsIdsForType", &Shard::LinksGetRelationshipsIdsForTypeViaLua, this);
        lua.set_function("LinksGetRelationshipsIdsForTypeId", &Shard::LinksGetRelationshipsIdsForTypeIdViaLua, this);
        lua.set_function("LinksGetRelationshipsIdsForTypes", &Shard::LinksGetRelationshipsIdsForTypesViaLua, this);

        lua.set_function("LinksGetRelationships", &Shard::LinksGetRelationshipsViaLua, this);
        lua.set_function("LinksGetRelationshipsForDirection", &Shard::LinksGetRelationshipsForDirectionViaLua, this);
        lua.set_function("LinksGetRelationshipsForDirectionForType", &Shard::LinksGetRelationshipsForDirectionForTypeViaLua, this);
        lua.set_function("LinksGetRelationshipsForDirectionForTypeId", &Shard::LinksGetRelationshipsForDirectionForTypeIdViaLua, this);
        lua.set_function("LinksGetRelationshipsForDirectionForTypes", &Shard::LinksGetRelationshipsForDirectionForTypesViaLua, this);
        lua.set_function("LinksGetRelationshipsForType", &Shard::LinksGetRelationshipsForTypeViaLua, this);
        lua.set_function("LinksGetRelationshipsForTypeId", &Shard::LinksGetRelationshipsForTypeIdViaLua, this);
        lua.set_function("LinksGetRelationshipsForTypes", &Shard::LinksGetRelationshipsForTypesViaLua, this);

        lua.set_function("LinksGetNeighbors", &Shard::LinksGetNeighborsViaLua, this);
        lua.set_function("LinksGetNeighborsForDirection", &Shard::LinksGetNeighborsForDirectionViaLua, this);
        lua.set_function("LinksGetNeighborsForDirectionForType", &Shard::LinksGetNeighborsForDirectionForTypeViaLua, this);
        lua.set_function("LinksGetNeighborsForDirectionForTypeId", &Shard::LinksGetNeighborsForDirectionForTypeIdViaLua, this);
        lua.set_function("LinksGetNeighborsForDirectionForTypes", &Shard::LinksGetNeighborsForDirectionForTypesViaLua, this);
        lua.set_function("LinksGetNeighborsForType", &Shard::LinksGetNeighborsForTypeViaLua, this);
        lua.set_function("LinksGetNeighborsForTypeId", &Shard::LinksGetNeighborsForTypeIdViaLua, this);
        lua.set_function("LinksGetNeighborsForTypes", &Shard::LinksGetNeighborsForTypesViaLua, this);

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
                script_result = lua.script(executable, [] (lua_State *, sol::protected_function_result pfr) {
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