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

    namespace {
        void relationship_index_add_helper(Shard& shard, uint16_t type_id, uint64_t internal_id, const std::string& property, const property_type_t& value) {
            if (value.index() >= 1 && value.index() <= 4) {
                uint64_t external_id = Shard::internalToExternal(type_id, internal_id);
                std::string type_name = shard.RelationshipGetType(external_id);
                uint16_t target_shard = shard.CalculateShardId(type_name, property, value);
                if (target_shard == seastar::this_shard_id()) {
                    shard.RelationshipIndexInsert(type_id, property, value, external_id);
                } else {
                    (void)shard.container().invoke_on(target_shard, [type_id, property, value, external_id](Shard &target) {
                        target.RelationshipIndexInsert(type_id, property, value, external_id);
                    });
                }
            }
        }

        void relationship_index_remove_helper(Shard& shard, uint16_t type_id, uint64_t internal_id, const std::string& property, const property_type_t& value) {
            if (value.index() >= 1 && value.index() <= 4) {
                uint64_t external_id = Shard::internalToExternal(type_id, internal_id);
                std::string type_name = shard.RelationshipGetType(external_id);
                uint16_t target_shard = shard.CalculateShardId(type_name, property, value);
                if (target_shard == seastar::this_shard_id()) {
                    shard.RelationshipIndexRemove(type_id, property, value, external_id);
                } else {
                    (void)shard.container().invoke_on(target_shard, [type_id, property, value, external_id](Shard &target) {
                        target.RelationshipIndexRemove(type_id, property, value, external_id);
                    });
                }
            }
        }
    }

    void Shard::insert_sorted(uint64_t id1, uint64_t external_id, std::vector<Link> &links) const {
      auto link = Link(id1, external_id);
      links.insert(std::ranges::upper_bound(links, link), link);
    }

    uint64_t Shard::RelationshipAddEmptySameShard(uint16_t rel_type_id, uint64_t id1, uint64_t id2) {
        uint64_t internal_id1 = externalToInternal(id1);
        uint64_t internal_id2 = externalToInternal(id2);
        uint16_t id1_type_id = externalToTypeId(id1);
        uint16_t id2_type_id = externalToTypeId(id2);
        uint64_t external_id = 0;
        if (ValidNodeId(id1, id1_type_id, internal_id1) && ValidNodeId(id2, id2_type_id, internal_id2)) {
            uint64_t internal_id = relationship_types.getCount(rel_type_id);
            if(relationship_types.hasDeleted(rel_type_id)) {
                // If we have deleted relationships, fill in the space by reusing the new relationship
                internal_id = relationship_types.getDeletedIdsMinimum(rel_type_id);
                relationship_types.setStartingNodeId(rel_type_id, internal_id, id1);
                relationship_types.setEndingNodeId(rel_type_id, internal_id, id2);
            } else {
                relationship_types.getStartingNodeIds(rel_type_id).emplace_back(id1);
                relationship_types.getEndingNodeIds(rel_type_id).emplace_back(id2);
            }

            relationship_types.addId(rel_type_id, internal_id);
            external_id = internalToExternal(rel_type_id, internal_id);

            // Add the relationship to the outgoing node
            auto group = std::ranges::find_if(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1),
              [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
            // See if the relationship type is already there
            if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
               insert_sorted(id2, external_id, group->links);
            } else {
                // otherwise, create a new type with the links
                node_types.getOutgoingRelationships(id1_type_id).at(internal_id1).emplace_back(rel_type_id, std::vector<Link>({Link(id2, external_id)}));
            }

            // Add the relationship to the incoming node
            group = std::ranges::find_if(node_types.getIncomingRelationships(id2_type_id).at(internal_id2),
              [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
            // See if the relationship type is already there
            if (group != std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2))) {
              insert_sorted(id1, external_id, group->links);
            } else {
                // otherwise, create a new type with the links
                node_types.getIncomingRelationships(id2_type_id).at(internal_id2).emplace_back(rel_type_id, std::vector<Link>({Link(id1, external_id)}));
            }

            return external_id;
        }
        // Invalid node links
        return 0;
    }

    uint64_t Shard::RelationshipAddSameShard(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties) {
        uint64_t internal_id1 = externalToInternal(id1);
        uint64_t internal_id2 = externalToInternal(id2);
        uint16_t id1_type_id = externalToTypeId(id1);
        uint16_t id2_type_id = externalToTypeId(id2);
        uint64_t external_id = 0;

        if (ValidNodeId(id1, id1_type_id, internal_id1) && ValidNodeId(id2, id2_type_id, internal_id2)) {
            uint64_t internal_id = relationship_types.getCount(rel_type_id);
            if(relationship_types.hasDeleted(rel_type_id)) {
                // If we have deleted relationships, fill in the space by reusing the new relationship
                internal_id = relationship_types.getDeletedIdsMinimum(rel_type_id);
                relationship_types.setStartingNodeId(rel_type_id, internal_id, id1);
                relationship_types.setEndingNodeId(rel_type_id, internal_id, id2);
            } else {
                relationship_types.getStartingNodeIds(rel_type_id).emplace_back(id1);
                relationship_types.getEndingNodeIds(rel_type_id).emplace_back(id2);
            }

            relationship_types.addId(rel_type_id, internal_id);
            relationship_types.setPropertiesFromJSON(rel_type_id, internal_id, properties);
            external_id = internalToExternal(rel_type_id, internal_id);

            auto type_indexes_it = relationship_indexes.find(rel_type_id);
            if (type_indexes_it != relationship_indexes.end()) {
                for (const auto& [property, index] : type_indexes_it->second) {
                    property_type_t val = relationship_types.getRelationshipProperty(rel_type_id, internal_id, property);
                    relationship_index_add_helper(*this, rel_type_id, internal_id, property, val);
                }
            }

            // Add the relationship to the outgoing node
            auto group = std::ranges::find_if(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1),
              [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
            // See if the relationship type is already there
            if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
              insert_sorted(id2, external_id, group->links);
            } else {
                // otherwise create a new type with the links
                node_types.getOutgoingRelationships(id1_type_id).at(internal_id1).emplace_back(rel_type_id, std::vector<Link>({Link(id2, external_id)}));
            }

            // Add the relationship to the incoming node
            group = std::ranges::find_if(node_types.getIncomingRelationships(id2_type_id).at(internal_id2),
              [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
            // See if the relationship type is already there
            if (group != std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2))) {
              insert_sorted(id1, external_id, group->links);
            } else {
                // otherwise create a new type with the links
                node_types.getIncomingRelationships(id2_type_id).at(internal_id2).emplace_back(rel_type_id, std::vector<Link>({Link(id1, external_id)}));
            }

            return external_id;
        }
        // Invalid node links
        return 0;
    }

    uint64_t Shard::RelationshipAddEmptySameShard(uint16_t rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2) {
        uint64_t id1 = NodeGetID(type1, key1);
        uint64_t id2 = NodeGetID(type2, key2);

        return RelationshipAddEmptySameShard(rel_type, id1, id2);
    }

    uint64_t Shard::RelationshipAddSameShard(uint16_t rel_type, const std::string &type1, const std::string &key1, const std::string &type2, const std::string &key2, const std::string& properties) {
        uint64_t id1 = NodeGetID(type1, key1);
        uint64_t id2 = NodeGetID(type2, key2);

        return RelationshipAddSameShard(rel_type, id1, id2, properties);
    }

    uint64_t Shard::RelationshipAddEmptyToOutgoing(uint16_t rel_type_id, uint64_t id1, uint64_t id2) {
        uint64_t internal_id1 = externalToInternal(id1);
        uint16_t id1_type_id = externalToTypeId(id1);
        uint64_t external_id = 0;

        uint64_t internal_id = relationship_types.getCount(rel_type_id);
        if(relationship_types.hasDeleted(rel_type_id)) {
            // If we have deleted relationships, fill in the space by reusing the new relationship
            internal_id = relationship_types.getDeletedIdsMinimum(rel_type_id);
            relationship_types.setStartingNodeId(rel_type_id, internal_id, id1);
            relationship_types.setEndingNodeId(rel_type_id, internal_id, id2);
        } else {
            relationship_types.getStartingNodeIds(rel_type_id).emplace_back(id1);
            relationship_types.getEndingNodeIds(rel_type_id).emplace_back(id2);
        }

        relationship_types.addId(rel_type_id, internal_id);
        external_id = internalToExternal(rel_type_id, internal_id);

        // Add the relationship to the outgoing node
        auto group = std::ranges::find_if(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1),
          [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
        // See if the relationship type is already there
        if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
          insert_sorted(id2, external_id, group->links);
        } else {
            // otherwise create a new type with the links
            node_types.getOutgoingRelationships(id1_type_id).at(internal_id1).emplace_back(rel_type_id, std::vector<Link>({Link(id2, external_id)}));
        }

        return external_id;
    }

    uint64_t Shard::RelationshipAddToOutgoing(uint16_t rel_type_id, uint64_t id1, uint64_t id2, const std::string& properties) {
        uint64_t internal_id1 = externalToInternal(id1);
        uint16_t id1_type_id = externalToTypeId(id1);
         uint64_t external_id = 0;

        uint64_t internal_id = relationship_types.getCount(rel_type_id);
        if(relationship_types.hasDeleted(rel_type_id)) {
            // If we have deleted relationships, fill in the space by reusing the new relationship
            internal_id = relationship_types.getDeletedIdsMinimum(rel_type_id);
            relationship_types.setStartingNodeId(rel_type_id, internal_id, id1);
            relationship_types.setEndingNodeId(rel_type_id, internal_id, id2);
        } else {
            relationship_types.getStartingNodeIds(rel_type_id).emplace_back(id1);
            relationship_types.getEndingNodeIds(rel_type_id).emplace_back(id2);
        }

        relationship_types.addId(rel_type_id, internal_id);
        relationship_types.setPropertiesFromJSON(rel_type_id, internal_id, properties);
        external_id = internalToExternal(rel_type_id, internal_id);

        auto type_indexes_it = relationship_indexes.find(rel_type_id);
        if (type_indexes_it != relationship_indexes.end()) {
            for (const auto& [property, index] : type_indexes_it->second) {
                property_type_t val = relationship_types.getRelationshipProperty(rel_type_id, internal_id, property);
                relationship_index_add_helper(*this, rel_type_id, internal_id, property, val);
            }
        }

        // Add the relationship to the outgoing node
        auto group = std::ranges::find_if(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1),
          [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
        // See if the relationship type is already there
        if (group != std::end(node_types.getOutgoingRelationships(id1_type_id).at(internal_id1))) {
          insert_sorted(id2, external_id, group->links);
        } else {
            // otherwise create a new type with the links
            node_types.getOutgoingRelationships(id1_type_id).at(internal_id1).emplace_back(rel_type_id, std::vector<Link>({Link(id2, external_id)}));
        }

        return external_id;
    }

    uint64_t Shard::RelationshipAddToIncoming(uint16_t rel_type_id, uint64_t external_id, uint64_t id1, uint64_t id2) {
        uint64_t internal_id2 = externalToInternal(id2);
        uint16_t id2_type_id = externalToTypeId(id2);
        // Add the relationship to the incoming node
        auto group = std::ranges::find_if(node_types.getIncomingRelationships(id2_type_id).at(internal_id2),
          [rel_type_id] (const Group& g) { return g.rel_type_id == rel_type_id; } );
        // See if the relationship type is already there
        if (group != std::end(node_types.getIncomingRelationships(id2_type_id).at(internal_id2))) {
            insert_sorted(id1, external_id, group->links);
        } else {
            // otherwise create a new type with the links
            node_types.getIncomingRelationships(id2_type_id).at(internal_id2).emplace_back(Group(rel_type_id, std::vector<Link>({Link(id1, external_id)})));
        }

        return external_id;
    }

    Relationship Shard::RelationshipGet(uint64_t rel_id) {
        if (ValidRelationshipId(rel_id)) {
            return relationship_types.getRelationship(rel_id);
        }       // Invalid Relationship
        return {};
    }

    std::string Shard::RelationshipGetType(uint64_t id) const {
        if (ValidRelationshipId(id)) {
            return relationship_types.getType(externalToTypeId(id));
        }
        // Invalid Relationship Id
        return relationship_types.getType(0);
    }

    uint16_t Shard::RelationshipGetTypeId(uint64_t id) const {
        if (ValidRelationshipId(id)) {
            return externalToTypeId(id);
        }
        // Invalid Relationship Id
        return 0;
    }

    uint64_t Shard::RelationshipGetStartingNodeId(uint64_t id) {
        if (ValidRelationshipId(id)) {
            return relationship_types.getStartingNodeId(externalToTypeId(id), externalToInternal(id));
        }
        // Invalid Relationship Id
        return 0;
    }

    uint64_t Shard::RelationshipGetEndingNodeId(uint64_t id) {
        if (ValidRelationshipId(id)) {
            return relationship_types.getEndingNodeId(externalToTypeId(id), externalToInternal(id));
        }
        // Invalid Relationship Id
        return 0;
    }

    property_type_t Shard::RelationshipGetProperty(uint64_t id, const std::string& property) {
        if (ValidRelationshipId(id)) {
            return relationship_types.getRelationshipProperty(id, property);
        }
        return {};
    }

    bool Shard::RelationshipSetProperty(uint64_t id, const std::string& property, const property_type_t& value) {
        if (ValidRelationshipId(id)) {
            uint16_t type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            bool indexed = false;
            auto type_indexes_it = relationship_indexes.find(type_id);
            if (type_indexes_it != relationship_indexes.end()) {
                indexed = (type_indexes_it->second.find(property) != type_indexes_it->second.end());
            }
            property_type_t old_value;
            if (indexed) {
                old_value = relationship_types.getRelationshipProperty(id, property);
            }
            bool success = relationship_types.setRelationshipProperty(id, property, value);
            if (success && indexed) {
                if (old_value != value) {
                    relationship_index_remove_helper(*this, type_id, internal_id, property, old_value);
                    relationship_index_add_helper(*this, type_id, internal_id, property, value);
                }
            }
            return success;
        }
        return false;
    }

    bool Shard::RelationshipSetPropertyFromJson(uint64_t id, const std::string& property, const std::string& value) {
        if (ValidRelationshipId(id)) {
            uint16_t type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            bool indexed = false;
            auto type_indexes_it = relationship_indexes.find(type_id);
            if (type_indexes_it != relationship_indexes.end()) {
                indexed = (type_indexes_it->second.find(property) != type_indexes_it->second.end());
            }
            property_type_t old_value;
            if (indexed) {
                old_value = relationship_types.getRelationshipProperty(id, property);
            }
            bool success = relationship_types.setRelationshipPropertyFromJson(id, property, value);
            if (success && indexed) {
                property_type_t new_value = relationship_types.getRelationshipProperty(id, property);
                if (old_value != new_value) {
                    relationship_index_remove_helper(*this, type_id, internal_id, property, old_value);
                    relationship_index_add_helper(*this, type_id, internal_id, property, new_value);
                }
            }
            return success;
        }
        return false;
    }

    bool Shard::RelationshipDeleteProperty(uint64_t id, const std::string& property) {
        if (ValidRelationshipId(id)) {
            uint16_t type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            bool indexed = false;
            auto type_indexes_it = relationship_indexes.find(type_id);
            if (type_indexes_it != relationship_indexes.end()) {
                indexed = (type_indexes_it->second.find(property) != type_indexes_it->second.end());
            }
            property_type_t old_value;
            if (indexed) {
                old_value = relationship_types.getRelationshipProperty(id, property);
            }
            bool success = relationship_types.deleteRelationshipProperty(id, property);
            if (success && indexed) {
                relationship_index_remove_helper(*this, type_id, internal_id, property, old_value);
            }
            return success;
        }
        return false;
    }

    std::map<std::string, property_type_t> Shard::RelationshipGetProperties(uint64_t id) {
        if (ValidRelationshipId(id)) {
            return relationship_types.getRelationshipProperties(externalToTypeId(id), externalToInternal(id));
        }
        return {};
    }

    bool Shard::RelationshipSetPropertiesFromJson(uint64_t id, const std::string& value) {
        if (ValidRelationshipId(id)) {
            uint16_t type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            auto type_indexes_it = relationship_indexes.find(type_id);
            std::map<std::string, property_type_t> old_values;
            if (type_indexes_it != relationship_indexes.end()) {
                for (const auto& [property, index] : type_indexes_it->second) {
                    old_values[property] = relationship_types.getRelationshipProperty(id, property);
                }
            }
            bool success = relationship_types.setPropertiesFromJSON(type_id, internal_id, value);
            if (success && type_indexes_it != relationship_indexes.end()) {
                for (const auto& [property, index] : type_indexes_it->second) {
                    property_type_t old_val = old_values[property];
                    property_type_t new_val = relationship_types.getRelationshipProperty(id, property);
                    if (old_val != new_val) {
                        relationship_index_remove_helper(*this, type_id, internal_id, property, old_val);
                        relationship_index_add_helper(*this, type_id, internal_id, property, new_val);
                    }
                }
            }
            return success;
        }
        return false;
    }

    bool Shard::RelationshipResetPropertiesFromJson(uint64_t id, const std::string& value) {
        if (ValidRelationshipId(id)) {
            uint16_t type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            auto type_indexes_it = relationship_indexes.find(type_id);
            std::map<std::string, property_type_t> old_values;
            if (type_indexes_it != relationship_indexes.end()) {
                for (const auto& [property, index] : type_indexes_it->second) {
                    old_values[property] = relationship_types.getRelationshipProperty(id, property);
                }
            }
            relationship_types.deleteProperties(type_id, internal_id);
            if (type_indexes_it != relationship_indexes.end()) {
                for (const auto& [property, index] : type_indexes_it->second) {
                    property_type_t old_val = old_values[property];
                    relationship_index_remove_helper(*this, type_id, internal_id, property, old_val);
                }
            }
            bool success = relationship_types.setPropertiesFromJSON(type_id, internal_id, value);
            if (success && type_indexes_it != relationship_indexes.end()) {
                for (const auto& [property, index] : type_indexes_it->second) {
                    property_type_t new_val = relationship_types.getRelationshipProperty(id, property);
                    relationship_index_add_helper(*this, type_id, internal_id, property, new_val);
                }
            }
            return success;
        }
        return false;
    }

    bool Shard::RelationshipDeleteProperties(uint64_t id) {
        if (ValidRelationshipId(id)) {
            uint16_t type_id = externalToTypeId(id);
            uint64_t internal_id = externalToInternal(id);
            auto type_indexes_it = relationship_indexes.find(type_id);
            std::map<std::string, property_type_t> old_values;
            if (type_indexes_it != relationship_indexes.end()) {
                for (const auto& [property, index] : type_indexes_it->second) {
                    old_values[property] = relationship_types.getRelationshipProperty(id, property);
                }
            }
            bool success = relationship_types.deleteProperties(type_id, internal_id);
            if (success && type_indexes_it != relationship_indexes.end()) {
                for (const auto& [property, index] : type_indexes_it->second) {
                    property_type_t old_val = old_values[property];
                    relationship_index_remove_helper(*this, type_id, internal_id, property, old_val);
                }
            }
            return success;
        }
        return false;
    }

}