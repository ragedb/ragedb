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

    static const unsigned int TYPE_BITS = 16U;
    static const unsigned int TYPE_MASK = 0x0000000003FFFFFFU;
    static const unsigned int SIXTY_FOUR = 64U;

    // 64 bits:  Up to 10 bits for core id (1024), 16 bits for the type (65536) 38 bits for the id (274877906944)

    uint64_t Shard::internalToExternal(uint16_t type_id, uint64_t internal_id) {
        return (((internal_id << TYPE_BITS) + type_id) << SHARD_BITS) + seastar::this_shard_id();
    }

    uint64_t Shard::externalToInternal(uint64_t id) {
        return (id >> (TYPE_BITS + SHARD_BITS));
    }

    std::vector<uint64_t> Shard::externalToInternal(const std::vector<uint64_t> &ids) {
        std::vector<uint64_t> internal_ids;
        internal_ids.reserve(ids.size());
        for (const auto& id : ids) {
            internal_ids.emplace_back((id >> (TYPE_BITS + SHARD_BITS)));
        }
        return internal_ids;
    }

    uint16_t Shard::externalToTypeId(uint64_t id) {
        return (uint16_t)((id & TYPE_MASK ) >> SHARD_BITS);
    }

    uint16_t Shard::CalculateShardId(uint64_t id) {
        if(id < SHARD_MASK) {
            return 0;
        }
        return (uint16_t)(id & SHARD_MASK);
    }

    uint16_t Shard::CalculateShardId(const std::string &type, const std::string &key) const {
        // We need to find where the node goes, so we use the type and key to create a 64 bit number
        uint64_t x64 = std::hash<std::string>()((type + '-' + key));

        // Then we bucket it into a shard depending on the number of cpus we have
        return (uint16_t)(((__uint128_t)x64 * (__uint128_t)cpus) >> SIXTY_FOUR);
    }

    uint16_t Shard::CalculateShardId(const std::string &type, const std::string &property, const property_type_t &value) const {
        // We need to find where the find goes, so we use the type, property and value to create a 64 bit number

        std::ostringstream ss;
        ss << type;
        ss << '-';
        ss << property;
        ss << '-';

        if (value.index() == 1) {
            ss << std::boolalpha << get<bool>(value);
        }
        if (value.index() == 2) {
            ss << std::to_string(get<int64_t>(value));
        }
        if (value.index() == 3) {
            ss << std::to_string(get<double>(value));
        }
        if (value.index() == 4) {
            ss << get<std::string>(value);
        }

        uint64_t x64 = std::hash<std::string>()(ss.str());

        // Then we bucket it into a shard depending on the number of cpus we have
        return (uint16_t)(((__uint128_t)x64 * (__uint128_t)cpus) >> SIXTY_FOUR);
    }

    bool Shard::ValidNodeId(uint64_t id) const {
        // Node must be greater than zero,
        // less than maximum node id,
        // belong to this shard
        // and not deleted
        return id > 0 && CalculateShardId(id) == seastar::this_shard_id()
               && node_types.ValidNodeId(externalToTypeId(id), externalToInternal(id));
    }

    bool Shard::ValidNodeIds(const std::vector<uint64_t> &ids) const {
        for(uint64_t id : ids) {
            if(!ValidNodeId(id)) {
                return false;
            }
        }
        return true;
    }

    bool Shard::ValidNodeId(uint64_t id, uint16_t type_id, uint64_t internal_id) const {
      // Node must be greater than zero,
      // less than maximum node id,
      // belong to this shard
      // and not deleted
      return id > 0 && CalculateShardId(id) == seastar::this_shard_id()
             && node_types.ValidNodeId(type_id, internal_id);
    }

    bool Shard::ValidRelationshipIds(const std::vector<uint64_t> &ids) const {
        for(const auto& id : ids) {
            if(!ValidRelationshipId(id)) {
                return false;
            }
        }
        return true;
    }

    bool Shard::ValidRelationshipId(uint64_t id) const {
        // Relationship must be greater than zero,
        // less than maximum relationship id,
        // belong to this shard
        // and not deleted
        return id > 0 && CalculateShardId(id) == seastar::this_shard_id()
        && relationship_types.ValidRelationshipId(externalToTypeId(id), externalToInternal(id));
    }

    bool Shard::ValidRelationshipId(uint64_t id, uint16_t type_id, uint64_t internal_id) const {
      // Relationship must be greater than zero,
      // less than maximum relationship id,
      // belong to this shard
      // and not deleted
      return id > 0 && CalculateShardId(id) == seastar::this_shard_id()
             && relationship_types.ValidRelationshipId(type_id, internal_id);
    }

    bool Shard::ValidLinks(const std::vector<Link> &links) const {
        for(const auto& link : links) {
            if(!ValidNodeId(link.node_id)) {
                return false;
            }
            if(!ValidRelationshipId(link.rel_id)) {
                return false;
            }
        }
        return true;
    }

    bool Shard::ValidLinksNodeIds(const std::vector<Link> &links) const {
        for(const auto& link : links) {
            if(!ValidNodeId(link.node_id)) {
                return false;
            }
         }
        return true;
    }

    bool Shard::ValidLinksRelationshipIds(const std::vector<Link> &links) const {
        for(const auto& link : links) {
            if(!ValidRelationshipId(link.rel_id)) {
                return false;
            }
        }
        return true;
    }
}