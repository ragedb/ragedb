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

#ifndef RAGEDB_RELATIONSHIP_H
#define RAGEDB_RELATIONSHIP_H

#include <cstdint>
#include <string>
#include <map>
#include <sol/sol.hpp>
#include "PropertyType.h"

namespace ragedb {

    class Relationship {
    private:
        uint64_t id{};
        std::string type{};
        uint64_t starting_node_id{};
        uint64_t ending_node_id{};
        std::map<std::string, property_type_t> properties{};

    public:
        Relationship();

        Relationship(uint64_t id, std::string type, uint64_t startingNodeId, uint64_t endingNodeId);
        Relationship(uint64_t id, std::string type, uint64_t startingNodeId, uint64_t endingNodeId, std::map<std::string, property_type_t> properties);

        [[nodiscard]] uint64_t getId() const;

        [[nodiscard]] uint16_t getTypeId() const;

        [[nodiscard]] std::string getType() const;

        [[nodiscard]] uint64_t getStartingNodeId() const;

        [[nodiscard]] uint64_t getEndingNodeId() const;

        [[nodiscard]] std::map<std::string, property_type_t> getProperties() const;

        [[nodiscard]] sol::table getPropertiesLua(sol::this_state ts) const;

        [[nodiscard]] property_type_t getProperty(const std::string& property) const;

        [[nodiscard]] sol::object getPropertyLua(const std::string& property, sol::this_state ts) const;

        friend std::ostream& operator<<(std::ostream& os, const Relationship& relationship);

    };
}

#endif //RAGEDB_RELATIONSHIP_H
