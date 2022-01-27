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

#include "Relationship.h"

#include <utility>
#include "Shard.h"

namespace ragedb {

    Relationship::Relationship() = default;

    Relationship::Relationship(uint64_t id,
                               std::string type,
                               uint64_t startingNodeId,
                               uint64_t endingNodeId) : id(id),
                               type(std::move(type)),
                               starting_node_id(startingNodeId),
                               ending_node_id(endingNodeId) {}

    Relationship::Relationship(uint64_t id,
                               std::string type,
                               uint64_t startingNodeId,
                               uint64_t endingNodeId,
                               std::map<std::string, std::any>  properties) : id(id),
                                                        type(std::move(type)),
                                                        starting_node_id(startingNodeId),
                                                        ending_node_id(endingNodeId),
                                                        properties(std::move(properties)){}

    uint64_t Relationship::getId() const {
        return id;
    }

    uint16_t Relationship::getTypeId() const {
        return Shard::externalToTypeId(id);
    }

    std::string Relationship::getType() const {
        return type;
    }

    uint64_t Relationship::getStartingNodeId() const {
        return starting_node_id;
    }

    uint64_t Relationship::getEndingNodeId() const {
        return ending_node_id;
    }

    std::map<std::string, std::any> Relationship::getProperties() const {
        return properties;
    }

    sol::object Relationship::getPropertyLua(const std::string& property, sol::this_state ts) {
      std::any value = getProperty(property);
      sol::state_view lua = ts;
      const auto& value_type = value.type();

      if(value_type == typeid(std::string)) {
        return sol::make_object(lua, std::any_cast<std::string>(value));
      }

      if(value_type == typeid(int64_t)) {
        return sol::make_object(lua, std::any_cast<int64_t>(value));
      }

      if(value_type == typeid(double)) {
        return sol::make_object(lua, std::any_cast<double>(value));
      }

      // Booleans are stored in std::any as a bit reference so we can't use if(value.type() == typeid(bool)) {
      if(value_type == typeid(std::_Bit_reference)) {
        if (std::any_cast<std::_Bit_reference>(value) ) {
          return sol::make_object(lua, true);
        } else {
          return sol::make_object(lua, false);
        }
      }

      if(value_type == typeid(std::vector<std::string>)) {
        return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<std::string>>(value)));
      }

      if(value_type == typeid(std::vector<int64_t>)) {
        return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<int64_t>>(value)));
      }

      if(value_type == typeid(std::vector<double>)) {
        return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<double>>(value)));
      }

      if(value_type == typeid(std::vector<bool>)) {
        return sol::make_object(lua, sol::as_table(std::any_cast<std::vector<bool>>(value)));
      }
      return sol::lua_nil;
    }

    sol::table Relationship::getPropertiesLua(sol::this_state ts) const {
        sol::state_view lua = ts;
        sol::table property_map = lua.create_table();
        for (auto [_key, value] : getProperties()) {
          const auto& value_type = value.type();

          if(value_type == typeid(std::string)) {
            property_map[_key] = sol::make_object(lua, std::any_cast<std::string>(value));
          }

          if(value_type == typeid(int64_t)) {
            property_map[_key] = sol::make_object(lua, std::any_cast<int64_t>(value));
          }

          if(value_type == typeid(double)) {
            property_map[_key] = sol::make_object(lua, std::any_cast<double>(value));
          }

          // Booleans are stored in std::any as a bit reference so we can't use if(value.type() == typeid(bool)) {
          if(value_type == typeid(std::_Bit_reference)) {
            if (std::any_cast<std::_Bit_reference>(value) ) {
              property_map[_key] = sol::make_object(lua, true);
            } else {
              property_map[_key] = sol::make_object(lua, false);
            }
            continue;
          }

          if(value_type == typeid(std::vector<std::string>)) {
            property_map[_key] = sol::make_object(lua, sol::as_table(std::any_cast<std::vector<std::string>>(value)));
          }

          if(value_type == typeid(std::vector<int64_t>)) {
            property_map[_key] = sol::make_object(lua, sol::as_table(std::any_cast<std::vector<int64_t>>(value)));
          }

          if(value_type == typeid(std::vector<double>)) {
            property_map[_key] = sol::make_object(lua, sol::as_table(std::any_cast<std::vector<double>>(value)));
          }

          if(value_type == typeid(std::vector<bool>)) {
            property_map[_key] = sol::make_object(lua, sol::as_table(std::any_cast<std::vector<bool>>(value)));
          }

        }
        return sol::as_table(property_map);
    }

    std::any Relationship::getProperty(const std::string& property) {
        std::map<std::string, std::any>::iterator it;
        it = properties.find(property);

        if (it != std::end(properties)) {
            return it->second;
        }
        return std::any();
    }

    std::ostream &operator<<(std::ostream &os, const Relationship &relationship) {
        os << "{ \"id\": " << relationship.id << R"(, "type": ")" << relationship.type << R"(", "starting_node_id": )" << relationship.starting_node_id << ", \"ending_node_id\": " << relationship.ending_node_id << ", \"properties\": { ";
        bool initial = true;
        for (auto [key, value] : relationship.properties) {
            if (!initial) {
                os << ", ";
            }
            initial = false;
            os << "\"" << key << "\": ";

            if(value.type() == typeid(std::string)) {
                os << "\"" << std::any_cast<std::string>(value) << "\"";
                continue;
            }
            if(value.type() == typeid(int64_t)) {
                os << std::any_cast<int64_t>(value);
                continue;
            }
            if(value.type() == typeid(double)) {
                os << std::any_cast<double>(value);
                continue;
            }

            // Booleans are stored in std::any as a bit reference so we can't use if(value.type() == typeid(bool)) {
            if(value.type() == typeid(std::_Bit_reference)) {
                if (std::any_cast<std::_Bit_reference>(value) ) {
                    os << "true" ;
                } else {
                    os << "false";
                }
                continue;
            }

            if(value.type() == typeid(std::vector<std::string>)) {
                os << '[';
                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<std::string>>(value)) {
                    if (!nested_initial) {
                        os << ", ";
                    }
                    os << "\"" << item << "\"";
                    nested_initial = false;
                }
                os << ']';
                continue;
            }

            if(value.type() == typeid(std::vector<int64_t>)) {
                os << '[';
                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<int64_t>>(value)) {
                    if (!nested_initial) {
                        os << ", ";
                    }
                    os << item;
                    nested_initial = false;
                }
                os << ']';
                continue;
            }

            if(value.type() == typeid(std::vector<double>)) {
                os << '[';
                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<double>>(value)) {
                    if (!nested_initial) {
                        os << ", ";
                    }
                    os << item;
                    nested_initial = false;
                }
                os << ']';
                continue;
            }

            if(value.type() == typeid(std::vector<bool>)) {
                os << '[';
                bool nested_initial = true;
                for (const auto& item : std::any_cast<std::vector<bool>>(value)) {
                    if (!nested_initial) {
                        os << ", ";
                    }
                    if (item) {
                        os << "true";
                    } else {
                        os << "false";
                    }
                    nested_initial = false;
                }
                os << ']';
                continue;
            }

        }
        os << " } }";

        return os;
    }
    
}