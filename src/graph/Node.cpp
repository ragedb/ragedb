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

#include "Node.h"

#include <utility>
#include "Shard.h"

namespace ragedb {

    Node::Node() = default;

    Node::Node(uint64_t id, std::string type, std::string key) : id(id), type(std::move(type)), key(std::move(key)) {}

    Node::Node(uint64_t id, std::string type, std::string key, std::map<std::string, property_type_t>  properties) : id(id), type(std::move(type)), key(std::move(key)), properties(std::move(properties)) {}

    uint64_t Node::getId() const {
        return id;
    }

    uint16_t Node::getTypeId() const {
        return Shard::externalToTypeId(id);
    }

    std::string Node::getKey() const {
        return key;
    }

    std::string Node::getType() const {
        return type;
    }

    std::map<std::string, property_type_t> Node::getProperties() const {
        return properties;
    }

    sol::lua_value Node::getPropertyLua(const std::string& property, sol::this_state ts) const {
      return getProperty(property);
    }

    sol::table Node::getPropertiesLua(sol::this_state ts) const {
        sol::state_view lua = ts;
        sol::table property_map = lua.create_table();
        for (auto [_key, value] : getProperties()) {
          switch (value.index()) {
            case 0:
              property_map[_key] = sol::lua_nil;
              break;
            case 1:
              property_map[_key] = sol::make_object(lua.lua_state(), get<bool>(value));
              break;
            case 2:
              property_map[_key] = sol::make_object(lua.lua_state(), get<int64_t>(value));
              break;
            case 3:
              property_map[_key] = sol::make_object(lua.lua_state(), get<double>(value));
              break;
            case 4:
              property_map[_key] = sol::make_object(lua.lua_state(), get<std::string>(value));
              break;
            case 5:
              property_map[_key] = sol::make_object(lua.lua_state(), sol::as_table(get<std::vector<bool>>(value)));
              break;
            case 6:
              property_map[_key] = sol::make_object(lua.lua_state(), sol::as_table(get<std::vector<int64_t>>(value)));
              break;
            case 7:
              property_map[_key] = sol::make_object(lua.lua_state(), sol::as_table(get<std::vector<double>>(value)));
              break;
            case 8:
              property_map[_key] = sol::make_object(lua.lua_state(), sol::as_table(get<std::vector<std::string>>(value)));
              break;
            }
          }

        return property_map;
    }

    property_type_t Node::getProperty(const std::string& property) const {
        if (auto it = properties.find(property); it != std::end(properties)) {
            return it->second;
        }
        return {};
    }

    std::ostream& operator<<(std::ostream& os, const Node& node) {
        os << "{ \"id\": " << node.id << R"(, "type": ")" << node.type << R"(", "key": )" << "\"" << node.key << "\"" << ", \"properties\": { ";
        bool initial = true;
        for (auto [key, value] : node.properties) {
            if (!initial) {
                os << ", ";
            }
            initial = false;
            os << "\"" << key << "\": ";

            if(value.index() == 1) {
              if (get<bool>(value) ) {
                os << "true" ;
              } else {
                os << "false";
              }
              continue;
            }

            if(value.index() == 2) {
              os << get<int64_t>(value);
              continue;
            }

            if(value.index() == 3) {
              os << get<double>(value);
              continue;
            }

            if(value.index() == 4) {
                os << "\"" << get<std::string>(value) << "\"";
                continue;
            }

            if(value.index() == 5) {
              os << '[';
              bool nested_initial = true;
              for (const auto& item : get<std::vector<bool>>(value)) {
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

            if(value.index() == 6) {
              os << '[';
              bool nested_initial = true;
              for (const auto& item : get<std::vector<int64_t>>(value)) {
                if (!nested_initial) {
                  os << ", ";
                }
                os << item;
                nested_initial = false;
              }
              os << ']';
              continue;
            }

            if(value.index() == 7) {
              os << '[';
              bool nested_initial = true;
              for (const auto& item : get<std::vector<double>>(value)) {
                if (!nested_initial) {
                  os << ", ";
                }
                os << item;
                nested_initial = false;
              }
              os << ']';
              continue;
            }

            if(value.index() == 8) {
                os << '[';
                bool nested_initial = true;
                for (const auto& item : get<std::vector<std::string>>(value)) {
                    if (!nested_initial) {
                        os << ", ";
                    }
                    os << "\"" << item << "\"";
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