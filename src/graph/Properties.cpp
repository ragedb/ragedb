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

#include "Properties.h"

namespace ragedb {

    Properties::Properties() : allowed_types({"", "boolean", "integer", "double", "string", "boolean_list", "integer_list", "double_list", "string_list", "date", "date_list"})  {
        clear();
    }

    void Properties::clear() {
        types.clear();
        types.emplace("", 0);
        deleted.clear();
        deleted.emplace("", roaring::Roaring64Map());
        booleans.clear();
        booleans.emplace("", std::vector<bool>());
        integers.clear();
        integers.emplace("", std::vector<int64_t>());
        doubles.clear();
        doubles.emplace("", std::vector<double>());
        strings.clear();
        strings.emplace("", std::vector<std::string>());
        booleans_list.clear();
        booleans_list.emplace("", std::vector<std::vector<bool>>());
        integers_list.clear();
        integers_list.emplace("", std::vector<std::vector<int64_t>>());
        doubles_list.clear();
        doubles_list.emplace("", std::vector<std::vector<double>>());
        strings_list.clear();
        strings_list.emplace("", std::vector<std::vector<std::string>>());
    }


    std::string Properties::getPropertyKey(uint8_t id) const {
      for (const auto& [type, type_id]: types) {
        if (id == type_id) {
          return type;
        }
      }
      return "";
    }

    std::map<std::string, std::string> Properties::getPropertyTypes() {
        std::map<std::string, std::string> map;
        for (const auto& [type, type_id]: types) {
            map.insert({type, allowed_types[type_id]});
        }
        map.erase("");
        return map;
    }

    uint8_t Properties::getPropertyTypeId(const std::string& key) const {
        if (types.find(key) != types.end()) {
            return types.at(key);
        }
        return 0;
    }

    std::string Properties::getPropertyType(const std::string &key) const {
      if (types.find(key) != types.end()) {
        return allowed_types[types.at(key)];
      }
      return "";
    }

    uint8_t Properties::setPropertyTypeId(const std::string &key, uint8_t property_type_id) {
        if (types.find(key) != types.end()) {
            if (types[key] == property_type_id) {
                return property_type_id;
            }
            // Type already exists and it is not what we asked for
            return 0;
        }

        types.emplace(key, property_type_id);
        deleted.emplace(key, roaring::Roaring64Map());

        addPropertyTypeVectors(key, property_type_id);

        return property_type_id;
    }

    void Properties::addPropertyTypeVectors(const std::string &key, uint8_t type_id) {
        switch (type_id) {
            case boolean_type: {
                booleans.emplace(key, std::vector<bool>());
                break;
            }
            case integer_type: {
                integers.emplace(key, std::vector<int64_t>());
                break;
            }
            case double_type: {
                doubles.emplace(key, std::vector<double>());
                break;
            }
            case string_type: {
                strings.emplace(key, std::vector<std::string>());
                break;
            }
            case date_type: {
              doubles.emplace(key, std::vector<double>());
              break;
            }
            case boolean_list_type: {
                booleans_list.emplace(key, std::vector<std::vector<bool>>());
                break;
            }
            case integer_list_type: {
                integers_list.emplace(key, std::vector<std::vector<int64_t>>());
                break;
            }
            case double_list_type: {
                doubles_list.emplace(key, std::vector<std::vector<double>>());
                break;
            }
            case string_list_type: {
                strings_list.emplace(key, std::vector<std::vector<std::string>>());
                break;
            }
            case date_list_type: {
              doubles_list.emplace(key, std::vector<std::vector<double>>());
              break;
            }
            default: {
                return;
            }
        }
    }

    void Properties::removePropertyTypeVectors(const std::string &key, uint8_t type_id) {
        switch (type_id) {
            case boolean_type: {
                booleans.erase(key);
                break;
            }
            case integer_type: {
                integers.erase(key);
                break;
            }
            case double_type: {
                doubles.erase(key);
                break;
            }
            case date_type: {
              doubles.erase(key);
              break;
            }
            case string_type: {
                strings.erase(key);
                break;
            }
            case boolean_list_type: {
                booleans_list.erase(key);
                break;
            }
            case integer_list_type: {
                integers_list.erase(key);
                break;
            }
            case double_list_type: {
                doubles_list.erase(key);
                break;
            }
            case date_list_type: {
              doubles_list.erase(key);
              break;
            }
            case string_list_type: {
                strings_list.erase(key);
                break;
            }
            default: {
                return;
            }
        }
    }

    uint8_t Properties::setPropertyType(const std::string &key, const std::string &type) {
        // What type do we want?
        uint8_t type_id = 0;
        if (auto type_search = type_map.find(type); type_search != type_map.end()) {
            type_id = type_map[type];
        }

        if (type_id == 0) {
            // We have a bad type
            return 0;
        }

        // See if we already have it set
        if (types.find(key) != types.end()) {
            if (types[key] == type_id) {
                return type_id;
            }
            // Type already exists and it is not what we asked for
            return 0;
        }

        types.emplace(key, type_id);
        deleted.emplace(key, roaring::Roaring64Map());

        addPropertyTypeVectors(key, type_id);

        return type_id;
    }

    bool Properties::removePropertyType(const std::string& key) {
        if (types.find(key) != types.end()) {
            removePropertyTypeVectors(key, types[key]);
            types.erase(key);
            deleted.erase(key);
            return true;
        }
        return false;
    }

    bool Properties::validType(const std::string &key) {
      if (types.empty()) {
        return false;
      }

      if (auto type_check = types.find(key); type_check == types.end()) {
        return false;
      }
      return true;
    }

    bool Properties::setBooleanProperty(const std::string &key, uint64_t index, bool value) {
        if (!validType(key)) {
            return false;
        }

        if (types[key] != boolean_type) {
            return false;
        }

        while (booleans[key].size() <= index) {
            booleans[key].emplace_back();
        }
        booleans[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setIntegerProperty(const std::string &key, uint64_t index, int64_t value) {
        if (!validType(key)) {
          return false;
        }

        if (types[key] != integer_type) {
            return false;
        }

        while (integers[key].size() <= index) {
            integers[key].emplace_back();
        }
        integers[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setDoubleProperty(const std::string &key, uint64_t index, double value) {
        if (!validType(key)) {
          return false;
        }

        if (types[key] != double_type) {
            return false;
        }

        while (doubles[key].size() <= index) {
            doubles[key].emplace_back();
        }
        doubles[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setDateProperty(const std::string &key, uint64_t index, double value) {
      if (!validType(key)) {
        return false;
      }

      if (types[key] != date_type) {
        return false;
      }

      while (doubles[key].size() <= index) {
        doubles[key].emplace_back();
      }
      doubles[key][index] = value;
      deleted[key].remove(index);

      return true;
    }

    bool Properties::setStringProperty(const std::string &key, uint64_t index, const std::string &value) {
        if (!validType(key)) {
          return false;
        }

        if (types[key] != string_type) {
            return false;
        }

        while (strings[key].size() <= index) {
            strings[key].emplace_back();
        }
        strings[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setListOfBooleanProperty(const std::string &key, uint64_t index, const std::vector<bool> &value) {
        if (!validType(key)) {
          return false;
        }

        if (types[key] != boolean_list_type) {
            return false;
        }

        while (booleans_list[key].size() <= index) {
            booleans_list[key].emplace_back();
        }
        booleans_list[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool
    Properties::setListOfIntegerProperty(const std::string &key, uint64_t index, const std::vector<int64_t> &value) {
        if (!validType(key)) {
          return false;
        }

        if (types[key] != integer_list_type) {
            return false;
        }

        while (integers_list[key].size() <= index) {
            integers_list[key].emplace_back();
        }
        integers_list[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setListOfDoubleProperty(const std::string &key, uint64_t index, const std::vector<double> &value) {
        if (!validType(key)) {
          return false;
        }

        if (types[key] != double_list_type) {
            return false;
        }

        while (doubles_list[key].size() <= index) {
            doubles_list[key].emplace_back();
        }
        doubles_list[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setListOfDateProperty(const std::string &key, uint64_t index, const std::vector<double> &value) {
      if (!validType(key)) {
        return false;
      }

      if (types[key] != date_list_type) {
        return false;
      }

      while (doubles_list[key].size() <= index) {
        doubles_list[key].emplace_back();
      }
      doubles_list[key][index] = value;
      deleted[key].remove(index);

      return true;
    }

    bool
    Properties::setListOfStringProperty(const std::string &key, uint64_t index, const std::vector<std::string> &value) {
        if (!validType(key)) {
            return false;
        }

        if (types[key] != string_list_type) {
            return false;
        }

        while (strings_list[key].size() <= index) {
            strings_list[key].emplace_back();
        }
        strings_list[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    std::vector<std::pair<uint64_t, property_type_t>> Properties::getProperty(const std::vector<uint64_t> &external_ids, const std::vector<uint64_t> &internal_ids, const std::string &key, const std::function<bool(property_type_t)> filter) const {
        std::vector<std::pair<uint64_t, property_type_t>> properties;
        // Check if empty
        if (external_ids.empty()) {
            return properties;
        }
        auto type_id = getPropertyTypeId(key);
        switch (type_id) {
        case boolean_type: {
            if (booleans.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = booleans.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        case integer_type: {
            if (integers.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = integers.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        case double_type: {
            if (doubles.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = doubles.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        case date_type: {
            if (doubles.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = doubles.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        case string_type: {
            if (strings.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = strings.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        case boolean_list_type: {
            if (booleans_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = booleans_list.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        case integer_list_type: {
            if (integers_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = integers_list.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        case double_list_type: {
            if (doubles_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = doubles_list.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        case date_list_type: {
            if (doubles_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = doubles_list.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        case string_list_type: {
            if (strings_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    auto property_value = strings_list.at(key)[internal_ids[i]];
                    if(filter(property_value)) {
                        properties.emplace_back(external_ids[i], property_value);
                    }
                }
            }
            break;
        }
        default: {

        }
        }

        if (!deleted.at(key).isEmpty()) {
            for (size_t i = 0; i < internal_ids.size(); i++) {
                if (deleted.at(key).contains(internal_ids[i])) {
                    properties.erase(std::remove_if(properties.begin(),
                              properties.end(),
                              [id = external_ids[i]](std::pair<uint64_t, property_type_t> x){ return x.first == id; }),
                      properties.end());
                }
            }
        }

        return properties;
    }

    std::map<uint64_t, property_type_t> Properties::getProperty(const std::vector<uint64_t> &external_ids, const std::vector<uint64_t> &internal_ids, const std::string &key) const {
        std::map<uint64_t, property_type_t> properties;
        // Check if empty
        if (external_ids.empty()) {
            return properties;
        }

        auto type_id = getPropertyTypeId(key);
        switch (type_id) {
        case boolean_type: {
            if (booleans.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i], booleans.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        case integer_type: {
            if (integers.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i], integers.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        case double_type: {
            if (doubles.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i],  doubles.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        case date_type: {
            if (doubles.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i], doubles.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        case string_type: {
            if (strings.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i], strings.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        case boolean_list_type: {
            if (booleans_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i],  booleans_list.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        case integer_list_type: {
            if (integers_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i], integers_list.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        case double_list_type: {
            if (doubles_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i], doubles_list.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        case date_list_type: {
            if (doubles_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i], doubles_list.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        case string_list_type: {
            if (strings_list.at(key).size() > internal_ids.back()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    properties.emplace(external_ids[i], strings_list.at(key)[internal_ids[i]]);
                }
            }
            break;
        }
        default: {

        }
        }

        if (!deleted.at(key).isEmpty()) {
            for (size_t i = 0; i < internal_ids.size(); i++) {
                if (deleted.at(key).contains(internal_ids[i])) {
                    properties.erase(external_ids[i]);
                }
            }
        }

        return properties;
    }

    std::map<uint64_t, std::map<std::string, property_type_t>> Properties::getProperties(const std::vector<uint64_t> &external_ids, const std::vector<uint64_t> &internal_ids) const {
        // Build a temporary map of id < string, any >
        std::map<uint64_t, std::map<std::string, property_type_t>> properties;
        // Check if empty
        if (external_ids.empty()) {
            return properties;
        }
        for(uint64_t id : external_ids) {
            properties.emplace(id, std::map<std::string, property_type_t>());
        }

        // Go through all the property types these Nodes or Relationships types are supposed to have
        for (const auto&[key, type_id] : types) {
            // Find the value based on the type and key and add it to the properties map
            switch (type_id) {
                case boolean_type: {
                    if (booleans.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, booleans.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                case integer_type: {
                    if (integers.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, integers.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                case double_type: {
                    if (doubles.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, doubles.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                case date_type: {
                    if (doubles.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, doubles.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                case string_type: {
                    if (strings.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, strings.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                case boolean_list_type: {
                    if (booleans_list.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, booleans_list.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                case integer_list_type: {
                    if (integers_list.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, integers_list.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                case double_list_type: {
                    if (doubles_list.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, doubles_list.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                case date_list_type: {
                    if (doubles_list.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, doubles_list.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                case string_list_type: {
                    if (strings_list.at(key).size() > internal_ids.back()) {
                        for (size_t i = 0; i < internal_ids.size(); i++) {
                            properties.at(external_ids[i]).emplace(key, strings_list.at(key)[internal_ids[i]]);
                        }
                    }
                    break;
                }
                default: {

                }
            }
            if (!deleted.at(key).isEmpty()) {
                for (size_t i = 0; i < internal_ids.size(); i++) {
                    if (deleted.at(key).contains(internal_ids[i])) {
                        properties.at(external_ids[i]).erase(key);
                    }
                }
            }
        }

        return properties;
    }

    std::map<std::string, property_type_t> Properties::getProperties(uint64_t index) const {
        // Build a temporary map of string, any
        std::map<std::string, property_type_t> properties;
        // Go through all the property types this Node or Relationship type is supposed to have
        for (const auto&[key, type_id] : types) {
            // If the entry has been deleted, then skip adding it to the map (alternatively we could add an unset Any)
            if (!deleted.at(key).contains(index)) {
                // Find the value based on the type and key and add it to the properties map
                switch (type_id) {
                case boolean_type: {
                    if (booleans.at(key).size() > index) {
                        properties.emplace(key, booleans.at(key)[index]);
                    }
                    break;
                }
                case integer_type: {
                    if (integers.at(key).size() > index) {
                        properties.emplace(key, integers.at(key)[index]);
                    }
                    break;
                }
                case double_type: {
                    if (doubles.at(key).size() > index) {
                        properties.emplace(key, doubles.at(key)[index]);
                    }
                    break;
                }
                case date_type: {
                  if (doubles.at(key).size() > index) {
                    properties.emplace(key, doubles.at(key)[index]);
                  }
                  break;
                }
                case string_type: {
                    if (strings.at(key).size() > index) {
                        properties.emplace(key, strings.at(key)[index]);
                    }
                    break;
                }
                case boolean_list_type: {
                    if (booleans_list.at(key).size() > index) {
                        properties.emplace(key, booleans_list.at(key)[index]);
                    }
                    break;
                }
                case integer_list_type: {
                    if (integers_list.at(key).size() > index) {
                        properties.emplace(key, integers_list.at(key)[index]);
                    }
                    break;
                }
                case double_list_type: {
                    if (doubles_list.at(key).size() > index) {
                        properties.emplace(key, doubles_list.at(key)[index]);
                    }
                    break;
                }
                case date_list_type: {
                  if (doubles_list.at(key).size() > index) {
                    properties.emplace(key, doubles_list.at(key)[index]);
                  }
                  break;
                }
                case string_list_type: {
                    if (strings_list.at(key).size() > index) {
                        properties.emplace(key, strings_list.at(key)[index]);
                    }
                    break;
                }
                default: {

                }
            }
        }
        }
        return properties;
    }

    bool Properties::isDeleted(const std::string& key, uint64_t index) {
        if (deleted.find(key) != deleted.end()) {
            return deleted[key].contains(index);
        }
        return true;
    }

    // Be careful with this method since it includes deleted nodes
    uint64_t Properties::getDeletedCount(const std::string& key) {
        if (deleted.find(key) != deleted.end()) {
            return deleted[key].cardinality();
        }
        return 0;
    }

    roaring::Roaring64Map& Properties::getDeletedMap(const std::string& key) {
        if (deleted.find(key) != deleted.end()) {
            return deleted[key];
        }
        return deleted[""];
    }

    std::vector<bool>& Properties::getBooleans(const std::string& key) {
        if (booleans.find(key) != booleans.end()) {
            return booleans[key];
        }
        return booleans[""];
    }

    std::vector<int64_t>& Properties::getIntegers(const std::string& key) {
        if (integers.find(key) != integers.end()) {
            return integers[key];
        }
        return integers[""];
    }

    std::vector<double>& Properties::getDoubles(const std::string& key) {
        if (doubles.find(key) != doubles.end()) {
            return doubles[key];
        }
        return doubles[""];
    }

    std::vector<std::string>& Properties::getStrings(const std::string& key) {
        if (strings.find(key) != strings.end()) {
            return strings[key];
        }
        return strings[""];
    }

    std::vector<std::vector<bool>>& Properties::getBooleansList(const std::string& key) {
        if (booleans_list.find(key) != booleans_list.end()) {
            return booleans_list[key];
        }
        return booleans_list[""];
    }

    std::vector<std::vector<int64_t>>& Properties::getIntegersList(const std::string& key) {
        if (integers_list.find(key) != integers_list.end()) {
            return integers_list[key];
        }
        return integers_list[""];
    }

    std::vector<std::vector<double>>& Properties::getDoublesList(const std::string& key) {
        if (doubles_list.find(key) != doubles_list.end()) {
            return doubles_list[key];
        }
        return doubles_list[""];
    }

    std::vector<std::vector<std::string>>& Properties::getStringsList(const std::string& key) {
        if (strings_list.find(key) != strings_list.end()) {
            return strings_list[key];
        }
        return strings_list[""];
    }

    property_type_t Properties::getProperty(const std::string& key, uint64_t index) {
        if (types.find(key) != types.end() && !deleted[key].contains(index)) {
            switch (types[key]) {
                case boolean_type: {
                    if (booleans[key].size() > index) {
                        // property_type_t and vector<bool> don't play nice together due to how vector<bool> is optimized
                        // so this cast makes sure we return the value typed correctly
                        return static_cast<bool>(booleans[key][index]);
                    }
                    break;
                }
                case integer_type: {
                    if (integers[key].size() > index) {
                        return integers[key][index];
                    }
                    break;
                }
                case double_type: {
                    if (doubles[key].size() > index) {
                        return doubles[key][index];
                    }
                    break;
                }
                case date_type: {
                    if (doubles.at(key).size() > index) {
                        return doubles[key][index];
                    }
                    break;
                }
                case string_type: {
                    if (strings[key].size() > index) {
                        return strings[key][index];
                    }
                    break;
                }
                case boolean_list_type: {
                    if (booleans_list[key].size() > index) {
                        return booleans_list[key][index];
                    }
                    break;
                }
                case integer_list_type: {
                    if (integers[key].size() > index) {
                        return integers_list[key][index];
                    }
                    break;
                }
                case double_list_type: {
                    if (doubles_list[key].size() > index) {
                        return doubles_list[key][index];
                    }
                    break;
                }
                case date_list_type: {
                    if (doubles_list.at(key).size() > index) {
                        return doubles_list[key][index];
                    }
                    break;
                }
                case string_list_type: {
                    if (strings_list[key].size() > index) {
                        return strings_list[key][index];
                    }
                    break;
                }
                default: {

                }
            }
        }
        return std::monostate();
    }

    bool Properties::getBooleanProperty(const std::string& key, uint64_t index) {
        if (booleans.contains(key) && index < booleans[key].size() ) {
            return booleans[key][index];
        }
        return tombstone_boolean;
    }

    int64_t Properties::getIntegerProperty(const std::string& key, uint64_t index) {
        if (integers.contains(key) && index < integers[key].size() ) {
            return integers[key][index];
        }
        return tombstone_int;
    }

    double Properties::getDoubleProperty(const std::string& key, uint64_t index) {
        if (doubles.contains(key) && index < doubles[key].size() ) {
            return doubles[key][index];
        }
        return tombstone_double;
    }

    std::string Properties::getStringProperty(const std::string& key, uint64_t index) {
         if (strings.contains(key) && index < strings[key].size() ) {
             return strings[key][index];
        }
        return tombstone_string;
    }

    std::vector<bool> Properties::getBooleanProperties(const std::string& key, std::vector<uint64_t> sorted_internal_ids) {
        std::vector<bool> properties;
        if (booleans.contains(key) && sorted_internal_ids.back() < booleans[key].size() ) {
             properties.reserve(sorted_internal_ids.size());
             for (auto id : sorted_internal_ids) {
                    properties.emplace_back(booleans[key][id]);
             }
        }
        return properties;
    }

    std::vector<int64_t> Properties::getIntegerProperties(const std::string& key, std::vector<uint64_t> sorted_internal_ids) {
        std::vector<int64_t> properties;
        if (integers.contains(key) && sorted_internal_ids.back() < integers[key].size() ) {
             properties.reserve(sorted_internal_ids.size());
             for (auto id : sorted_internal_ids) {
                    properties.emplace_back(integers[key][id]);
             }
        }
        return properties;
    }

    std::vector<double> Properties::getDoubleProperties(const std::string& key, std::vector<uint64_t> sorted_internal_ids) {
        std::vector<double> properties;
        if (doubles.contains(key) && sorted_internal_ids.back() < doubles[key].size() ) {
             properties.reserve(sorted_internal_ids.size());
             for (auto id : sorted_internal_ids) {
                    properties.emplace_back(doubles[key][id]);
             }
        }
        return properties;
    }

    std::vector<std::string> Properties::getStringProperties(const std::string& key, std::vector<uint64_t> sorted_internal_ids){
        std::vector<std::string> properties;
        if (strings.contains(key) && sorted_internal_ids.back() < strings[key].size() ) {
             properties.reserve(sorted_internal_ids.size());
             for (auto id : sorted_internal_ids) {
                    properties.emplace_back(strings[key][id]);
             }
        }
        return properties;
    }

    std::vector<bool> Properties::getListOfBooleanProperty(const std::string& key, uint64_t index) {
         if (booleans_list.contains(key) && index < booleans_list[key].size() ) {
             return booleans_list[key][index];
        }
        return tombstone_list_of_booleans;
    }

    std::vector<int64_t> Properties::getListOfIntegerProperty(const std::string& key, uint64_t index) {
        if (integers_list.contains(key) && index < integers_list[key].size() ) {
            return integers_list[key][index];
        }
        return tombstone_list_of_ints;
    }

    std::vector<double> Properties::getListOfDoubleProperty(const std::string& key, uint64_t index) {
        if (doubles_list.contains(key) && index < doubles_list[key].size() ) {
            return doubles_list[key][index];
        }
        return tombstone_list_of_doubles;
    }

    std::vector<std::string> Properties::getListOfStringProperty(const std::string& key, uint64_t index) {
        if (strings_list.contains(key) && index < strings_list[key].size() ) {
            return strings_list[key][index];
        }
        return tombstone_list_of_strings;
    }

    bool Properties::deleteProperties(uint64_t index) {
        for (const auto& [key, value] : types) {
            deleted[key].add(index);
        }
        return true;
    }

    bool Properties::deleteProperty(const std::string& key, uint64_t index) {
        if (types.contains(key)) {
            deleted[key].add(index);
            return true;
        }
        return false;
    }

    bool Properties::isBooleanProperty(const property_type_t& value) {
        return value.index() == 1;
    }

    bool Properties::isIntegerProperty(const property_type_t& value) {
        return value.index() == 2;
    }

    bool Properties::isDoubleProperty(const property_type_t& value) {
        return value.index() == 3;
    }

    bool Properties::isStringProperty(const property_type_t& value) {
        return value.index() == 4;
    }

    bool Properties::isBooleanListProperty(const property_type_t& value) {
        return value.index() == 5;
    }

    bool Properties::isIntegerListProperty(const property_type_t& value) {
        return value.index() == 6;
    }

    bool Properties::isDoubleListProperty(const property_type_t& value) {
        return value.index() == 7;
    }

    bool Properties::isStringListProperty(const property_type_t& value) {
        return value.index() == 8;
    }
}