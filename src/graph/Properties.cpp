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

    const static uint8_t boolean_type = 1;
    const static uint8_t integer_type = 2;
    const static uint8_t double_type = 3;
    const static uint8_t string_type = 4;
    const static uint8_t boolean_list_type = 5;
    const static uint8_t integer_list_type = 6;
    const static uint8_t double_list_type = 7;
    const static uint8_t string_list_type = 8;

    Properties::Properties()  {
        type_map = {
                {"boolean",      getBooleanPropertyType()},
                {"integer",      getIntegerPropertyType()},
                {"double",       getDoublePropertyType()},
                {"string",       getStringPropertyType()},
                {"boolean_list", getBooleanListPropertyType()},
                {"integer_list", getIntegerListPropertyType()},
                {"double_list",  getDoubleListPropertyType()},
                {"string_list",  getStringListPropertyType()}
        };

        allowed_types = {"", "boolean", "integer", "double", "string", "boolean_list", "integer_list", "double_list", "string_list"};

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

    std::map<std::string, std::string> Properties::getPropertyTypes() {
        std::map<std::string, std::string> map;
        for (auto [type, type_id]: types) {
            map.insert({type, allowed_types[type_id]});
        }
        map.erase("");
        return map;
    }

    uint8_t Properties::getPropertyTypeId(const std::string& key) {
        if (types.find(key) != types.end()) {
            return types[key];
        }
        return 0;
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
        auto type_search = type_map.find(type);
        if (type_search != type_map.end()) {
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

    bool Properties::setBooleanProperty(const std::string &key, uint64_t index, bool value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != boolean_type) {
            return false;
        }

        if (booleans[key].size() <= index) {
            booleans[key].resize(1 + index);
        }
        booleans[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setIntegerProperty(const std::string &key, uint64_t index, int64_t value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != integer_type) {
            return false;
        }

        if (integers[key].size() <= index) {
            integers[key].resize(1 + index);
        }
        integers[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setDoubleProperty(const std::string &key, uint64_t index, double value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != double_type) {
            return false;
        }

        if (doubles[key].size() <= index) {
            doubles[key].resize(1 + index);
        }
        doubles[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setStringProperty(const std::string &key, uint64_t index, const std::string &value) {
        if(types.empty()) {
            return false;
        }
        if (types.find(key) == types.end()) {
            return false;
        }
        if (types[key] != string_type) {
            return false;
        }

        if (strings[key].size() <= index) {
            strings[key].resize(1 + index);
        }
        strings[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setListOfBooleanProperty(const std::string &key, uint64_t index, const std::vector<bool> &value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != boolean_list_type) {
            return false;
        }

        if (booleans_list[key].size() <= index) {
            booleans_list[key].resize(1 + index);
        }
        booleans_list[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool
    Properties::setListOfIntegerProperty(const std::string &key, uint64_t index, const std::vector<int64_t> &value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != integer_list_type) {
            return false;
        }

        if (integers_list[key].size() <= index) {
            integers_list[key].resize(1 + index);
        }
        integers_list[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool Properties::setListOfDoubleProperty(const std::string &key, uint64_t index, const std::vector<double> &value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != double_list_type) {
            return false;
        }

        if (doubles_list[key].size() <= index) {
            doubles_list[key].resize(1 + index);
        }
        doubles_list[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    bool
    Properties::setListOfStringProperty(const std::string &key, uint64_t index, const std::vector<std::string> &value) {
        if(types.empty()) {
            return false;
        }
        auto type_check = types.find(key);
        if (type_check == types.end()) {
            return false;
        }
        if (types[key] != string_list_type) {
            return false;
        }

        if (strings_list[key].size() <= index) {
            strings_list[key].resize(1 + index);
        }
        strings_list[key][index] = value;
        deleted[key].remove(index);

        return true;
    }

    std::map<std::string, std::any> Properties::getProperties(uint64_t index) {
        // Build a temporary map of string, any
        std::map<std::string, std::any> properties;
        // Go through all the property types this Node or Relationship type is supposed to have
        for (auto const&[key, type_id] : types) {
            // If the entry has been deleted, then skip adding it to the map (alternatively we could add an unset Any)
            if (!deleted[key].contains(index)) {
                // Find the value based on the type and key and add it to the properties map
                switch (type_id) {
                case boolean_type: {
                    if (booleans[key].size() > index) {
                        properties.emplace(key, booleans[key][index]);
                    }
                    break;
                }
                case integer_type: {
                    if (integers[key].size() > index) {
                        properties.emplace(key, integers[key][index]);
                    }
                    break;
                }
                case double_type: {
                    if (doubles[key].size() > index) {
                        properties.emplace(key, doubles[key][index]);
                    }
                    break;
                }
                case string_type: {
                    if (strings[key].size() > index) {
                        properties.emplace(key, strings[key][index]);
                    }
                    break;
                }
                case boolean_list_type: {
                    if (booleans_list[key].size() > index) {
                        properties.emplace(key, booleans_list[key][index]);
                    }
                    break;
                }
                case integer_list_type: {
                    if (integers_list[key].size() > index) {
                        properties.emplace(key, integers_list[key][index]);
                    }
                    break;
                }
                case double_list_type: {
                    if (doubles_list[key].size() > index) {
                        properties.emplace(key, doubles_list[key][index]);
                    }
                    break;
                }
                case string_list_type: {
                    if (strings_list[key].size() > index) {
                        properties.emplace(key, strings_list[key][index]);
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

    std::any Properties::getProperty(const std::string& key, uint64_t index) {
        if (types.find(key) != types.end()) {
            if (!deleted[key].contains(index)) {
                switch (types[key]) {
                    case boolean_type: {
                        if (booleans[key].size() > index) {
                            // std::any and vector<bool> don't play nice together due to how vector<bool> is optimized
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
        }
        return tombstone_any;
    }

    bool Properties::getBooleanProperty(const std::string& key, uint64_t index) {
        auto search = booleans.find(key);

        if (search != booleans.end()) {
            if (index < booleans[key].size() ) {
                return booleans[key][index];
            }
        }
        return tombstone_boolean;
    }

    int64_t Properties::getIntegerProperty(const std::string& key, uint64_t index) {
        auto search = integers.find(key);

        if (search != integers.end()) {
            if (index < integers[key].size() ) {
                return integers[key][index];
            }
        }
        return tombstone_int;
    }

    double Properties::getDoubleProperty(const std::string& key, uint64_t index) {
        auto search = doubles.find(key);

        if (search != doubles.end()) {
            if (index < doubles[key].size() ) {
                return doubles[key][index];
            }
        }
        return tombstone_double;
    }

    std::string Properties::getStringProperty(const std::string& key, uint64_t index) {
        auto search = strings.find(key);

        if (search != strings.end()) {
            if (index < strings[key].size() ) {
                return strings[key][index];
            }
        }
        return tombstone_string;
    }

    std::vector<bool> Properties::getListOfBooleanProperty(const std::string& key, uint64_t index) {
        auto search = booleans_list.find(key);

        if (search != booleans_list.end()) {
            if (index < booleans_list[key].size() ) {
                return booleans_list[key][index];
            }
        }
        return tombstone_list_of_booleans;
    }

    std::vector<int64_t> Properties::getListOfIntegerProperty(const std::string& key, uint64_t index) {
        auto search = integers_list.find(key);

        if (search != integers_list.end()) {
            if (index < integers_list[key].size() ) {
                return integers_list[key][index];
            }
        }
        return tombstone_list_of_ints;
    }

    std::vector<double> Properties::getListOfDoubleProperty(const std::string& key, uint64_t index) {
        auto search = doubles_list.find(key);

        if (search != doubles_list.end()) {
            if (index < doubles_list[key].size() ) {
                return doubles_list[key][index];
            }
        }
        return tombstone_list_of_doubles;
    }

    std::vector<std::string> Properties::getListOfStringProperty(const std::string& key, uint64_t index) {
        auto search = strings_list.find(key);

        if (search != strings_list.end()) {
            if (index < strings_list[key].size() ) {
                return strings_list[key][index];
            }
        }
        return tombstone_list_of_strings;
    }

    bool Properties::deleteProperties(uint64_t index) {
        for (auto[key, value] : types) {
            deleted[key].add(index);
        }
        return true;
    }

    bool Properties::deleteProperty(const std::string& key, uint64_t index) {
        if (types.find(key) != types.end()) {
            deleted[key].add(index);
            return true;
        }
        return false;
    }

    bool Properties::isBooleanProperty(std::any value) {
        return value.type() == typeid(bool);
    }

    bool Properties::isIntegerProperty(std::any value) {
        return value.type() == typeid(int64_t);
    }

    bool Properties::isDoubleProperty(std::any value) {
        return value.type() == typeid(double);
    }

    bool Properties::isStringProperty(std::any value) {
        return value.type() == typeid(std::string);
    }

    bool Properties::isBooleanListProperty(std::any value) {
        return value.type() == typeid(std::vector<bool>);
    }

    bool Properties::isIntegerListProperty(std::any value) {
        return value.type() == typeid(std::vector<int64_t>);
    }

    bool Properties::isDoubleListProperty(std::any value) {
        return value.type() == typeid(std::vector<double>);
    }

    bool Properties::isStringListProperty(std::any value) {
        return value.type() == typeid(std::vector<std::string>);
    }
}